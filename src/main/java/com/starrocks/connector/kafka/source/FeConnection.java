/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.kafka.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Owns the JDBC connection to a StarRocks FE: which URL to use, when to reconnect, when to rotate
 * to another FE, and how many times to retry.
 *
 * <p>Two transports, chosen entirely by the URL prefix in {@code starrocks.jdbc.url} -- there is
 * no separate config key, because a JDBC URL already names the driver it wants:
 * <ul>
 *   <li>{@code jdbc:mysql://host:9030} (rewritten to {@code jdbc:mariadb://}) -- the MySQL wire
 *       protocol. Results funnel back through the FE.</li>
 *   <li>{@code jdbc:arrow-flight-sql://host:<arrow_flight_port>?useEncryption=false} -- Arrow
 *       Flight SQL. Results travel as Arrow record batches, so the row/column conversion the MySQL
 *       protocol pays for disappears. Note that the FE does <em>not</em> stop being the data path
 *       by default: {@code arrow_flight_proxy_enabled} is {@code true}, which has the FE pull each
 *       BE stream and re-serve it. {@code SET GLOBAL arrow_flight_proxy_enabled = false} hands BE
 *       endpoints to the client instead, which does remove the funnel but requires the worker to
 *       reach every BE's {@code arrow_flight_port} directly.
 *       <p>Cluster-side prerequisites: a non-negative {@code arrow_flight_port} in both
 *       {@code fe.conf} and {@code be.conf} (default {@code -1}, not mutable, needs a restart),
 *       and {@code --add-opens=java.base/java.nio=ALL-UNNAMED} in the FE's {@code JAVA_OPTS} as
 *       well as the Connect worker's -- Arrow's {@code MemoryUtil} fails to initialize without it
 *       on Java 9+, on whichever side is missing it.</li>
 * </ul>
 *
 * <p><b>Not thread-safe.</b> The single reused {@link Connection}, the URL rotation index, and any
 * open streaming {@link ResultSet} are all plain mutable state, and the MariaDB driver itself
 * serializes Connection/streaming-result access on one lock. Every caller must drive one instance
 * from a single thread; {@code StarRocksCdcSourceTask} keeps all of its JDBC on the poll thread for
 * exactly this reason.
 */
final class FeConnection implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FeConnection.class);

    private static final String MYSQL_SCHEME_PREFIX = "jdbc:mysql://";
    private static final String MARIADB_SCHEME_PREFIX = "jdbc:mariadb://";
    /**
     * Arrow Flight SQL transport. The URL prefix is the whole transport switch -- there is no
     * separate config key, because a JDBC URL already declares which driver it wants. It must
     * point at the FE's {@code arrow_flight_port} rather than 9030, and a plaintext server needs
     * {@code ?useEncryption=false} since the driver defaults to TLS.
     */
    private static final String ARROW_FLIGHT_SCHEME_PREFIX = "jdbc:arrow-flight-sql://";

    private static final long RETRY_PAUSE_MS = 500L;

    /**
     * Fetch size for the two streaming reads, applied on the MySQL/MariaDB transport only.
     * MariaDB streams a result set row-by-row for <em>any</em> positive fetch size, so a plain
     * batch size is all that is needed there.
     *
     * <p>It must not be {@code Integer.MIN_VALUE}: that is the MySQL Connector/J streaming idiom
     * and is MySQL-driver-specific. {@code org.mariadb.jdbc.Statement#setFetchSize} rejects every
     * negative value with {@code SQLException("invalid fetch size")}, so passing it here would
     * throw before the query was ever sent -- i.e. no row could ever be read.
     *
     * <p>Arrow Flight streams RecordBatches natively and has no equivalent knob, so the call is
     * skipped there rather than guessed at: driver-specific contracts on this exact method are
     * what produced that MariaDB defect in the first place.
     */
    private static final int STREAM_FETCH_SIZE = 1024;

    static {
        // Both drivers are shaded into the plugin jar. Registering explicitly guards against
        // Connect's per-plugin classloader isolation defeating ServiceLoader discovery.
        registerDriver("org.mariadb.jdbc.Driver", "mariadb-java-client");
        registerDriver("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver", "flight-sql-jdbc-driver");
    }

    private static void registerDriver(String className, String artifact) {
        try {
            Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(artifact + " driver not found on classpath", e);
        }
    }

    private final StarRocksCdcSourceConfig config;
    private final List<String> urls;
    private int urlIndex;
    private Connection conn;

    FeConnection(StarRocksCdcSourceConfig config) {
        this.config = config;
        this.urls = parseUrls(config.jdbcUrl());
        if (this.urls.isEmpty()) {
            throw new IllegalArgumentException(
                    "No usable host found in " + StarRocksCdcSourceConfig.JDBC_URL + ": " + config.jdbcUrl());
        }
        this.urlIndex = 0;
    }

    /** True when this connection talks Arrow Flight SQL rather than the MySQL protocol. */
    boolean isArrowFlight() {
        return !urls.isEmpty() && urls.get(0).startsWith(ARROW_FLIGHT_SCHEME_PREFIX);
    }

    /** Lazily opens, and reuses, the connection to the currently selected URL. */
    Connection get() throws SQLException {
        if (conn == null || conn.isClosed()) {
            conn = openConnection(urls.get(urlIndex));
        }
        return conn;
    }

    /**
     * Row-by-row streaming is a MySQL-protocol concern. Arrow Flight already streams
     * RecordBatches, and its driver's contract for this method is not ours to assume.
     */
    void applyStreamingFetchSize(Statement stmt) throws SQLException {
        if (!isArrowFlight()) {
            stmt.setFetchSize(STREAM_FETCH_SIZE);
        }
    }

    /**
     * Runs a statement that must execute on the FE leader, returning its single-row single-column
     * string result (or null if it returned no rows).
     *
     * <p>A "must run on the FE leader" failure closes the current connection, rotates to the
     * next configured URL, and retries there; each URL is attempted at most once in this
     * rotation, so a full lap that never finds the leader throws the last exception seen. Any
     * other SQLException is retried up to {@code maxRetries()} times against the same URL, with
     * a pause between attempts.
     */
    String executeOnLeader(String sql) throws SQLException {
        int totalUrls = urls.size();
        int attempts = Math.max(1, config.maxRetries());
        SQLException lastEx = null;
        for (int lap = 0; lap < totalUrls; lap++) {
            boolean redirected = false;
            for (int attempt = 0; attempt < attempts; attempt++) {
                try {
                    Connection c = get();
                    try (Statement stmt = c.createStatement();
                         ResultSet rs = stmt.executeQuery(sql)) {
                        return rs.next() ? rs.getString(1) : null;
                    }
                } catch (SQLException e) {
                    lastEx = e;
                    if (isLeaderRedirect(e)) {
                        closeQuietly();
                        String from = urls.get(urlIndex);
                        rotateUrl();
                        LOG.warn("Bookmark function must run on the FE leader; rotating from {} to {}",
                                from, urls.get(urlIndex));
                        redirected = true;
                        break;
                    }
                    closeIfBroken(e);
                    if (attempt < attempts - 1) {
                        sleepBeforeRetry();
                    }
                }
            }
            if (!redirected) {
                throw lastEx;
            }
        }
        throw lastEx;
    }

    /** Drops the pooled connection when the failure indicates it is no longer usable. */
    void closeIfBroken(SQLException e) {
        if (e instanceof SQLNonTransientConnectionException) {
            closeQuietly();
        }
    }

    void closeQuietly() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
                // close() contract is silent per the interface
            }
            conn = null;
        }
    }

    @Override
    public void close() {
        closeQuietly();
    }

    private Connection openConnection(String url) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", config.username());
        props.setProperty("password", config.password());
        props.setProperty("connectTimeout", String.valueOf(config.connectTimeoutMs()));
        return DriverManager.getConnection(url, props);
    }

    private void rotateUrl() {
        urlIndex = (urlIndex + 1) % urls.size();
    }

    private static boolean isLeaderRedirect(SQLException e) {
        String message = e.getMessage();
        return message != null && message.contains("must run on the FE leader");
    }

    private static void sleepBeforeRetry() throws SQLException {
        try {
            Thread.sleep(RETRY_PAUSE_MS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while waiting to retry", ie);
        }
    }

    // ------------------------------------------------------------------
    // JDBC URL parsing: comma-separated host list -> one URL per host.
    // ------------------------------------------------------------------

    /**
     * Fans a comma-separated host list out into one URL per host, preserving the scheme, any path,
     * and any query string on every one of them.
     *
     * <p>Package-visible so it can be unit-tested directly. It used to be private and the test
     * reached it by reflection, which is the usual sign that a boundary is in the wrong place.
     */
    static List<String> parseUrls(String jdbcUrl) {
        String url = rewriteMariadbScheme(jdbcUrl);
        int schemeEnd = url.indexOf("://");
        if (schemeEnd < 0) {
            throw new IllegalArgumentException("Invalid JDBC URL: " + jdbcUrl);
        }
        String prefix = url.substring(0, schemeEnd + 3);
        String rest = url.substring(schemeEnd + 3);

        int suffixStart = rest.length();
        int slashIdx = rest.indexOf('/');
        if (slashIdx >= 0) {
            suffixStart = Math.min(suffixStart, slashIdx);
        }
        int qIdx = rest.indexOf('?');
        if (qIdx >= 0) {
            suffixStart = Math.min(suffixStart, qIdx);
        }
        String hostList = rest.substring(0, suffixStart);
        String suffix = rest.substring(suffixStart);

        List<String> result = new ArrayList<>();
        for (String host : hostList.split(",")) {
            String trimmed = host.trim();
            if (!trimmed.isEmpty()) {
                result.add(prefix + trimmed + suffix);
            }
        }
        return result;
    }

    /**
     * {@code jdbc:mysql://} is rewritten to {@code jdbc:mariadb://} because the bundled driver is
     * MariaDB's. Every other scheme -- Arrow Flight in particular -- passes through untouched:
     * rewriting it would hand the URL to the wrong driver.
     */
    static String rewriteMariadbScheme(String url) {
        return url.startsWith(MYSQL_SCHEME_PREFIX)
                ? MARIADB_SCHEME_PREFIX + url.substring(MYSQL_SCHEME_PREFIX.length())
                : url;
    }
}
