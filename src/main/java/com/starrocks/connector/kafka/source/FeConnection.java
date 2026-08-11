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
 * Owns the JDBC connection to a StarRocks FE: which URL, when to reconnect, when to rotate, how
 * often to retry. The transport is chosen entirely by the URL prefix -- there is no separate config
 * key, because a JDBC URL already names the driver it wants.
 *
 * <p>Arrow Flight needs cluster-side setup the MySQL protocol does not: a non-negative
 * {@code arrow_flight_port} in both {@code fe.conf} and {@code be.conf} (default -1, not mutable),
 * and {@code --add-opens=java.base/java.nio=ALL-UNNAMED} on the FE <em>and</em> the worker. It also
 * does not bypass the FE by default: {@code arrow_flight_proxy_enabled} is true, so the FE still
 * relays each BE stream.
 *
 * <p><b>Not thread-safe.</b> The reused {@link Connection}, the rotation index and any open
 * streaming {@link ResultSet} are plain mutable state, and the MariaDB driver serializes access on
 * one lock. {@code StarRocksCdcSourceTask} keeps all JDBC on the poll thread for this reason.
 */
final class FeConnection implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FeConnection.class);

    private static final String MYSQL_SCHEME_PREFIX = "jdbc:mysql://";
    private static final String MARIADB_SCHEME_PREFIX = "jdbc:mariadb://";
    /** Points at {@code arrow_flight_port}, not 9030; a plaintext server needs {@code ?useEncryption=false}. */
    private static final String ARROW_FLIGHT_SCHEME_PREFIX = "jdbc:arrow-flight-sql://";

    private static final long RETRY_PAUSE_MS = 500L;

    /**
     * MySQL transport only; MariaDB streams row-by-row for any positive fetch size.
     *
     * <p><b>Must not be {@code Integer.MIN_VALUE}.</b> That is Connector/J's streaming idiom;
     * MariaDB rejects every negative value with {@code SQLException("invalid fetch size")}, so it
     * would throw before the query was sent and no row could ever be read.
     */
    private static final int STREAM_FETCH_SIZE = 1024;

    static {
        // Explicit registration guards against Connect's per-plugin classloader isolation
        // defeating ServiceLoader discovery.
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

    boolean isArrowFlight() {
        return !urls.isEmpty() && urls.get(0).startsWith(ARROW_FLIGHT_SCHEME_PREFIX);
    }

    Connection get() throws SQLException {
        if (conn == null || conn.isClosed()) {
            conn = openConnection(urls.get(urlIndex));
        }
        return conn;
    }

    /** Arrow Flight already streams RecordBatches, and its contract here is not ours to assume. */
    void applyStreamingFetchSize(Statement stmt) throws SQLException {
        if (!isArrowFlight()) {
            stmt.setFetchSize(STREAM_FETCH_SIZE);
        }
    }

    /**
     * Runs a leader-only statement, returning its single-row single-column result (null if empty).
     * A "must run on the FE leader" failure rotates to the next URL; each is tried at most once per
     * lap. Anything else is retried up to {@code maxRetries()} times on the same URL.
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

    /** Fans a comma-separated host list into one URL per host, copying scheme, path and query. */
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

    /** mysql -> mariadb, because that is the bundled driver. Every other scheme passes through
     * untouched; rewriting Arrow Flight would hand it to the wrong driver. */
    static String rewriteMariadbScheme(String url) {
        return url.startsWith(MYSQL_SCHEME_PREFIX)
                ? MARIADB_SCHEME_PREFIX + url.substring(MYSQL_SCHEME_PREFIX.length())
                : url;
    }
}
