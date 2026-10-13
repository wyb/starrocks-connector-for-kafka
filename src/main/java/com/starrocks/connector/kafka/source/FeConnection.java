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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Owns the FE JDBC connection, URL rotation, and leader retries.
 * The URL selects MySQL or Arrow Flight transport. Connection and streaming state stay on the
 * poll thread; the runtime may call close() concurrently.
 */
final class FeConnection implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FeConnection.class);

    private static final String MYSQL_SCHEME_PREFIX = "jdbc:mysql://";
    private static final String MARIADB_SCHEME_PREFIX = "jdbc:mariadb://";
    /** Points at {@code arrow_flight_port}, not 9030; a plaintext server needs {@code ?useEncryption=false}. */
    private static final String ARROW_FLIGHT_SCHEME_PREFIX = "jdbc:arrow-flight-sql://";

    private static final long RETRY_PAUSE_MS = 500L;

    /**
     * Pin MySQL's nested binary rendering for {@link MysqlValueReader}. Cluster defaults can
     * change; {@code raw} breaks text decoding, while encoding {@code all} binary values changes
     * the top-level {@code getBytes()} result. Arrow Flight does not use these settings.
     */
    static final String SESSION_SETUP_SQL = "SET binary_encoding_format = '"
            + MysqlValueReader.SESSION_BINARY_ENCODING.name().toLowerCase(java.util.Locale.ROOT)
            + "', binary_encoding_level = 'nested'";

    /**
     * MariaDB streams rows for positive fetch sizes. Connector/J's
     * {@code Integer.MIN_VALUE} convention is rejected by MariaDB.
     */
    private static final int STREAM_FETCH_SIZE = 1024;

    private static final String MARIADB_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String ARROW_FLIGHT_DRIVER = "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver";

    private final StarRocksCdcSourceConfig config;
    private final List<String> urls;
    private final ConnectionFactory connectionFactory;
    private int urlIndex;
    private volatile Connection conn;
    private volatile boolean closed;

    FeConnection(StarRocksCdcSourceConfig config) {
        this(config, DriverManager::getConnection);
    }

    @FunctionalInterface
    interface ConnectionFactory {
        Connection open(String url, Properties props) throws SQLException;
    }

    FeConnection(StarRocksCdcSourceConfig config, ConnectionFactory connectionFactory) {
        this(config, connectionFactory, 0);
    }

    FeConnection(StarRocksCdcSourceConfig config, ConnectionFactory connectionFactory, int startUrlIndex) {
        this.config = config;
        this.connectionFactory = connectionFactory;
        this.urls = parseUrls(config.jdbcUrl());
        if (this.urls.isEmpty()) {
            throw new IllegalArgumentException(
                    "No usable host found in " + StarRocksCdcSourceConfig.JDBC_URL + ": " + config.jdbcUrl());
        }
        this.urlIndex = startUrlIndex;
        String driverClass = driverClassFor(urls.get(0));
        registerDriver(driverClass, artifactFor(driverClass));
    }

    boolean isArrowFlight() {
        return !urls.isEmpty() && urls.get(0).startsWith(ARROW_FLIGHT_SCHEME_PREFIX);
    }

    int urlIndex() {
        return urlIndex;
    }

    /** Flight JDBC can throw unchecked transport errors from ResultSet.next() as well as queries. */
    SQLException asSqlException(Exception failure) {
        if (failure instanceof SQLException) {
            return (SQLException) failure;
        }
        if (isArrowFlight() && FlightFailures.isFlightFailure(failure)) {
            return new SQLException(failure.getMessage(), failure);
        }
        // Programming errors (including consumer failures) must not enter the SQL retry loop.
        throw (RuntimeException) failure;
    }

    Connection get() throws SQLException {
        // Metadata and dedicated snapshot connections also need failover before any bookmark
        // statement runs. Visit each endpoint once, without nesting executeOnLeader's retries.
        SQLException last = null;
        for (int i = 0; i < urls.size(); i++) {
            try {
                return getCurrent();
            } catch (SQLException | RuntimeException failure) {
                SQLException e = asSqlException(failure);
                if (closed || Thread.currentThread().isInterrupted() || !isConnectionFailure(e)) {
                    throw e;
                }
                last = e;
                rotateAfterFailure("FE connection failed");
            }
        }
        throw last;
    }

    private Connection getCurrent() throws SQLException {
        if (closed) {
            throw new SQLException("FE connection is closed");
        }
        Connection c = conn; // one read: a concurrent close() nulling the field must not NPE here
        if (c == null || c.isClosed()) {
            c = openConnection(urls.get(urlIndex));
            conn = c;
            if (closed) {
                // close() ran while we were connecting; without this the new connection is
                // unreachable and never closed -- one leaked FE connection per stop-during-poll.
                closeQuietly();
                throw new SQLException("FE connection is closed");
            }
        }
        return c;
    }

    /** Arrow Flight already streams RecordBatches, and its contract here is not ours to assume. */
    void applyStreamingFetchSize(Statement stmt) throws SQLException {
        if (!isArrowFlight()) {
            stmt.setFetchSize(STREAM_FETCH_SIZE);
        }
    }

    /**
     * Runs a leader-only statement, returning its single-row single-column result (null if empty).
     * Leader redirects and connection failures move to the next URL. Other SQL failures, and
     * connection failures with only one configured URL, keep the bounded per-URL retry budget.
     */
    String executeOnLeader(String sql) throws SQLException {
        int totalUrls = urls.size();
        int attempts = Math.max(1, config.maxRetries());
        SQLException lastEx = null;
        for (int lap = 0; lap < totalUrls; lap++) {
            boolean redirected = false;
            for (int attempt = 0; attempt < attempts; attempt++) {
                try {
                    Connection c = getCurrent();
                    try (Statement stmt = c.createStatement();
                         ResultSet rs = stmt.executeQuery(sql)) {
                        return rs.next() ? rs.getString(1) : null;
                    }
                } catch (SQLException | RuntimeException failure) {
                    SQLException e = asSqlException(failure);
                    if (closed || Thread.currentThread().isInterrupted()) {
                        throw e;
                    }
                    lastEx = e;
                    boolean leaderRedirect = isLeaderRedirect(e);
                    if (leaderRedirect || (totalUrls > 1 && isConnectionFailure(e))) {
                        rotateAfterFailure(leaderRedirect ? "Bookmark function must run on the FE leader"
                                : "FE connection failed");
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
        // A failed get() has already visited the endpoints and did not publish a connection.
        // A failure on an established connection must move the next poll off that endpoint.
        if (conn != null && isConnectionFailure(e)) {
            rotateAfterFailure("FE connection failed");
        }
    }

    private boolean isConnectionFailure(SQLException e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLNonTransientConnectionException
                    || cause instanceof SQLTransientConnectionException
                    || cause instanceof SQLRecoverableException || cause instanceof IOException) {
                return true;
            }
            if (cause instanceof SQLException) {
                String state = ((SQLException) cause).getSQLState();
                if (state != null && state.startsWith("08")) {
                    return true;
                }
            }
            if (isArrowFlight() && FlightFailures.isConnectionFailure(cause)) {
                return true;
            }
        }
        return false;
    }

    /** Loaded only on the Flight path, so MySQL workers do not require Arrow's Java release. */
    private static final class FlightFailures {
        static boolean isFlightFailure(Throwable failure) {
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                if (cause instanceof org.apache.arrow.flight.FlightRuntimeException) {
                    return true;
                }
            }
            return false;
        }

        static boolean isConnectionFailure(Throwable cause) {
            if (!(cause instanceof org.apache.arrow.flight.FlightRuntimeException)) {
                return false;
            }
            org.apache.arrow.flight.FlightStatusCode code =
                    ((org.apache.arrow.flight.FlightRuntimeException) cause).status().code();
            return code == org.apache.arrow.flight.FlightStatusCode.UNAVAILABLE
                    || code == org.apache.arrow.flight.FlightStatusCode.TIMED_OUT;
        }
    }

    void closeQuietly() {
        Connection c = conn;
        if (c != null) {
            try {
                c.close();
            } catch (SQLException | RuntimeException e) {
                // Flight cleanup can also throw unchecked exceptions after a stream disconnects.
                LOG.debug("Failed to close FE connection", e);
            }
            conn = null;
        }
    }

    /** Terminal, unlike {@link #closeQuietly}: rotation and broken-connection recovery reopen. */
    @Override
    public void close() {
        closed = true;
        closeQuietly();
    }

    private Connection openConnection(String url) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", config.username());
        props.setProperty("password", config.password());
        props.setProperty("connectTimeout", String.valueOf(config.connectTimeoutMs()));
        Connection c = connectionFactory.open(url, props);
        if (pinsSession(url)) {
            try (Statement stmt = c.createStatement()) {
                stmt.execute(SESSION_SETUP_SQL);
            } catch (SQLException | RuntimeException e) {
                try {
                    c.close();
                } catch (SQLException suppressed) {
                    e.addSuppressed(suppressed);
                }
                throw e;
            }
        }
        return c;
    }

    /** Only the MySQL protocol renders nested values as text, so only it needs the session pinned. */
    static boolean pinsSession(String url) {
        return !url.startsWith(ARROW_FLIGHT_SCHEME_PREFIX);
    }

    private void rotateUrl() {
        urlIndex = (urlIndex + 1) % urls.size();
    }

    private void rotateAfterFailure(String reason) {
        closeQuietly();
        String from = urls.get(urlIndex);
        rotateUrl();
        LOG.warn("{}; rotating from {} to {}", reason, from, urls.get(urlIndex));
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

    /**
     * The one driver a URL needs. Loading both broke Java 8 workers: the Arrow driver is compiled
     * for Java 11, so {@code Class.forName} threw a {@link LinkageError} that left the class unusable.
     * Registered explicitly because Connect's per-plugin classloader defeats {@link java.sql.DriverManager}'s
     * {@link java.util.ServiceLoader} discovery.
     */
    static String driverClassFor(String url) {
        return url.startsWith(ARROW_FLIGHT_SCHEME_PREFIX) ? ARROW_FLIGHT_DRIVER : MARIADB_DRIVER;
    }

    static void registerDriver(String className, String artifact) {
        try {
            Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(artifact + " driver not found on classpath", e);
        } catch (LinkageError e) {
            throw new IllegalStateException(artifact + " driver is on the classpath but unusable on this JVM ("
                    + System.getProperty("java.version") + "); it is compiled for a newer Java release", e);
        }
    }

    private static String artifactFor(String driverClass) {
        return ARROW_FLIGHT_DRIVER.equals(driverClass) ? "flight-sql-jdbc-core" : "mariadb-java-client";
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
