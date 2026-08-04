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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

/**
 * {@link CdcClient} implementation backed by the MariaDB JDBC driver, talking to a StarRocks FE
 * over the MySQL wire protocol.
 *
 * <p>Connections are lazily created and reused across calls. {@code bookmarkCreate}/
 * {@code bookmarkRelease} additionally tolerate FE-leader failover: a "must run on the FE
 * leader" failure rotates to the next configured host and retries there, since those two
 * functions can only execute on the current FE leader.
 *
 * <p><b>Not thread-safe.</b> The single reused {@link Connection}, the URL rotation index, and
 * any open streaming {@link ResultSet} are all plain mutable state, and the MariaDB driver itself
 * serializes Connection/streaming-result access on one lock. Every caller must therefore drive one
 * client instance from a single thread; {@code StarRocksCdcSourceTask} keeps all of its JDBC on the
 * poll thread for exactly this reason (its {@code commit()} callback runs on a different Connect
 * thread and deliberately issues no SQL of its own).
 *
 * <p>Not unit-tested per the implementation plan for this task; connection behavior is exercised
 * by the Task 8 integration smoke test instead.
 */
public class StarRocksJdbcClient implements CdcClient {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksJdbcClient.class);

    private static final String MYSQL_SCHEME_PREFIX = "jdbc:mysql://";
    private static final String MARIADB_SCHEME_PREFIX = "jdbc:mariadb://";
    private static final long RETRY_PAUSE_MS = 500L;

    /**
     * Fetch size used for the two streaming reads. MariaDB streams a result set row-by-row for
     * <em>any</em> positive fetch size, so a plain batch size is all that is needed here.
     *
     * <p>It must not be {@code Integer.MIN_VALUE}: that is the MySQL Connector/J streaming idiom
     * and is MySQL-driver-specific. {@code org.mariadb.jdbc.Statement#setFetchSize} rejects every
     * negative value with {@code SQLException("invalid fetch size")}, so passing it here would
     * throw before the query was ever sent -- i.e. no row could ever be read.
     */
    private static final int STREAM_FETCH_SIZE = 1024;

    static {
        try {
            Class.forName("org.mariadb.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("mariadb-java-client driver not found on classpath", e);
        }
    }

    private final StarRocksCdcSourceConfig config;
    private final List<String> urls;
    private int urlIndex;
    private Connection conn;

    public StarRocksJdbcClient(StarRocksCdcSourceConfig config) {
        this.config = config;
        this.urls = parseUrls(config.jdbcUrl());
        if (this.urls.isEmpty()) {
            throw new IllegalArgumentException(
                    "No usable host found in " + StarRocksCdcSourceConfig.JDBC_URL + ": " + config.jdbcUrl());
        }
        this.urlIndex = 0;
    }

    @Override
    public long bookmarkCreate(String db, String table, String holder, long ttlMs) throws SQLException {
        String sql = SqlBuilder.bookmarkCreateSql(db, table, holder, ttlMs);
        String result = executeBookmarkFunction(sql);
        // executeBookmarkFunction returns null for an empty result set, and a raw parseLong would
        // then throw NumberFormatException -- unchecked, so it escapes poll()'s catch (SQLException)
        // and kills the task instead of being handled as the SQL failure it is.
        if (result == null) {
            throw new SQLException("bookmark_create returned no bookmark id for " + db + "." + table);
        }
        try {
            return Long.parseLong(result.trim());
        } catch (NumberFormatException e) {
            throw new SQLException(
                    "bookmark_create returned a non-numeric bookmark id for " + db + "." + table + ": " + result, e);
        }
    }

    @Override
    public void bookmarkRelease(String db, String table, long bookmarkId, String holder) throws SQLException {
        String sql = SqlBuilder.bookmarkReleaseSql(db, table, bookmarkId, holder);
        executeBookmarkFunction(sql);
    }

    @Override
    public List<ColumnMeta> fetchColumns(String db, String table) throws SQLException {
        String sql = SqlBuilder.columnsProbeSql(db, table);
        try {
            Connection c = getConnection();
            try (Statement stmt = c.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                ResultSetMetaData meta = rs.getMetaData();
                List<ColumnMeta> result = new ArrayList<>();
                int columnCount = meta.getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    result.add(new ColumnMeta(
                            meta.getColumnName(i),
                            meta.getColumnType(i),
                            meta.getPrecision(i),
                            meta.getScale(i),
                            meta.isNullable(i) != ResultSetMetaData.columnNoNulls));
                }
                return result;
            }
        } catch (SQLException e) {
            closeIfBroken(e);
            throw e;
        }
    }

    @Override
    public List<String> fetchPrimaryKeys(String db, String table) throws SQLException {
        String raw = fetchTableConfigRow(db, table).primaryKey;
        List<String> result = new ArrayList<>();
        if (raw == null || raw.trim().isEmpty()) {
            return result;
        }
        for (String part : raw.split(",")) {
            String cleaned = stripBackticksAndWhitespace(part);
            if (!cleaned.isEmpty()) {
                result.add(cleaned);
            }
        }
        return result;
    }

    @Override
    public String fetchTableModel(String db, String table) throws SQLException {
        return fetchTableConfigRow(db, table).tableModel;
    }

    @Override
    public boolean cdcPropertyEnabled(String db, String table) throws SQLException {
        String sql = SqlBuilder.showCreateTableSql(db, table);
        try {
            Connection c = getConnection();
            try (Statement stmt = c.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (!rs.next()) {
                    throw new SQLException("table not found: " + db + "." + table);
                }
                String ddl = rs.getString(2);
                return ddl != null && ddl.contains("\"enable_change_data_capture\" = \"true\"");
            }
        } catch (SQLException e) {
            closeIfBroken(e);
            throw e;
        }
    }

    @Override
    public void streamSnapshot(String db, String table, List<String> cols, long bookmarkId, RowConsumer consumer)
            throws SQLException {
        String sql = SqlBuilder.snapshotSql(db, table, cols, bookmarkId);
        try {
            Connection c = getConnection();
            try (Statement stmt = c.createStatement()) {
                stmt.setFetchSize(STREAM_FETCH_SIZE);
                Calendar utc = newUtcCalendar();
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        consumer.accept(extractRow(rs, cols.size(), utc));
                    }
                }
            }
        } catch (SQLException e) {
            closeIfBroken(e);
            throw e;
        }
    }

    @Override
    public void streamChanges(String db, String table, List<String> cols, long base, long head,
                               ChangeRowConsumer consumer) throws SQLException, NonTrackableException {
        String sql = SqlBuilder.changesSql(db, table, cols, base, head);
        try {
            Connection c = getConnection();
            try (Statement stmt = c.createStatement()) {
                stmt.setFetchSize(STREAM_FETCH_SIZE);
                Calendar utc = newUtcCalendar();
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    // The last two projected columns are always __CHANGE_TYPE__ (int) and
                    // __ROW_VERSION__ (long); extractRow only covers the leading cols.size()
                    // business columns, so they are peeled off directly here.
                    int changeTypeIdx = cols.size() + 1;
                    int rowVersionIdx = cols.size() + 2;
                    while (rs.next()) {
                        Object[] row = extractRow(rs, cols.size(), utc);
                        int changeType = rs.getInt(changeTypeIdx);
                        long rowVersion = rs.getLong(rowVersionIdx);
                        consumer.accept(row, changeType, rowVersion);
                    }
                }
            }
        } catch (SQLException e) {
            closeIfBroken(e);
            NonTrackableException nte = NonTrackableException.classify(e);
            if (nte != null) {
                throw nte;
            }
            throw e;
        }
    }

    @Override
    public void close() {
        closeQuietlyCurrent();
    }

    // ------------------------------------------------------------------
    // Bookmark functions: leader fault tolerance + bounded retry.
    // ------------------------------------------------------------------

    /**
     * Executes a bookmark_create/bookmark_release call, returning the single-row single-column
     * string result (or null if the call returned no rows).
     *
     * <p>A "must run on the FE leader" failure closes the current connection, rotates to the
     * next configured URL, and retries there; each URL is attempted at most once in this
     * rotation, so a full lap that never finds the leader throws the last exception seen. Any
     * other SQLException is retried up to {@code maxRetries()} times against the same URL, with
     * a pause between attempts.
     */
    private String executeBookmarkFunction(String sql) throws SQLException {
        int totalUrls = urls.size();
        int attempts = Math.max(1, config.maxRetries());
        SQLException lastEx = null;
        for (int lap = 0; lap < totalUrls; lap++) {
            boolean redirected = false;
            for (int attempt = 0; attempt < attempts; attempt++) {
                try {
                    Connection c = getConnection();
                    try (Statement stmt = c.createStatement();
                         ResultSet rs = stmt.executeQuery(sql)) {
                        return rs.next() ? rs.getString(1) : null;
                    }
                } catch (SQLException e) {
                    lastEx = e;
                    if (isLeaderRedirect(e)) {
                        closeQuietlyCurrent();
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
    // information_schema.tables_config lookups shared by fetchPrimaryKeys/fetchTableModel.
    // ------------------------------------------------------------------

    private TableConfigRow fetchTableConfigRow(String db, String table) throws SQLException {
        String sql = SqlBuilder.tableConfigSql(db, table);
        try {
            Connection c = getConnection();
            try (Statement stmt = c.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (!rs.next()) {
                    throw new SQLException("table not found: " + db + "." + table);
                }
                return new TableConfigRow(rs.getString("TABLE_MODEL"), rs.getString("PRIMARY_KEY"));
            }
        } catch (SQLException e) {
            closeIfBroken(e);
            throw e;
        }
    }

    private static String stripBackticksAndWhitespace(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (ch != '`' && !Character.isWhitespace(ch)) {
                sb.append(ch);
            }
        }
        return sb.toString();
    }

    private static final class TableConfigRow {
        final String tableModel;
        final String primaryKey;

        TableConfigRow(String tableModel, String primaryKey) {
            this.tableModel = tableModel;
            this.primaryKey = primaryKey;
        }
    }

    // ------------------------------------------------------------------
    // Row extraction shared by streamSnapshot/streamChanges.
    // ------------------------------------------------------------------

    /**
     * Builds the UTC calendar handed to every {@code getDate}/{@code getTimestamp} call of one
     * streaming read.
     *
     * <p>Without an explicit calendar the driver materializes temporal values in the JVM default
     * zone, which breaks Kafka Connect's logical types on any worker not running in UTC. Connect's
     * {@code Date} logical type is defined as UTC midnight, so a local-midnight {@code
     * java.sql.Date} makes JsonConverter/AvroConverter throw {@code DataException("Kafka Connect
     * Date type should not have any time fields set to non-zero values")}; a {@code Timestamp}
     * read in local time silently lands the instant off by the worker's UTC offset.
     *
     * <p>{@link Calendar} is not thread-safe, so this is deliberately not a shared static: one
     * instance is created per streaming read and stays confined to the poll thread driving it
     * (the MariaDB date/timestamp codecs {@code clear()} it before each use, so reuse across the
     * rows of a single read is safe).
     */
    private static Calendar newUtcCalendar() {
        return Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    }

    private static Object[] extractRow(ResultSet rs, int columnCount, Calendar utc) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        Object[] row = new Object[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            row[i - 1] = extractValue(rs, meta, i, utc);
        }
        return row;
    }

    private static Object extractValue(ResultSet rs, ResultSetMetaData meta, int index, Calendar utc)
            throws SQLException {
        Object value;
        switch (meta.getColumnType(index)) {
            case Types.DECIMAL:
            case Types.NUMERIC:
                value = rs.getBigDecimal(index);
                break;
            case Types.DATE:
                value = rs.getDate(index, utc);
                break;
            case Types.TIMESTAMP:
                value = rs.getTimestamp(index, utc);
                break;
            case Types.BIT:
            case Types.BOOLEAN:
                value = rs.getBoolean(index);
                break;
            case Types.TINYINT:
                value = rs.getByte(index);
                break;
            case Types.SMALLINT:
                value = rs.getShort(index);
                break;
            case Types.INTEGER:
                value = rs.getInt(index);
                break;
            case Types.BIGINT:
                value = rs.getLong(index);
                break;
            case Types.REAL:
            case Types.FLOAT:
                value = rs.getFloat(index);
                break;
            case Types.DOUBLE:
                value = rs.getDouble(index);
                break;
            default:
                value = rs.getString(index);
                break;
        }
        return rs.wasNull() ? null : value;
    }

    // ------------------------------------------------------------------
    // Connection lifecycle: lazy build, reuse, rebuild on broken connection.
    // ------------------------------------------------------------------

    private Connection getConnection() throws SQLException {
        if (conn == null || conn.isClosed()) {
            conn = openConnection(urls.get(urlIndex));
        }
        return conn;
    }

    private Connection openConnection(String url) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", config.username());
        props.setProperty("password", config.password());
        props.setProperty("connectTimeout", String.valueOf(config.connectTimeoutMs()));
        Connection connection = DriverManager.getConnection(url, props);
        if (config.netChanges()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("SET enable_cdc_net_change=true");
            } catch (SQLException e) {
                try {
                    connection.close();
                } catch (SQLException ignored) {
                    // best-effort cleanup of the half-open connection
                }
                throw e;
            }
        }
        return connection;
    }

    private void closeIfBroken(SQLException e) {
        if (e instanceof SQLNonTransientConnectionException) {
            closeQuietlyCurrent();
        }
    }

    private void closeQuietlyCurrent() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
                // close() contract is silent per the interface
            }
            conn = null;
        }
    }

    private void rotateUrl() {
        urlIndex = (urlIndex + 1) % urls.size();
    }

    // ------------------------------------------------------------------
    // JDBC URL parsing: comma-separated host list -> one jdbc:mariadb:// URL per host.
    // ------------------------------------------------------------------

    private static List<String> parseUrls(String jdbcUrl) {
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

    private static String rewriteMariadbScheme(String url) {
        if (url.startsWith(MYSQL_SCHEME_PREFIX)) {
            return MARIADB_SCHEME_PREFIX + url.substring(MYSQL_SCHEME_PREFIX.length());
        }
        return url;
    }
}
