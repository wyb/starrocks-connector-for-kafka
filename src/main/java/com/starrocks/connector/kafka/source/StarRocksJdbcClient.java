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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;

/**
 * {@link CdcClient} implementation talking to a StarRocks FE over JDBC.
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

    public StarRocksJdbcClient(StarRocksCdcSourceConfig config) {
        this.config = config;
        this.urls = parseUrls(config.jdbcUrl());
        if (this.urls.isEmpty()) {
            throw new IllegalArgumentException(
                    "No usable host found in " + StarRocksCdcSourceConfig.JDBC_URL + ": " + config.jdbcUrl());
        }
        this.urlIndex = 0;
    }

    /** True when this client talks Arrow Flight SQL rather than the MySQL protocol. */
    private boolean isArrowFlight() {
        return !urls.isEmpty() && urls.get(0).startsWith(ARROW_FLIGHT_SCHEME_PREFIX);
    }

    /**
     * Row-by-row streaming is a MySQL-protocol concern. Arrow Flight already streams
     * RecordBatches, and its driver's contract for this method is not ours to assume.
     */
    private void applyStreamingFetchSize(Statement stmt) throws SQLException {
        if (!isArrowFlight()) {
            stmt.setFetchSize(STREAM_FETCH_SIZE);
        }
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
        // Arrow Flight's driver cannot describe a result set usably: it repeats the schema, reports
        // every column NOT NULL, and zeroes precision and scale. Nullability alone is fatal -- a
        // column described as NOT NULL becomes a required Connect field, and the first NULL value
        // then fails Struct validation. Ask the server for the table definition instead.
        //
        // The MySQL path deliberately stays on result-set metadata: it is correct there and has
        // been verified end to end, and there is no reason to put it behind newer, less-exercised
        // code. Both paths log what they resolved, so the two can be compared directly before this
        // divergence is ever collapsed back into one.
        return isArrowFlight()
                ? fetchColumnsFromInformationSchema(db, table)
                : fetchColumnsFromResultSetMetadata(db, table);
    }

    private List<ColumnMeta> fetchColumnsFromResultSetMetadata(String db, String table) throws SQLException {
        String sql = SqlBuilder.columnsProbeSql(db, table);
        try {
            Connection c = getConnection();
            try (Statement stmt = c.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                ResultSetMetaData meta = rs.getMetaData();
                List<ColumnMeta> result = new ArrayList<>();
                Set<String> seen = new LinkedHashSet<>();
                int columnCount = meta.getColumnCount();
                StringBuilder described = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    String name = meta.getColumnName(i);
                    int type = meta.getColumnType(i);
                    int precision = meta.getPrecision(i);
                    int scale = meta.getScale(i);
                    boolean nullable = meta.isNullable(i) != ResultSetMetaData.columnNoNulls;
                    if (described.length() > 0) {
                        described.append(", ");
                    }
                    described.append(i).append(':').append(name)
                            .append("(type=").append(type)
                            .append(",p=").append(precision)
                            .append(",s=").append(scale)
                            .append(",null=").append(nullable).append(')');
                    // A duplicate name would otherwise surface much later and far away, as a bare
                    // SchemaBuilderException("Cannot create field because of field name
                    // duplication") from inside ChangeRecordMapper, with no hint that the column
                    // metadata this driver reported was the problem. Fail here, quoting all of it.
                    if (!seen.add(name)) {
                        throw new SQLException("Column metadata for " + db + "." + table
                                + " reports the name '" + name + "' more than once, so no record schema can be"
                                + " built from it. This is the JDBC driver's view of `" + sql + "`, not the"
                                + " table's real definition -- the two transports do not derive it the same"
                                + " way. Reported " + columnCount + " column(s): " + described);
                    }
                    result.add(new ColumnMeta(name, type, precision, scale, nullable));
                }
                // One line per table at task start, and the only record of what the driver actually
                // reported. Column discovery differs enough between the two transports that this is
                // worth having before anything downstream can go wrong.
                LOG.info("Resolved {} column(s) for {}.{}: {}", columnCount, db, table, described);
                return result;
            }
        } catch (SQLException e) {
            closeIfBroken(e);
            throw e;
        }
    }

    /**
     * The column list as the server reports it, from {@code information_schema.columns}.
     *
     * <p>Used for the Arrow Flight transport, whose driver-supplied result-set metadata cannot be
     * trusted. These are ordinary result rows, so the answer does not depend on how a driver
     * chooses to describe a query.
     */
    private List<ColumnMeta> fetchColumnsFromInformationSchema(String db, String table) throws SQLException {
        String sql = SqlBuilder.columnsMetadataSql(db, table);
        try {
            Connection c = getConnection();
            try (Statement stmt = c.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                List<ColumnMeta> result = new ArrayList<>();
                Set<String> seen = new LinkedHashSet<>();
                StringBuilder described = new StringBuilder();
                while (rs.next()) {
                    String name = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    boolean nullable = !"NO".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
                    int precision = rs.getInt("COLUMN_SIZE");
                    int scale = rs.getInt("DECIMAL_DIGITS");
                    int jdbcType = toJdbcType(dataType);
                    if (described.length() > 0) {
                        described.append(", ");
                    }
                    described.append(result.size() + 1).append(':').append(name)
                            .append('(').append(dataType).append("->").append(jdbcType)
                            .append(",p=").append(precision)
                            .append(",s=").append(scale)
                            .append(",null=").append(nullable).append(')');
                    if (!seen.add(name)) {
                        throw new SQLException("information_schema.columns lists the column '" + name
                                + "' more than once for " + db + "." + table + ": " + described);
                    }
                    result.add(new ColumnMeta(name, jdbcType, precision, scale, nullable));
                }
                if (result.isEmpty()) {
                    throw new SQLException("table not found: " + db + "." + table
                            + " (information_schema.columns returned no rows)");
                }
                LOG.info("Resolved {} column(s) for {}.{} from information_schema: {}",
                        result.size(), db, table, described);
                return result;
            }
        } catch (SQLException e) {
            closeIfBroken(e);
            throw e;
        }
    }

    /**
     * StarRocks' {@code information_schema.columns.DATA_TYPE} spelling to a {@link Types} constant.
     *
     * <p>The accepted set is closed: it is exactly what the BE's
     * {@code SchemaColumnsScanner::to_mysql_data_type_string} can emit, so an unrecognized value
     * means StarRocks grew a type and this needs revisiting -- not that the caller passed
     * something odd. Unrecognized and opaque types both land on {@link Types#OTHER}, which
     * {@code ChangeRecordMapper} renders as a string, preserving the value rather than guessing at
     * a structured representation.
     */
    static int toJdbcType(String dataType) {
        if (dataType == null) {
            return Types.OTHER;
        }
        switch (dataType.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "tinyint":
                return Types.TINYINT;
            case "smallint":
                return Types.SMALLINT;
            case "int":
                return Types.INTEGER;
            case "bigint":
                return Types.BIGINT;
            // LARGEINT. 128-bit, so it does not fit a long; carried as text to stay lossless.
            case "bigint unsigned":
                return Types.OTHER;
            case "float":
                return Types.REAL;
            case "double":
                return Types.DOUBLE;
            case "decimal":
                return Types.DECIMAL;
            case "char":
                return Types.CHAR;
            case "varchar":
                return Types.VARCHAR;
            case "date":
                return Types.DATE;
            case "datetime":
                return Types.TIMESTAMP;
            case "binary":
                return Types.BINARY;
            case "varbinary":
                return Types.VARBINARY;
            // Opaque metric and semi-structured types: no faithful JDBC type, read as text.
            case "hll":
            case "bitmap":
            case "percentile":
            case "json":
                return Types.OTHER;
            default:
                return Types.OTHER;
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
                applyStreamingFetchSize(stmt);
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
                applyStreamingFetchSize(stmt);
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
