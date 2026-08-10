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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Resolves a table's column list -- the authoritative one, used both to build the Connect schema
 * and to read values back out.
 *
 * <p>Two strategies, because the transports do not describe a result set the same way. The MySQL
 * path reads {@link ResultSetMetaData}; the Arrow Flight path cannot, and asks the server instead.
 * See {@link #fetchColumns} for why.
 */
final class ColumnMetadataReader {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnMetadataReader.class);

    private final FeConnection connection;

    ColumnMetadataReader(FeConnection connection) {
        this.connection = connection;
    }

    /**
     * The column list for one table, in ordinal order.
     *
     * <p>Arrow Flight's driver cannot describe a result set usably: it repeats the schema, reports
     * every column NOT NULL, and zeroes precision and scale. Nullability alone is fatal -- a
     * column described as NOT NULL becomes a required Connect field, and the first NULL value then
     * fails Struct validation. Ask the server for the table definition instead.
     *
     * <p>The MySQL path deliberately stays on result-set metadata: it is correct there and has
     * been verified end to end, and there is no reason to put it behind newer, less-exercised
     * code. Both paths log what they resolved, so the two can be compared directly before this
     * divergence is ever collapsed back into one.
     */
    List<ColumnMeta> fetchColumns(String db, String table) throws SQLException {
        return connection.isArrowFlight()
                ? fetchFromInformationSchema(db, table)
                : fetchFromResultSetMetadata(db, table);
    }

    /**
     * Result-set metadata, then StarRocks' own type names layered on top.
     *
     * <p>The second query is not redundant. {@link ResultSetMetaData} cannot say that a column is
     * an ARRAY, a JSON or an HLL sketch -- all three arrive as a string type over the MySQL
     * protocol, indistinguishable from VARCHAR. Without the server's answer this transport could
     * neither refuse a table it cannot meaningfully capture nor stamp a logical type on the schema,
     * and the same table would then produce a different schema depending on which URL scheme was
     * configured. The transport is meant to be an implementation detail; a schema that changes with
     * it would not be one.
     */
    private List<ColumnMeta> fetchFromResultSetMetadata(String db, String table) throws SQLException {
        String sql = SqlBuilder.columnsProbeSql(db, table);
        try {
            Connection c = connection.get();
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
                return withServerTypes(db, table, result);
            }
        } catch (SQLException e) {
            connection.closeIfBroken(e);
            throw e;
        }
    }

    /**
     * Layers {@code information_schema}'s type names onto columns already described by the driver,
     * matching on column name.
     *
     * <p>A mismatch fails rather than leaving some columns without the server's view: a silently
     * un-enriched column would be one the HLL/BITMAP guard cannot see, which is precisely the case
     * that must not slip through.
     */
    private List<ColumnMeta> withServerTypes(String db, String table, List<ColumnMeta> jdbcView)
            throws SQLException {
        Map<String, InfoSchemaColumn> byName = new LinkedHashMap<>();
        for (InfoSchemaColumn c : readInformationSchema(db, table)) {
            byName.put(c.name, c);
        }
        List<ColumnMeta> result = new ArrayList<>(jdbcView.size());
        for (ColumnMeta col : jdbcView) {
            InfoSchemaColumn server = byName.get(col.name);
            if (server == null) {
                throw new SQLException("column '" + col.name + "' of " + db + "." + table
                        + " is in the query's result set but not in information_schema.columns"
                        + " (which listed " + byName.keySet() + "), so its StarRocks type is unknown"
                        + " and it cannot be checked for a type this connector must refuse");
            }
            result.add(col.withStarRocksType(server.dataType, server.columnType));
        }
        return result;
    }

    /**
     * The column list as the server reports it, from {@code information_schema.columns}.
     *
     * <p>Used for the Arrow Flight transport, whose driver-supplied result-set metadata cannot be
     * trusted. These are ordinary result rows, so the answer does not depend on how a driver
     * chooses to describe a query.
     */
    private List<ColumnMeta> fetchFromInformationSchema(String db, String table) throws SQLException {
        List<InfoSchemaColumn> rows = readInformationSchema(db, table);
        List<ColumnMeta> result = new ArrayList<>(rows.size());
        for (InfoSchemaColumn c : rows) {
            result.add(new ColumnMeta(c.name, toJdbcType(c.dataType), c.precision, c.scale, c.nullable,
                    c.dataType, c.columnType));
        }
        return result;
    }

    /**
     * The column list as the server reports it, from {@code information_schema.columns}.
     *
     * <p>Authoritative for the Arrow Flight transport, whose driver-supplied result-set metadata
     * cannot be trusted, and the source of the StarRocks type names on both. These are ordinary
     * result rows, so the answer does not depend on how a driver chooses to describe a query.
     */
    private List<InfoSchemaColumn> readInformationSchema(String db, String table) throws SQLException {
        String sql = SqlBuilder.columnsMetadataSql(db, table);
        try {
            Connection c = connection.get();
            try (Statement stmt = c.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                List<InfoSchemaColumn> result = new ArrayList<>();
                Set<String> seen = new LinkedHashSet<>();
                StringBuilder described = new StringBuilder();
                while (rs.next()) {
                    String name = rs.getString("COLUMN_NAME");
                    String dataType = normalize(rs.getString("DATA_TYPE"));
                    String columnType = rs.getString("COLUMN_TYPE");
                    boolean nullable = !"NO".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
                    int precision = rs.getInt("COLUMN_SIZE");
                    int scale = rs.getInt("DECIMAL_DIGITS");
                    if (described.length() > 0) {
                        described.append(", ");
                    }
                    described.append(result.size() + 1).append(':').append(name)
                            .append('(').append(dataType).append("->").append(toJdbcType(dataType))
                            .append(",sql=").append(columnType)
                            .append(",p=").append(precision)
                            .append(",s=").append(scale)
                            .append(",null=").append(nullable).append(')');
                    if (!seen.add(name)) {
                        throw new SQLException("information_schema.columns lists the column '" + name
                                + "' more than once for " + db + "." + table + ": " + described);
                    }
                    result.add(new InfoSchemaColumn(name, dataType, columnType, precision, scale, nullable));
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
            connection.closeIfBroken(e);
            throw e;
        }
    }

    private static String normalize(String dataType) {
        return dataType == null ? null : dataType.trim().toLowerCase(Locale.ROOT);
    }

    /** One {@code information_schema.columns} row, before it is reconciled with the JDBC view. */
    private static final class InfoSchemaColumn {
        final String name;
        final String dataType;
        final String columnType;
        final int precision;
        final int scale;
        final boolean nullable;

        InfoSchemaColumn(String name, String dataType, String columnType,
                         int precision, int scale, boolean nullable) {
            this.name = name;
            this.dataType = dataType;
            this.columnType = columnType;
            this.precision = precision;
            this.scale = scale;
            this.nullable = nullable;
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
        switch (dataType.trim().toLowerCase(Locale.ROOT)) {
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
            // Complex types. java.sql.Types has nothing that describes them, and StarRocks renders
            // them as text on the wire, so they are carried as text for now -- but they are listed
            // explicitly rather than left to the default, because "a StarRocks ARRAY, carried as
            // text" and "a type this connector has never heard of" are different situations and
            // ChangeRecordMapper stamps a different logical type on each. The full nested type is
            // in ColumnMeta.srColumnType, ready for the step that builds real nested schemas.
            case "array":
            case "map":
            case "struct":
            // Semi-structured. Also text, and a genuine JSON document -- see ChangeRecordMapper.
            case "json":
                return Types.OTHER;
            // Opaque aggregate sketches. They have no exportable value at all: selecting one
            // without an accompanying function yields nothing a consumer can use. The connector
            // refuses tables that contain them (see StarRocksCdcSourceConnector's preflight), so
            // this mapping exists only to keep the switch exhaustive.
            case "hll":
            case "bitmap":
            case "percentile":
                return Types.OTHER;
            default:
                return Types.OTHER;
        }
    }

    /** StarRocks type names whose values cannot be meaningfully exported by a plain SELECT. */
    static boolean isNonExportable(String srDataType) {
        if (srDataType == null) {
            return false;
        }
        switch (normalize(srDataType)) {
            case "hll":
            case "bitmap":
            case "percentile":
                return true;
            default:
                return false;
        }
    }
}
