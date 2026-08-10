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
 * The authoritative column list, used both to build the Connect schema and to read values back out.
 * Two strategies, because the transports do not describe a result set the same way.
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
     * <p>Arrow Flight's driver cannot describe a result set usably: it repeats the schema, calls
     * every column NOT NULL and zeroes precision and scale. Nullability alone is fatal -- NOT NULL
     * becomes a required Connect field and the first NULL fails Struct validation. The MySQL path
     * stays on result-set metadata, which is correct there and verified end to end.
     */
    List<ColumnMeta> fetchColumns(String db, String table) throws SQLException {
        return connection.isArrowFlight()
                ? fetchFromInformationSchema(db, table)
                : fetchFromResultSetMetadata(db, table);
    }

    /**
     * Result-set metadata, then StarRocks' own type names layered on top.
     *
     * <p>The second query is not redundant: {@link ResultSetMetaData} cannot tell an ARRAY, a JSON
     * or an HLL sketch from a VARCHAR over the MySQL protocol. Without it this transport could
     * neither refuse a table it cannot capture nor stamp a logical type, and the same table would
     * produce a different schema depending on the URL scheme.
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
                    // Otherwise this surfaces far away as a bare SchemaBuilderException about field
                    // name duplication, with no hint that the driver's metadata was the problem.
                    if (!seen.add(name)) {
                        throw new SQLException("Column metadata for " + db + "." + table
                                + " reports the name '" + name + "' more than once, so no record schema can be"
                                + " built from it. This is the JDBC driver's view of `" + sql + "`, not the"
                                + " table's real definition -- the two transports do not derive it the same"
                                + " way. Reported " + columnCount + " column(s): " + described);
                    }
                    result.add(new ColumnMeta(name, type, precision, scale, nullable));
                }
                // The only record of what the driver actually reported, and the transports differ
                // enough that it is worth having before anything downstream goes wrong.
                LOG.info("Resolved {} column(s) for {}.{}: {}", columnCount, db, table, described);
                return withServerTypes(db, table, result);
            }
        } catch (SQLException e) {
            connection.closeIfBroken(e);
            throw e;
        }
    }

    /**
     * Layers the server's type names onto driver-described columns, matching by name. A mismatch
     * fails rather than leaving a column un-enriched -- that column is one the HLL guard cannot see.
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

    /** The Arrow Flight path: the server's answer, used whole. */
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
     * The column list as the server reports it: authoritative on Arrow Flight, and the source of
     * the StarRocks type names on both transports. Ordinary rows, so no driver opinion involved.
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

    /** One information_schema row, before reconciliation with the JDBC view. */
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
     * {@code DATA_TYPE} to a {@link Types} constant. The accepted set is closed -- exactly what FE's
     * {@code Type.toMysqlDataTypeString()} emits -- so an unrecognized value means StarRocks grew a
     * type. Unknown lands on {@link Types#OTHER} and is carried as text rather than guessed at.
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
            // Complex and semi-structured: carried as text for now, but listed explicitly rather
            // than left to the default, because "a StarRocks ARRAY as text" and "a type never heard
            // of" get different logical names in ChangeRecordMapper.
            case "array":
            case "map":
            case "struct":
            case "json":
                return Types.OTHER;
            // Aggregate sketches. Refused by preflight; listed only to keep the switch exhaustive.
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
