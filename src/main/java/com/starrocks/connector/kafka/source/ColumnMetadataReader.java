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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The authoritative column list, used both to build the Connect schema and to read values back out.
 * One strategy for both transports: ask the server.
 */
final class ColumnMetadataReader {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnMetadataReader.class);

    private final FeConnection connection;

    ColumnMetadataReader(FeConnection connection) {
        this.connection = connection;
    }

    /**
     * The column list for one table, in ordinal order, straight from {@code information_schema}.
     *
     * <p>Never from {@link java.sql.ResultSetMetaData}: the Arrow Flight driver repeats the schema,
     * calls every column NOT NULL and zeroes precision and scale, and the MySQL protocol cannot tell
     * an ARRAY, a JSON or an HLL sketch from a VARCHAR. Two descriptions would also mean one table
     * producing two schemas depending on the URL scheme -- a StarRocks BOOLEAN really did, arriving
     * as int8 on one transport and boolean on the other.
     */
    List<ColumnMeta> fetchColumns(String db, String table) throws SQLException {
        List<InfoSchemaColumn> rows = readInformationSchema(db, table);
        List<ColumnMeta> result = new ArrayList<>(rows.size());
        for (InfoSchemaColumn c : rows) {
            result.add(new ColumnMeta(c.name, toJdbcType(c.dataType, c.columnType), c.precision, c.scale,
                    c.nullable, c.dataType, c.columnType));
        }
        return result;
    }

    /**
     * The column list as the server reports it. Ordinary rows, so no driver opinion involved.
     */
    private List<InfoSchemaColumn> readInformationSchema(String db, String table) throws SQLException {
        String sql = SqlBuilder.columnsMetadataSql(db, table);
        try {
            Connection c = connection.get();
            try (Statement stmt = c.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                List<InfoSchemaColumn> result = new ArrayList<>();
                Set<String> seen = new HashSet<>();
                StringBuilder described = new StringBuilder();
                while (rs.next()) {
                    String name = rs.getString("COLUMN_NAME");
                    String dataType = normalize(rs.getString("DATA_TYPE"));
                    String columnType = rs.getString("COLUMN_TYPE");
                    boolean nullable = !"NO".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
                    int precision = rs.getInt("NUMERIC_PRECISION");
                    int scale = rs.getInt("NUMERIC_SCALE");
                    if (described.length() > 0) {
                        described.append(", ");
                    }
                    described.append(result.size() + 1).append(':').append(name)
                            .append('(').append(dataType).append("->").append(toJdbcType(dataType, columnType))
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

    /** One information_schema row. */
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
    static int toJdbcType(String dataType, String columnType) {
        // FE renders BOOLEAN's DATA_TYPE as "tinyint" and only its COLUMN_TYPE as "tinyint(1)"
        // (ScalarType#toMysqlDataTypeString / #toMysqlColumnTypeString), so the name alone loses it.
        if ("tinyint(1)".equals(normalize(columnType))) {
            return Types.BOOLEAN;
        }
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

    /** Mirrors {@link #toJdbcType}'s cases; that switch cannot tell "text on purpose" from
     *  "never heard of it", since both are {@link Types#OTHER}. Add a case there, add a name here. */
    private static final Set<String> KNOWN_DATA_TYPES = new HashSet<>(Arrays.asList(
            "tinyint", "smallint", "int", "bigint", "bigint unsigned", "float", "double", "decimal",
            "char", "varchar", "date", "datetime", "binary", "varbinary",
            "array", "map", "struct", "json", "hll", "bitmap", "percentile"));

    /**
     * True when StarRocks named a type this connector has no mapping for; null means the server was
     * not asked. Match the set, never a literal: FE renders an unmapped type as its own lowercase
     * name ("variant", "time") and UNKNOWN_TYPE as "unknown_type", so no spelling is fixed enough
     * to compare against.
     */
    static boolean isUnrecognized(String srDataType) {
        return srDataType != null && !KNOWN_DATA_TYPES.contains(normalize(srDataType));
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
