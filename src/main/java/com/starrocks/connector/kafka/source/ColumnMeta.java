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

import java.sql.Types;
import java.util.Locale;

/**
 * Immutable metadata for one captured column.
 *
 * <p>Both the JDBC view and StarRocks' own are kept: {@link java.sql.Types} cannot express
 * "StarRocks ARRAY" or "HLL sketch" -- both are {@link java.sql.Types#OTHER}, and over the MySQL
 * protocol result-set metadata cannot tell either from a VARCHAR. The two are folded into one
 * {@link ColumnType} at construction, and that tree is what the schema and the value readers use.
 */
public final class ColumnMeta {
    public final String name;
    public final int jdbcType;     // java.sql.Types.*
    /**
     * {@code NUMERIC_PRECISION}. Diagnostics only: Connect's {@code Decimal} logical type carries a
     * scale and no precision, so nothing reads it -- which is how it held a length for a long time.
     */
    public final int precision;
    public final int scale;
    public final boolean nullable;
    /** {@code DATA_TYPE}: "array", "json", "hll", "varchar"... null when the server was not asked. */
    public final String srDataType;
    /** {@code COLUMN_TYPE}: the full type with nesting, e.g. {@code "map<varchar(10),int>"}. */
    public final String srColumnType;
    /**
     * The column's type as one tree, for every column: a parsed ARRAY/MAP/STRUCT with its nesting, a
     * scalar derived from {@link #jdbcType}, or {@link ColumnType.Kind#OPAQUE} for what is carried
     * as text -- a complex column whose COLUMN_TYPE did not parse, or a type never mapped.
     */
    final ColumnType type;

    public ColumnMeta(String name, int jdbcType, int precision, int scale, boolean nullable) {
        this(name, jdbcType, precision, scale, nullable, null, null);
    }

    public ColumnMeta(String name, int jdbcType, int precision, int scale, boolean nullable,
                      String srDataType, String srColumnType) {
        this.name = name;
        this.jdbcType = jdbcType;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
        this.srDataType = srDataType;
        this.srColumnType = srColumnType;
        this.type = typeOf(jdbcType, scale, srDataType, srColumnType);
    }

    static boolean isComplex(String srDataType) {
        switch (normalize(srDataType)) {
            case "array":
            case "map":
            case "struct":
                return true;
            default:
                return false;
        }
    }

    private static ColumnType typeOf(int jdbcType, int scale, String srDataType, String srColumnType) {
        if (isComplex(srDataType)) {
            return ColumnTypeParser.parse(srColumnType).orElse(ColumnType.opaque(fallbackLogicalName(srDataType)));
        }
        switch (jdbcType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return ColumnType.scalar(ColumnType.Kind.BOOLEAN);
            case Types.TINYINT:
                return ColumnType.scalar(ColumnType.Kind.TINYINT);
            case Types.SMALLINT:
                return ColumnType.scalar(ColumnType.Kind.SMALLINT);
            case Types.INTEGER:
                return ColumnType.scalar(ColumnType.Kind.INT);
            case Types.BIGINT:
                return ColumnType.scalar(ColumnType.Kind.BIGINT);
            case Types.REAL:
                return ColumnType.scalar(ColumnType.Kind.FLOAT);
            // FLOAT is double precision in JDBC; REAL is the single-precision one.
            case Types.FLOAT:
            case Types.DOUBLE:
                return ColumnType.scalar(ColumnType.Kind.DOUBLE);
            case Types.DECIMAL:
            case Types.NUMERIC:
                // LARGEINT rides the DECIMAL getters but is its own kind: scale 0 whatever the server says.
                return "bigint unsigned".equals(normalize(srDataType))
                        ? ColumnType.scalar(ColumnType.Kind.LARGEINT) : ColumnType.decimal(scale);
            case Types.DATE:
                return ColumnType.scalar(ColumnType.Kind.DATE);
            case Types.TIMESTAMP:
                return ColumnType.scalar(ColumnType.Kind.DATETIME);
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return ColumnType.scalar(ColumnType.Kind.BYTES);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return ColumnType.scalar(ColumnType.Kind.STRING);
            default:
                return "json".equals(normalize(srDataType))
                        ? ColumnType.scalar(ColumnType.Kind.JSON) : ColumnType.opaque(fallbackLogicalName(srDataType));
        }
    }

    /**
     * The logical name a complex column keeps when it is carried as text: StarRocks-specific rather
     * than Debezium's JSON name, since BE's rendering is not JSON (unquoted map keys, single-quoted
     * nested JSON). Anything else is a plain string.
     */
    private static String fallbackLogicalName(String srDataType) {
        switch (normalize(srDataType)) {
            case "array":
                return "com.starrocks.data.Array";
            case "map":
                return "com.starrocks.data.Map";
            case "struct":
                return "com.starrocks.data.Struct";
            default:
                return null;
        }
    }

    private static String normalize(String srDataType) {
        return srDataType == null ? "" : srDataType.trim().toLowerCase(Locale.ROOT);
    }
}
