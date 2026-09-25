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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Immutable metadata for one captured column, as {@code information_schema.columns} describes it,
 * folded at construction into the one {@link ColumnType} the schema and the value readers use.
 * Only the server's description, never a driver's; {@link ColumnMetaReader} says why.
 */
public final class ColumnMeta {
    /** The closed set FE's {@code Type.toMysqlDataTypeString} emits; a name outside it means StarRocks grew a type. */
    private static final Set<String> KNOWN_DATA_TYPES = new HashSet<>(Arrays.asList(
            "tinyint", "smallint", "int", "bigint", "bigint unsigned", "float", "double", "decimal",
            "char", "varchar", "date", "datetime", "binary", "varbinary",
            "array", "map", "struct", "json", "hll", "bitmap", "percentile"));

    public final String name;
    public final boolean nullable;
    /** {@code DATA_TYPE}, normalized: "array", "json", "hll", "bigint unsigned"... null when the server was not asked. */
    public final String srDataType;
    /** {@code COLUMN_TYPE}: the full type with nesting, e.g. {@code "map<varchar(10),int>"}, or BOOLEAN's {@code "tinyint(1)"}. */
    public final String srColumnType;
    /**
     * The column's type as one tree, for every column: a parsed ARRAY/MAP/STRUCT with its nesting, a
     * scalar named by {@code DATA_TYPE}, or {@link ColumnType.Kind#OPAQUE} for what is carried as
     * text -- a complex column whose COLUMN_TYPE did not parse, or a type never mapped.
     */
    final ColumnType type;

    /** @param scale {@code NUMERIC_SCALE}; DECIMAL is the only type that reads it. */
    public ColumnMeta(String name, String srDataType, String srColumnType, int scale, boolean nullable) {
        this.name = name;
        this.nullable = nullable;
        this.srDataType = srDataType == null ? null : normalize(srDataType);
        this.srColumnType = srColumnType;
        this.type = typeOf(this.srDataType, srColumnType, scale);
    }

    /** What the server said and what it became: {@code v(varchar->string,sql=varchar(20),null=true)}. */
    @Override
    public String toString() {
        return name + "(" + srDataType + "->" + type + ",sql=" + srColumnType + ",null=" + nullable + ")";
    }

    private static ColumnType typeOf(String dataType, String columnType, int scale) {
        if (isComplex(dataType)) {
            return ColumnTypeParser.parse(columnType).orElse(ColumnType.opaque(fallbackLogicalName(dataType)));
        }
        // FE renders BOOLEAN's DATA_TYPE as "tinyint" and only its COLUMN_TYPE as "tinyint(1)"
        // (ScalarType#toMysqlDataTypeString / #toMysqlColumnTypeString), so the name alone loses it.
        if ("tinyint(1)".equals(normalize(columnType))) {
            return ColumnType.scalar(ColumnType.Kind.BOOLEAN);
        }
        switch (normalize(dataType)) {
            case "tinyint":
                return ColumnType.scalar(ColumnType.Kind.TINYINT);
            case "smallint":
                return ColumnType.scalar(ColumnType.Kind.SMALLINT);
            case "int":
                return ColumnType.scalar(ColumnType.Kind.INT);
            case "bigint":
                return ColumnType.scalar(ColumnType.Kind.BIGINT);
            // LARGEINT: 128 bits, integral, so its Decimal schema is scale 0 whatever NUMERIC_SCALE
            // says (FE leaves it NULL, which getInt reads as 0 by accident).
            case "bigint unsigned":
                return ColumnType.scalar(ColumnType.Kind.LARGEINT);
            case "float":
                return ColumnType.scalar(ColumnType.Kind.FLOAT);
            case "double":
                return ColumnType.scalar(ColumnType.Kind.DOUBLE);
            case "decimal":
                return ColumnType.decimal(scale);
            case "char":
            case "varchar":
                return ColumnType.scalar(ColumnType.Kind.STRING);
            case "date":
                return ColumnType.scalar(ColumnType.Kind.DATE);
            case "datetime":
                return ColumnType.scalar(ColumnType.Kind.DATETIME);
            case "binary":
            case "varbinary":
                return ColumnType.scalar(ColumnType.Kind.BYTES);
            case "json":
                return ColumnType.scalar(ColumnType.Kind.JSON);
            // hll/bitmap/percentile are refused by preflight; anything else is carried as text, unnamed.
            default:
                return ColumnType.opaque(null);
        }
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

    /**
     * True when StarRocks named a type this connector has no mapping for; null means the server was
     * not asked. Matched against the set, never a literal: FE renders an unmapped type as its own
     * lowercase name ("variant", "time") and UNKNOWN_TYPE as "unknown_type".
     */
    static boolean isUnrecognized(String srDataType) {
        return srDataType != null && !KNOWN_DATA_TYPES.contains(normalize(srDataType));
    }

    /** StarRocks type names whose values cannot be meaningfully exported by a plain SELECT. */
    static boolean isNonExportable(String srDataType) {
        switch (normalize(srDataType)) {
            case "hll":
            case "bitmap":
            case "percentile":
                return true;
            default:
                return false;
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
                return ColumnType.ARRAY_LOGICAL_NAME;
            case "map":
                return ColumnType.MAP_LOGICAL_NAME;
            case "struct":
                return ColumnType.STRUCT_LOGICAL_NAME;
            default:
                return null;
        }
    }

    private static String normalize(String srDataType) {
        return srDataType == null ? "" : srDataType.trim().toLowerCase(Locale.ROOT);
    }
}
