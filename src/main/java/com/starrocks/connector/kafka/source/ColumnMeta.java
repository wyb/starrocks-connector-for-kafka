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

/**
 * Immutable metadata for one captured column.
 * Carries just enough information for {@link ChangeRecordMapper} to build Kafka Connect
 * {@link org.apache.kafka.connect.data.Schema} instances without touching JDBC types directly.
 *
 * <p>The JDBC view and StarRocks' own view are both kept, because neither alone is enough.
 * {@link java.sql.Types} has no way to say "this is a StarRocks ARRAY" or "this is an HLL sketch"
 * -- both arrive as {@link java.sql.Types#OTHER} and, over the MySQL protocol, are indistinguishable
 * from a VARCHAR in result-set metadata. Deciding whether a column can be captured at all, and what
 * logical type to stamp on its schema, needs the server's own answer.
 */
public final class ColumnMeta {
    public final String name;
    public final int jdbcType;     // java.sql.Types.*
    public final int precision;
    public final int scale;
    public final boolean nullable;
    /**
     * {@code information_schema.columns.DATA_TYPE}: StarRocks' own type name, lower-cased
     * ({@code "array"}, {@code "json"}, {@code "hll"}, {@code "varchar"} ...). {@code null} when
     * the column was described without consulting the server.
     */
    public final String srDataType;
    /**
     * {@code information_schema.columns.COLUMN_TYPE}: the full SQL type including nesting
     * ({@code "array<int>"}, {@code "map<varchar(10),int>"}, {@code "struct<a int, b varchar(20)>"}).
     * Currently carried for diagnostics only; rebuilding nested Connect schemas from it is the
     * next step, not this one. {@code null} when unknown.
     *
     * <p>StarRocks truncates past 15 levels of nesting, rendering {@code "array<...>"}: any future
     * parser must reject that rather than guess at what was elided.
     */
    public final String srColumnType;

    /** Without the server's view -- used by tests, and by any path that cannot consult it. */
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
    }

    /** A copy carrying the server's type names, leaving the JDBC view untouched. */
    public ColumnMeta withStarRocksType(String srDataType, String srColumnType) {
        return new ColumnMeta(name, jdbcType, precision, scale, nullable, srDataType, srColumnType);
    }
}
