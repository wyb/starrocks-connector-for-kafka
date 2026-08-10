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
 *
 * <p>Both the JDBC view and StarRocks' own are kept: {@link java.sql.Types} cannot express
 * "StarRocks ARRAY" or "HLL sketch" -- both are {@link java.sql.Types#OTHER}, and over the MySQL
 * protocol result-set metadata cannot tell either from a VARCHAR.
 */
public final class ColumnMeta {
    public final String name;
    public final int jdbcType;     // java.sql.Types.*
    public final int precision;
    public final int scale;
    public final boolean nullable;
    /** {@code DATA_TYPE}: "array", "json", "hll", "varchar"... null when the server was not asked. */
    public final String srDataType;
    /**
     * {@code COLUMN_TYPE}: full type with nesting, e.g. {@code "map<varchar(10),int>"}. Diagnostics
     * only for now. StarRocks truncates past 15 levels to {@code "array<...>"}, which any future
     * parser must reject rather than guess at.
     */
    public final String srColumnType;

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

    public ColumnMeta withStarRocksType(String srDataType, String srColumnType) {
        return new ColumnMeta(name, jdbcType, precision, scale, nullable, srDataType, srColumnType);
    }
}
