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
 * Immutable metadata for one captured column, sourced from JDBC result set metadata.
 * Carries just enough information for {@link ChangeRecordMapper} to build Kafka Connect
 * {@link org.apache.kafka.connect.data.Schema} instances without touching JDBC types directly.
 */
public final class ColumnMeta {
    public final String name;
    public final int jdbcType;     // java.sql.Types.*
    public final int precision;
    public final int scale;
    public final boolean nullable;

    public ColumnMeta(String name, int jdbcType, int precision, int scale, boolean nullable) {
        this.name = name;
        this.jdbcType = jdbcType;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
    }
}
