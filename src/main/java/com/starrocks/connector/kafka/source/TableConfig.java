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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.SQLException;

/** One table's row of {@code information_schema.tables_config}, as far as preflight and the bookmark queries need it. */
final class TableConfig {

    private static final ObjectMapper JSON = new ObjectMapper();
    static final String CDC_PROPERTY = "enable_change_data_capture";

    private final String db;
    private final String table;
    final long tableId;
    /** {@code TABLE_MODEL}: {@code KeysType.toString()}, or "" when FE never set it (a view, an external table). */
    final String model;
    /** {@code PROPERTIES}: the table's property map as JSON, which is how FE writes it. */
    private final String properties;

    TableConfig(String db, String table, long tableId, String model, String properties) {
        this.db = db;
        this.table = table;
        this.tableId = tableId;
        this.model = model;
        this.properties = properties;
    }

    /**
     * Whether {@code enable_change_data_capture} is on. Parsed here rather than at fetch time: a
     * view's PROPERTIES is empty and its model check comes first. Unreadable PROPERTIES throws
     * rather than answering false, which would send an operator to enable a property already on.
     */
    boolean cdcEnabled() throws SQLException {
        if (properties == null || properties.trim().isEmpty()) {
            throw new SQLException("information_schema.tables_config.PROPERTIES is empty for " + db + "." + table
                    + ", so it cannot be told whether change data capture is enabled");
        }
        JsonNode root;
        try {
            root = JSON.readTree(properties);
        } catch (IOException e) {
            throw new SQLException("information_schema.tables_config.PROPERTIES for " + db + "." + table
                    + " is not readable as JSON: " + properties, e);
        }
        if (root == null || !root.isObject()) {
            throw new SQLException("information_schema.tables_config.PROPERTIES for " + db + "." + table
                    + " is not a JSON object: " + properties);
        }
        JsonNode value = root.get(CDC_PROPERTY);
        return value != null && "true".equalsIgnoreCase(value.asText());
    }
}
