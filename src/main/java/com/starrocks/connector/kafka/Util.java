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

package com.starrocks.connector.kafka;

import com.starrocks.connector.kafka.common.KeyValueListParser;

import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

public class Util {

    static boolean isValidStarrocksTableName(String tableName) {
        return tableName.matches("^([_a-zA-Z]{1}[_$a-zA-Z0-9]+\\.){0,2}[_a-zA-Z]{1}[_$a-zA-Z0-9]+$");
    }
    /**
     * Parses {@code starrocks.topic2table.map} and checks every mapped table name.
     *
     * <p>Always throws {@link ConfigException} on a bad value; it never returns {@code null}. The
     * previous version returned {@code null} for a malformed list, and the caller read that as
     * "mapping disabled" and routed every topic to a same-named table -- so a typo silently changed
     * where data landed instead of failing the task. The parsing itself now lives in
     * {@link KeyValueListParser}, shared with the CDC source connector.
     */
    public static Map<String, String> parseTopicToTableMap(String input) {
        Map<String, String> topic2Table =
                KeyValueListParser.parse(StarRocksSinkConnectorConfig.STARROCKS_TOPIC2TABLE_MAP, input);
        for (Map.Entry<String, String> entry : topic2Table.entrySet()) {
            if (!isValidStarrocksTableName(entry.getValue())) {
                throw new ConfigException(StarRocksSinkConnectorConfig.STARROCKS_TOPIC2TABLE_MAP, input,
                        "table name '" + entry.getValue() + "' mapped from topic '" + entry.getKey()
                                + "' must be at least 2 characters, start with _ or a-zA-Z, and contain only"
                                + " _$a-zA-Z0-9");
            }
        }
        return topic2Table;
    }
}
