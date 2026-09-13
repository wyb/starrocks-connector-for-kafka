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

package com.starrocks.connector.kafka.common;

import org.apache.kafka.common.config.ConfigException;

import java.util.HashMap;
import java.util.Map;

/**
 * The {@code key:value,key:value} config format, used by {@code starrocks.table2topic.map}.
 *
 * <p>Entries split on the first {@code ':'}, so values may contain one. Duplicate keys and empty
 * entries are rejected rather than skipped, and a malformed value always throws rather than
 * returning a sentinel a caller could read as "mapping disabled". Values are returned unvalidated.
 */
public final class KeyValueListParser {

    private KeyValueListParser() {
    }

    /** @throws ConfigException if an entry is empty, lacks a {@code ':'}, has an empty side, or repeats a key */
    public static Map<String, String> parse(String configKey, String raw) {
        Map<String, String> result = new HashMap<>();
        if (raw == null || raw.trim().isEmpty()) {
            return result;
        }
        for (String rawEntry : raw.split(",")) {
            String entry = rawEntry.trim();
            if (entry.isEmpty()) {
                // Skipping it would let a typo change where a table's records land without saying
                // so. A trailing comma never reaches here: String.split drops trailing empties.
                throw new ConfigException(configKey, raw,
                        "Empty entry between commas; remove it or complete the key:value pair.");
            }
            int idx = entry.indexOf(':');
            if (idx < 0) {
                throw new ConfigException(configKey, raw,
                        "Entry '" + entry + "' is missing a ':' separator; expected key:value.");
            }
            String key = entry.substring(0, idx).trim();
            String value = entry.substring(idx + 1).trim();
            if (key.isEmpty() || value.isEmpty()) {
                throw new ConfigException(configKey, raw,
                        "Entry '" + entry + "' must have non-empty key and value segments.");
            }
            String previous = result.put(key, value);
            if (previous != null) {
                throw new ConfigException(configKey, raw,
                        "Key '" + key + "' is mapped more than once ('" + previous + "' and '" + value + "').");
            }
        }
        return result;
    }
}
