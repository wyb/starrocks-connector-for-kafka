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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parses the {@code key:value,key:value} config format shared by the sink's
 * {@code starrocks.topic2table.map} and the CDC source's {@code starrocks.table2topic.map}.
 *
 * <p>Both connectors used to carry their own copy of this parsing, and the copies disagreed on
 * every failure: one returned {@code null} for a malformed list (which the caller then treated as
 * "mapping disabled", silently routing every topic to a same-named table) while throwing for a bad
 * table name, and the other threw for everything. This is the strict behaviour, for both: a
 * malformed mapping is a configuration error and fails loudly rather than changing routing behind
 * the operator's back.
 *
 * <p>Entries split on the <i>first</i> {@code ':'}, so a value may itself contain a colon. Duplicate
 * keys are rejected rather than silently last-one-wins -- a repeated key is always a config mistake,
 * and which of the two mappings takes effect is not something an operator should have to guess.
 *
 * <p>Values are returned unvalidated; callers apply whatever naming rules their side requires
 * (the sink, for instance, additionally checks that each value is a legal StarRocks table name).
 */
public final class KeyValueListParser {

    private KeyValueListParser() {
    }

    /**
     * @param configKey the config property name, used only to build the {@link ConfigException}
     * @param raw       the raw config value; {@code null} or blank yields an empty map
     * @return the parsed pairs, in the order they appeared
     * @throws ConfigException if any entry lacks a {@code ':'}, has an empty side, or repeats a key
     */
    public static Map<String, String> parse(String configKey, String raw) {
        Map<String, String> result = new LinkedHashMap<>();
        if (raw == null || raw.trim().isEmpty()) {
            return result;
        }
        for (String rawEntry : raw.split(",")) {
            String entry = rawEntry.trim();
            if (entry.isEmpty()) {
                continue;
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
