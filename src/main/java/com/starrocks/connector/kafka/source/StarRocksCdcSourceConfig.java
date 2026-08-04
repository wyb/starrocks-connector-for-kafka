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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration for the StarRocks CDC source connector and its tasks.
 */
public class StarRocksCdcSourceConfig extends AbstractConfig {

    // JDBC URL of the StarRocks FE MySQL protocol endpoint(s).
    public static final String JDBC_URL = "starrocks.jdbc.url";
    // The name of the source StarRocks database.
    public static final String DATABASE_NAME = "starrocks.database.name";
    // The username used to connect to StarRocks.
    public static final String USERNAME = "starrocks.username";
    // The password used to connect to StarRocks.
    public static final String PASSWORD = "starrocks.password";
    // Comma-separated list of StarRocks table names to capture changes from.
    public static final String TABLE_NAMES = "starrocks.table.names";
    // Optional mapping from table name to Kafka topic name, formatted as table:topic,table:topic.
    public static final String TABLE2TOPIC_MAP = "starrocks.table2topic.map";
    // The prefix used to derive a topic name for tables without an explicit topic mapping.
    public static final String TOPIC_PREFIX = "source.topic.prefix";
    // Controls whether an initial snapshot of the captured tables is taken before streaming changes.
    public static final String SNAPSHOT_MODE = "source.snapshot.mode";
    // The interval, in milliseconds, between successive polls for changes.
    public static final String POLL_INTERVALMS = "source.poll.intervalms";
    // The time-to-live, in milliseconds, for stored bookmarks before they are considered stale.
    public static final String BOOKMARK_TTLMS = "source.bookmark.ttlms";
    // The action to take when a captured table becomes non-trackable.
    public static final String NONTRACKABLE_POLICY = "source.nontrackable.policy";
    // Whether to emit only the final net change per key instead of every intermediate change.
    public static final String NETCHANGES = "source.netchanges";
    // Whether to emit an additional tombstone record (null value) following a delete record.
    public static final String TOMBSTONES_ON_DELETE = "source.tombstones.on.delete";
    // The number of times to retry a failed source operation before giving up.
    public static final String MAXRETRIES = "source.maxretries";
    // The period of time, in milliseconds, after which a connection attempt to StarRocks times out.
    public static final String CONNECT_TIMEOUTMS = "connect.timeoutms";

    // Internal task-sharding key used only to pass the assigned tables from the Connector to a Task;
    // it is not part of CONFIG_DEF and must never be surfaced to users.
    public static final String TASK_TABLES = "task.tables";

    public static final ConfigDef CONFIG_DEF = newConfigDef();

    private final Map<String, String> table2Topic;

    public StarRocksCdcSourceConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
        this.table2Topic = parseTable2Topic(getString(TABLE2TOPIC_MAP));
        rejectNoSnapshotWithResnapshot();
    }

    /**
     * Rejects {@code source.snapshot.mode=no_snapshot} together with {@code
     * source.nontrackable.policy=resnapshot}, which would be silently lossy rather than
     * self-healing: the resnapshot path discards the table's position and pins a fresh bookmark at
     * the current version, and with no snapshot to rebuild from it reads nothing at all -- so every
     * change between the unusable base and that new bookmark is dropped while the operator believes
     * the table healed itself.
     */
    private void rejectNoSnapshotWithResnapshot() {
        if ("no_snapshot".equals(snapshotMode()) && "resnapshot".equals(nonTrackablePolicy())) {
            throw new ConfigException(
                    NONTRACKABLE_POLICY + "=resnapshot cannot be combined with " + SNAPSHOT_MODE + "=no_snapshot: "
                            + "resnapshot rebuilds a table's position from a fresh snapshot, so with snapshots "
                            + "disabled it would silently drop every change between the unusable base bookmark and "
                            + "the new one. Use " + SNAPSHOT_MODE + "=initial, or " + NONTRACKABLE_POLICY + "=fail.");
        }
    }

    public static ConfigDef newConfigDef() {
        return new ConfigDef()
                .define(
                        JDBC_URL,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        "JDBC URL of the StarRocks FE MySQL protocol endpoint(s)."
                ).define(
                        DATABASE_NAME,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        "The name of the source StarRocks database."
                ).define(
                        USERNAME,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        "The username used to connect to StarRocks."
                ).define(
                        PASSWORD,
                        ConfigDef.Type.PASSWORD,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        "The password used to connect to StarRocks."
                ).define(
                        TABLE_NAMES,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        "Comma-separated list of StarRocks table names to capture changes from."
                ).define(
                        TABLE2TOPIC_MAP,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.LOW,
                        "Optional mapping from table name to Kafka topic name, formatted as table:topic,table:topic."
                ).define(
                        TOPIC_PREFIX,
                        ConfigDef.Type.STRING,
                        "sr",
                        ConfigDef.Importance.MEDIUM,
                        "The prefix used to derive a topic name for tables without an explicit topic mapping."
                ).define(
                        SNAPSHOT_MODE,
                        ConfigDef.Type.STRING,
                        "initial",
                        ConfigDef.ValidString.in("initial", "no_snapshot"),
                        ConfigDef.Importance.MEDIUM,
                        "Controls whether an initial snapshot of the captured tables is taken before streaming changes."
                ).define(
                        POLL_INTERVALMS,
                        ConfigDef.Type.LONG,
                        5000L,
                        ConfigDef.Importance.MEDIUM,
                        "The interval, in milliseconds, between successive polls for changes."
                ).define(
                        BOOKMARK_TTLMS,
                        ConfigDef.Type.LONG,
                        604800000L,
                        ConfigDef.Importance.MEDIUM,
                        "The time-to-live, in milliseconds, for stored bookmarks before they are considered stale."
                ).define(
                        NONTRACKABLE_POLICY,
                        ConfigDef.Type.STRING,
                        "fail",
                        ConfigDef.ValidString.in("fail", "resnapshot"),
                        ConfigDef.Importance.MEDIUM,
                        "The action to take when a captured table becomes non-trackable."
                ).define(
                        NETCHANGES,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        "Whether to emit only the final net change per key instead of every intermediate change."
                ).define(
                        TOMBSTONES_ON_DELETE,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        "Whether to emit an additional tombstone record (null value) following a delete record."
                ).define(
                        MAXRETRIES,
                        ConfigDef.Type.INT,
                        3,
                        ConfigDef.Importance.LOW,
                        "The number of times to retry a failed source operation before giving up."
                ).define(
                        CONNECT_TIMEOUTMS,
                        ConfigDef.Type.INT,
                        1000,
                        ConfigDef.Importance.LOW,
                        "The period of time, in milliseconds, after which a connection attempt to StarRocks times out."
                );
    }

    /**
     * Parses and validates the TABLE2TOPIC_MAP value. Each non-empty entry must be split on the
     * first ':' into two non-empty segments; otherwise a ConfigException is raised.
     */
    private static Map<String, String> parseTable2Topic(String raw) {
        Map<String, String> result = new HashMap<>();
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
                throw new ConfigException(TABLE2TOPIC_MAP, raw,
                        "Entry '" + entry + "' is missing a ':' separator; expected table:topic.");
            }
            String table = entry.substring(0, idx).trim();
            String topic = entry.substring(idx + 1).trim();
            if (table.isEmpty() || topic.isEmpty()) {
                throw new ConfigException(TABLE2TOPIC_MAP, raw,
                        "Entry '" + entry + "' must have non-empty table and topic segments.");
            }
            result.put(table, topic);
        }
        return result;
    }

    public List<String> tableNames() {
        List<String> result = new ArrayList<>();
        for (String rawName : getString(TABLE_NAMES).split(",")) {
            String trimmed = rawName.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }

    public String topicFor(String table) {
        String mapped = table2Topic.get(table);
        if (mapped != null) {
            return mapped;
        }
        return topicPrefix() + "." + databaseName() + "." + table;
    }

    public String holderId(String connectorName) {
        return "kc:" + connectorName;
    }

    public String jdbcUrl() {
        return getString(JDBC_URL);
    }

    public String databaseName() {
        return getString(DATABASE_NAME);
    }

    public String username() {
        return getString(USERNAME);
    }

    public String password() {
        return getPassword(PASSWORD).value();
    }

    public String topicPrefix() {
        return getString(TOPIC_PREFIX);
    }

    public String snapshotMode() {
        return getString(SNAPSHOT_MODE);
    }

    public long pollIntervalMs() {
        return getLong(POLL_INTERVALMS);
    }

    public long bookmarkTtlMs() {
        return getLong(BOOKMARK_TTLMS);
    }

    public String nonTrackablePolicy() {
        return getString(NONTRACKABLE_POLICY);
    }

    public boolean netChanges() {
        return getBoolean(NETCHANGES);
    }

    public boolean tombstonesOnDelete() {
        return getBoolean(TOMBSTONES_ON_DELETE);
    }

    public int maxRetries() {
        return getInt(MAXRETRIES);
    }

    public int connectTimeoutMs() {
        return getInt(CONNECT_TIMEOUTMS);
    }
}
