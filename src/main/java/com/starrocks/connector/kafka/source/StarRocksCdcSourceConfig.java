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

import com.starrocks.connector.kafka.common.KeyValueListParser;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Configuration for the StarRocks CDC source connector and its tasks. Each key's user-facing
 * description lives once, in {@link #newConfigDef()}, which is what Connect surfaces.
 */
public class StarRocksCdcSourceConfig extends AbstractConfig {

    public static final String JDBC_URL = "starrocks.jdbc.url";
    public static final String DATABASE_NAME = "starrocks.database.name";
    public static final String USERNAME = "starrocks.username";
    public static final String PASSWORD = "starrocks.password";
    public static final String TABLE_NAMES = "starrocks.table.names";
    public static final String TABLE2TOPIC_MAP = "starrocks.table2topic.map";
    public static final String TOPIC_PREFIX = "source.topic.prefix";
    public static final String SNAPSHOT_MODE = "source.snapshot.mode";
    public static final String POLL_INTERVALMS = "source.poll.intervalms";
    public static final String BOOKMARK_TTLMS = "source.bookmark.ttlms";
    public static final String NONTRACKABLE_POLICY = "source.nontrackable.policy";
    public static final String TOMBSTONES_ON_DELETE = "source.tombstones.on.delete";
    public static final String MAXRETRIES = "source.maxretries";
    public static final String CONNECT_TIMEOUTMS = "connect.timeoutms";

    // Internal task-sharding key used only to pass the assigned tables from the Connector to a Task;
    // it is not part of CONFIG_DEF and must never be surfaced to users.
    public static final String TASK_TABLES = "task.tables";

    public static final ConfigDef CONFIG_DEF = newConfigDef();

    private final Map<String, String> table2Topic;

    public StarRocksCdcSourceConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
        this.table2Topic = parseTable2Topic(getString(TABLE2TOPIC_MAP));
        rejectEmptyTableList();
        rejectNoSnapshotWithResnapshot();
    }

    /**
     * ConfigDef only checks that {@link #TABLE_NAMES} is present, so a value of separators and
     * whitespace passes and then names nothing: preflight checks no table, no task is started, and
     * nothing is logged wrong -- a typo reads as a source with nothing to send.
     */
    private void rejectEmptyTableList() {
        if (tableNames().isEmpty()) {
            throw new ConfigException(TABLE_NAMES, getString(TABLE_NAMES),
                    "names no table; expected a comma-separated list of at least one table name.");
        }
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

    private static Map<String, String> parseTable2Topic(String raw) {
        return KeyValueListParser.parse(TABLE2TOPIC_MAP, raw);
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
