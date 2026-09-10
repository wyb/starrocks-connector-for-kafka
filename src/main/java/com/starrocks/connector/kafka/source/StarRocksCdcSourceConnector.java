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

import com.starrocks.connector.kafka.common.Version;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Validates configuration, runs fail-fast preflight against every captured table, and shards the
 * table list round-robin across {@code maxTasks} tasks.
 *
 * <p>Preflight rejects at {@link #start} rather than mid-stream: the UNIQUE KEY model; a PRIMARY KEY
 * table without {@code enable_change_data_capture} (the message names the exact ALTER); a column
 * colliding with {@code __CHANGE_TYPE__} or {@code __ROW_VERSION__}; a column whose type cannot be
 * exported; and disabled bookmark meta functions, proven by {@link #probeBookmarkFunctions}.
 */
public class StarRocksCdcSourceConnector extends SourceConnector {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksCdcSourceConnector.class);

    // The two CDC metadata pseudo-columns appended to every CHANGES projection; see
    // StarRocksJdbcClient#streamChanges. A captured table must not declare a real column with
    // either name, or the two would be indistinguishable downstream.
    private static final String CHANGE_TYPE_COLUMN = "__CHANGE_TYPE__";
    private static final String ROW_VERSION_COLUMN = "__ROW_VERSION__";

    // TTL of the throwaway preflight bookmark. Short, because it is released immediately and only
    // needs to survive its own round trip: if the release fails, it expires on its own shortly.
    private static final long PROBE_BOOKMARK_TTL_MS = 60_000L;

    // Suffix that makes the preflight probe's holder id distinct from the one the tasks use.
    // See probeBookmarkFunctions() for why the two must never be the same string.
    private static final String PROBE_HOLDER_SUFFIX = ":preflight";

    private Map<String, String> props;

    /** Test injection point: test subclasses override to return a scripted fake. */
    protected CdcClient createClient(StarRocksCdcSourceConfig cfg) {
        return new StarRocksJdbcClient(cfg);
    }

    @Override
    public String version() {
        return Version.get();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return StarRocksCdcSourceTask.class;
    }

    @Override
    public ConfigDef config() {
        return StarRocksCdcSourceConfig.CONFIG_DEF;
    }

    /**
     * Exactly-once is offered only with {@code source.snapshot.mode=no_snapshot}.
     *
     * <p>Snapshot rows carry {@code snapshot_done=false} and restore trusts only {@code true}, so a
     * committed snapshot is still redone whole on restart -- deliberate, but under exactly-once that
     * redo is duplicates the consumer has already seen. A CHANGES window has no such marker: the
     * offset it commits is the bookmark to resume from, written in the same transaction.
     *
     * <p>Read from the raw properties rather than through {@link StarRocksCdcSourceConfig}: the
     * herder calls this while validating, so a half-filled config must not throw here.
     */
    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> props) {
        String mode = props.getOrDefault(StarRocksCdcSourceConfig.SNAPSHOT_MODE,
                StarRocksCdcSourceConfig.SNAPSHOT_MODE_INITIAL);
        return StarRocksCdcSourceConfig.SNAPSHOT_MODE_NO_SNAPSHOT.equals(mode.trim())
                ? ExactlyOnceSupport.SUPPORTED
                : ExactlyOnceSupport.UNSUPPORTED;
    }

    @Override
    public void start(Map<String, String> props) {
        StarRocksCdcSourceConfig config = new StarRocksCdcSourceConfig(props);
        this.props = props;

        String db = config.databaseName();
        List<String> tables = config.tableNames();
        CdcClient client = createClient(config);
        try {
            for (String t : tables) {
                preflightCheckTable(client, db, t);
            }
            if (!tables.isEmpty()) {
                probeBookmarkFunctions(client, config, db, tables.get(0));
            }
        } finally {
            client.close();
        }
        LOG.info("CDC source connector preflight passed for {} table(s) in database {}: {}",
                tables.size(), db, tables);
    }

    /**
     * Creates and releases one short-TTL bookmark, proving the meta functions work.
     * {@code enable_bookmark_meta_functions} defaults to false, so this is the likeliest first-run
     * blocker; without the probe it surfaces two {@code getCause()} levels inside a poll failure.
     *
     * <p><b>The probe's holder must differ from the tasks' -- do not "simplify"
     * {@link #PROBE_HOLDER_SUFFIX} away.</b> {@code bookmark_create} is idempotent per holder, so a
     * shared holder would hand the probe the task's committed bookmark and the release would delete
     * it, failing every restart of an idle table with "bookmark not found".
     */
    private void probeBookmarkFunctions(CdcClient client, StarRocksCdcSourceConfig config, String db, String table) {
        String holder = config.holderId(props.getOrDefault("name", "default")) + PROBE_HOLDER_SUFFIX;
        long bookmarkId;
        try {
            bookmarkId = client.bookmarkCreate(db, table, holder, PROBE_BOOKMARK_TTL_MS);
        } catch (SQLException e) {
            throw new ConnectException(
                    "Failed to create a bookmark on " + db + "." + table + "; bookmark meta functions are most "
                            + "likely disabled (Config.enable_bookmark_meta_functions defaults to false). On the FE "
                            + "leader run: ADMIN SET FRONTEND CONFIG (\"enable_bookmark_meta_functions\" = \"true\") "
                            + "-- and make sure the connector user holds OPERATE ON SYSTEM.", e);
        }
        try {
            client.bookmarkRelease(db, table, bookmarkId, holder);
        } catch (SQLException e) {
            LOG.warn("Failed to release preflight probe bookmark {} for {}.{}; it expires in {} ms",
                    bookmarkId, db, table, PROBE_BOOKMARK_TTL_MS, e);
        }
    }

    /**
     * Runs every preflight check for one table, wrapping any {@link SQLException} raised while
     * probing it (including "table not found") into a {@link ConnectException} that names the
     * table.
     */
    private void preflightCheckTable(CdcClient client, String db, String t) {
        try {
            String model = client.fetchTableModel(db, t);
            // Empty, not null, is what "unknown" looks like: InformationSchemaDataSource sets
            // table_model only after casting to OlapTable, so a view or an external table leaves the
            // thrift field unset and the BE fills the column with "". Treating that as permission
            // would let every check below pass and the table fail later, at bookmark_create.
            if (model == null || model.trim().isEmpty()) {
                throw new ConnectException(
                        "could not determine the table model of " + db + "." + t
                                + " (information_schema.tables_config reports it empty), so this connector"
                                + " cannot tell whether the table is capturable. Views, materialized views"
                                + " and external tables have no table model; capture the base table instead.");
            }
            // TABLE_MODEL is KeysType.toString(), so the live value is "UNIQUE_KEYS"; some docs
            // spell it "UNQ_KEYS". Both are checked so the guard cannot go inert against either.
            if (model.contains("UNQ") || model.contains("UNIQUE")) {
                throw new ConnectException(
                        "table " + db + "." + t + " uses UNIQUE KEY model, which CHANGES does not support");
            }
            // "PRIMARY" itself contains "PRI", so the shorter test alone covers both spellings.
            // UNIQUE above needs two because "UNIQUE_KEYS" does not contain "UNQ".
            // AGG needs no warning: rows sharing an aggregate key are folded into one, so the key
            // identifies a row exactly as a primary key does. DUP's key is a sort key and admits
            // duplicates, which is fine for partitioning but not for compaction.
            if (model.contains("DUP")) {
                LOG.warn("Table {}.{} uses the DUPLICATE KEY model, whose key columns are a sort key "
                        + "and are not unique, so several rows can share one Kafka key. Partitioning "
                        + "and per-key ordering still hold; log compaction does not -- it would drop "
                        + "rows that are not duplicates, and with {}=true a tombstone would delete "
                        + "every row sharing the deleted row's key.",
                        db, t, StarRocksCdcSourceConfig.TOMBSTONES_ON_DELETE);
            }
            // && short-circuits, so cdcPropertyEnabled's query runs only for a primary key table.
            if (model.contains("PRI") && !client.cdcPropertyEnabled(db, t)) {
                throw new ConnectException(
                        "primary key table " + db + "." + t + " does not have change data capture enabled; "
                                + "run: ALTER TABLE " + db + "." + t
                                + " SET (\"enable_change_data_capture\" = \"true\")");
            }
            for (ColumnMeta col : client.fetchColumns(db, t)) {
                // Case-insensitive, matching StarRocks: ChangesMetaDescriptor.resolve compares with
                // CASE_INSENSITIVE_ORDER and renames its own pseudo-column on a collision, so a
                // lowercase __change_type__ would leave our bare projection reading the user's column.
                if (CHANGE_TYPE_COLUMN.equalsIgnoreCase(col.name) || ROW_VERSION_COLUMN.equalsIgnoreCase(col.name)) {
                    throw new ConnectException(
                            "table " + db + "." + t + " has a column named " + col.name
                                    + " which collides with a CDC metadata column");
                }
                // Aggregate sketches, not values: a plain SELECT yields nothing a consumer can
                // interpret. Without this the connector starts happily and streams that non-value
                // forever, which from the outside looks like working.
                if (ColumnMetadataReader.isNonExportable(col.srDataType)) {
                    throw new ConnectException(
                            "table " + db + "." + t + " has column '" + col.name + "' of type "
                                    + col.srDataType + ", whose value cannot be exported by a SELECT"
                                    + " -- it is an aggregate sketch, not a value. Capture a view that"
                                    + " projects only the columns you need, or remove this column from"
                                    + " the captured table.");
                }
                // Not fatal -- text preserves the value -- but it means StarRocks grew a type this
                // connector was never told about, worth saying once at startup.
                if (ColumnMetadataReader.isUnrecognized(col.srDataType)) {
                    LOG.warn("Column {}.{}.{} has type '{}' (declared as: {}), which this connector does not"
                                    + " recognise; it will be carried as text. This usually means StarRocks"
                                    + " added a type.",
                            db, t, col.name, col.srDataType, col.srColumnType);
                }
            }
        } catch (SQLException e) {
            throw new ConnectException("Failed CDC preflight check for table " + db + "." + t, e);
        }
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<String> tables = new StarRocksCdcSourceConfig(props).tableNames();
        if (tables.isEmpty()) {
            // Unreachable: the config constructor above rejects a table list that names nothing,
            // so this branch only survives a future change that relaxes that.
            return new ArrayList<>();
        }

        int groups = Math.min(maxTasks, tables.size());
        List<List<String>> groupTables = new ArrayList<>(groups);
        for (int i = 0; i < groups; i++) {
            groupTables.add(new ArrayList<>());
        }
        for (int i = 0; i < tables.size(); i++) {
            groupTables.get(i % groups).add(tables.get(i));
        }

        List<Map<String, String>> result = new ArrayList<>(groups);
        for (List<String> group : groupTables) {
            Map<String, String> taskProps = new HashMap<>(props);
            taskProps.put(StarRocksCdcSourceConfig.TASK_TABLES, String.join(",", group));
            result.add(taskProps);
        }
        return result;
    }

    /**
     * Validates what the offsets REST API is about to write. Connect writes the payload to the
     * offset store whether or not a connector overrides this, so without the check a malformed one
     * -- {@code bookmark_id} quoted as a string, say -- lands silently and only surfaces when a task
     * restarts and reads it back as "no position at all".
     */
    @Override
    public boolean alterOffsets(Map<String, String> connectorConfig,
                                Map<Map<String, ?>, Map<String, ?>> offsets) {
        for (Map.Entry<Map<String, ?>, Map<String, ?>> entry : offsets.entrySet()) {
            Map<String, ?> offset = entry.getValue();
            if (offset == null) {
                // A reset, and DELETE /offsets sends every stored partition this way. Validating
                // those would leave a partition whose shape this connector no longer writes with no
                // supported way to clear it, since a reset is the only way.
                continue;
            }
            Map<String, ?> partition = entry.getKey();
            requireNonEmptyString(partition, OffsetState.KEY_DB, "partition");
            requireNonEmptyString(partition, OffsetState.KEY_TABLE, "partition");
            Object bookmarkId = offset.get(OffsetState.KEY_BOOKMARK_ID);
            if (!(bookmarkId instanceof Number) || ((Number) bookmarkId).longValue() < 0) {
                throw new ConnectException("offset for " + partition + " must carry "
                        + OffsetState.KEY_BOOKMARK_ID + " as a non-negative number, not "
                        + describe(bookmarkId) + ". Quote nothing: {\"" + OffsetState.KEY_BOOKMARK_ID
                        + "\": 11955, \"" + OffsetState.KEY_SNAPSHOT_DONE + "\": true}");
            }
            Object snapshotDone = offset.get(OffsetState.KEY_SNAPSHOT_DONE);
            if (!(snapshotDone instanceof Boolean)) {
                throw new ConnectException("offset for " + partition + " must carry "
                        + OffsetState.KEY_SNAPSHOT_DONE + " as a boolean, not " + describe(snapshotDone)
                        + ". false re-reads the table from scratch; true resumes CHANGES from "
                        + OffsetState.KEY_BOOKMARK_ID + ".");
            }
        }
        return true;
    }

    private static void requireNonEmptyString(Map<String, ?> map, String key, String what) {
        Object value = map == null ? null : map.get(key);
        if (!(value instanceof String) || ((String) value).isEmpty()) {
            throw new ConnectException(what + " " + map + " must carry " + key
                    + " as a non-empty string, not " + describe(value));
        }
    }

    /** Names the offending value with its type, since "11955" and 11955 print the same. */
    private static String describe(Object value) {
        return value == null ? "null" : value.getClass().getSimpleName() + " " + value;
    }

    @Override
    public void stop() {
    }
}
