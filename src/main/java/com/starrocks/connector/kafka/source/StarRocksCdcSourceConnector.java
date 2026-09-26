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
 * Validates configuration, runs fail-fast preflight against every captured table, shards the table
 * list round-robin across {@code maxTasks} tasks, and vets the offsets the REST API is about to
 * write ({@link #alterOffsets}). {@link #exactlyOnceSupport} says when exactly-once is on offer.
 *
 * <p>Preflight rejects at {@link #start} rather than mid-stream: a table with no model (a view, an
 * external table) or one this connector does not know; the UNIQUE KEY model; a PRIMARY KEY table
 * without {@code enable_change_data_capture} (the message names the exact ALTER); a column colliding
 * with {@code __CHANGE_TYPE__} or {@code __ROW_VERSION__}; a column whose type cannot be exported;
 * and disabled bookmark meta functions, proven by {@link #probeBookmarkFunctions}.
 */
public class StarRocksCdcSourceConnector extends SourceConnector {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksCdcSourceConnector.class);

    /** The probe's bookmark is released right after creation; if that fails it expires on its own. */
    private static final long PROBE_BOOKMARK_TTL_MS = 60_000L;

    /** Keeps the probe's holder apart from the tasks'; {@link #probeBookmarkFunctions} says why. */
    private static final String PROBE_HOLDER_SUFFIX = ":preflight";

    private Map<String, String> props;
    private List<String> tables;

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
     * herder calls this while validating, so a half-filled config -- the key missing or null -- must
     * not throw here.
     */
    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> props) {
        String mode = props.get(StarRocksCdcSourceConfig.SNAPSHOT_MODE);
        return mode != null && StarRocksCdcSourceConfig.SNAPSHOT_MODE_NO_SNAPSHOT.equals(mode.trim())
                ? ExactlyOnceSupport.SUPPORTED
                : ExactlyOnceSupport.UNSUPPORTED;
    }

    @Override
    public void start(Map<String, String> props) {
        StarRocksCdcSourceConfig config = new StarRocksCdcSourceConfig(props);
        this.props = props;
        this.tables = config.tableNames();

        String db = config.databaseName();
        CdcClient client = createClient(config);
        try {
            for (String table : tables) {
                preflightCheckTable(client, db, table);
            }
            probeBookmarkFunctions(client, db, tables.get(0), config.holderId());
        } finally {
            client.close();
        }
        LOG.info("CDC source connector preflight passed for {} table(s) in database {}: {}",
                tables.size(), db, tables);
    }

    /**
     * Runs every preflight check for one table, wrapping any {@link SQLException} raised while
     * probing it (including "table not found") into a {@link ConnectException} that names the
     * table.
     */
    private void preflightCheckTable(CdcClient client, String db, String table) {
        try {
            checkModel(db, table, client.fetchTableConfig(db, table));
            checkColumns(db, table, client.fetchColumns(db, table));
        } catch (SQLException e) {
            throw new ConnectException("Failed CDC preflight check for table " + db + "." + table, e);
        }
    }

    /** Refuses a missing, unknown or UNIQUE KEY model, requires the CDC property on a primary key table,
     *  and warns that a DUPLICATE KEY is not unique. */
    private static void checkModel(String db, String table, TableConfig tableConfig) throws SQLException {
        switch (tableConfig.model) {
            case NONE:
                // Treating no model as permission would let the column checks pass and the table
                // fail later, at bookmark_create.
                throw new ConnectException(
                        "could not determine the table model of " + db + "." + table
                                + " (information_schema.tables_config reports it empty), so this connector"
                                + " cannot tell whether the table is capturable. Views, materialized views"
                                + " and external tables have no table model; capture the base table instead.");
            case OTHER:
                throw new ConnectException(
                        "table " + db + "." + table + " has table model '" + tableConfig.modelName
                                + "', which this connector does not know, so it cannot tell whether CHANGES"
                                + " can read it");
            case UNIQUE:
                throw new ConnectException(
                        "table " + db + "." + table + " uses UNIQUE KEY model, which CHANGES does not support");
            case DUPLICATE:
                // DUP's key is a sort key and admits duplicates: fine for partitioning, not for compaction.
                LOG.warn("Table {}.{} uses the DUPLICATE KEY model, whose key columns are a sort key "
                        + "and are not unique, so several rows can share one Kafka key. Partitioning "
                        + "and per-key ordering still hold; log compaction does not -- it would drop "
                        + "rows that are not duplicates, and with {}=true a tombstone would delete "
                        + "every row sharing the deleted row's key.",
                        db, table, StarRocksCdcSourceConfig.TOMBSTONES_ON_DELETE);
                break;
            case PRIMARY:
                // The only model that carries the property.
                if (!tableConfig.cdcEnabled()) {
                    throw new ConnectException(
                            "primary key table " + db + "." + table + " does not have change data capture "
                                    + "enabled; run: ALTER TABLE " + db + "." + table
                                    + " SET (\"" + TableConfig.CDC_PROPERTY + "\" = \"true\")");
                }
                break;
            case AGGREGATE:
                // Rows sharing an aggregate key are folded into one, so the key identifies a row
                // exactly as a primary key does: nothing to warn about.
                break;
        }
    }

    /** Refuses a column that collides with a CDC pseudo-column or holds an aggregate sketch; warns where
     *  one is carried as text. */
    private static void checkColumns(String db, String table, List<ColumnMeta> cols) {
        for (ColumnMeta col : cols) {
            // Case-insensitive, matching StarRocks: ChangesMetaDescriptor.resolve compares with
            // CASE_INSENSITIVE_ORDER and renames its own pseudo-column on a collision, so a lowercase
            // __change_type__ would leave changesSql's bare projection reading the user's column.
            if (SqlBuilder.CHANGE_TYPE_COLUMN.equalsIgnoreCase(col.name)
                    || SqlBuilder.ROW_VERSION_COLUMN.equalsIgnoreCase(col.name)) {
                throw new ConnectException(
                        "table " + db + "." + table + " has a column named " + col.name
                                + " which collides with a CDC metadata column");
            }
            // Aggregate sketches, not values: a plain SELECT yields nothing a consumer can interpret.
            if (ColumnMeta.isNonExportable(col.srDataType)) {
                throw new ConnectException(
                        "table " + db + "." + table + " has column '" + col.name + "' of type "
                                + col.srDataType + ", whose value cannot be exported by a SELECT"
                                + " -- it is an aggregate sketch, not a value. Leave this table out of "
                                + StarRocksCdcSourceConfig.TABLE_NAMES + ", or drop the column.");
            }
            // COLUMN_TYPE is a display string; when it does not parse the column is still
            // captured, as text, and this says so once.
            if (ColumnMeta.isComplex(col.srDataType) && col.type.kind == ColumnType.Kind.OPAQUE) {
                LOG.warn("Column {}.{}.{} is declared as '{}', which this connector could not parse"
                                + " into a nested schema; it will be carried as text.",
                        db, table, col.name, col.srColumnType);
            }
            // Not fatal -- text preserves the value -- but StarRocks grew a type this connector
            // was never told about, worth saying once at startup.
            if (ColumnMeta.isUnrecognized(col.srDataType)) {
                LOG.warn("Column {}.{}.{} has type '{}' (declared as: {}), which this connector does not"
                                + " recognise; it will be carried as text. This usually means StarRocks"
                                + " added a type.",
                        db, table, col.name, col.srDataType, col.srColumnType);
            }
        }
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
    private void probeBookmarkFunctions(CdcClient client, String db, String table, String taskHolder) {
        String holder = taskHolder + PROBE_HOLDER_SUFFIX;
        long bookmarkId;
        try {
            bookmarkId = client.bookmarkCreate(db, table, holder, PROBE_BOOKMARK_TTL_MS);
        } catch (SQLException e) {
            // The FE names the failed gate itself (functions disabled, no OPERATE privilege, not the
            // leader), so its message goes first; the flag is per FE and not persisted.
            throw new ConnectException(
                    "Failed to create a bookmark on " + db + "." + table + ": " + e.getMessage()
                            + ". The bookmark meta functions must be enabled on every FE, with"
                            + " ADMIN SET FRONTEND CONFIG (\"enable_bookmark_meta_functions\" = \"true\") and the same"
                            + " line in fe.conf to survive a restart, and the connector user needs OPERATE ON SYSTEM.", e);
        }
        try {
            client.bookmarkRelease(db, table, bookmarkId, holder);
        } catch (SQLException e) {
            LOG.warn("Failed to release preflight probe bookmark {} for {}.{}; it expires in {} ms",
                    bookmarkId, db, table, PROBE_BOOKMARK_TTL_MS, e);
        }
    }

    /** Round-robin: table i goes to task i mod min(maxTasks, tables). The config rejects an empty list. */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        int taskCount = Math.min(maxTasks, tables.size());
        List<Map<String, String>> result = new ArrayList<>(taskCount);
        for (int task = 0; task < taskCount; task++) {
            List<String> own = new ArrayList<>();
            for (int i = task; i < tables.size(); i += taskCount) {
                own.add(tables.get(i));
            }
            Map<String, String> taskProps = new HashMap<>(props);
            taskProps.put(StarRocksCdcSourceConfig.TASK_TABLES, String.join(",", own));
            result.add(taskProps);
        }
        return result;
    }

    /**
     * Validates what the offsets REST API is about to write. Connect writes the payload to the
     * offset store whether or not a connector overrides this, so without the check a malformed one
     * -- {@code bookmark_id} quoted as a string, say -- lands silently and only surfaces when a task
     * restarts and reads it back as "no position at all". A partition naming a table the config does
     * not capture is refused for the same reason: no task would ever read it.
     */
    @Override
    public boolean alterOffsets(Map<String, String> connectorConfig,
                                Map<Map<String, ?>, Map<String, ?>> offsets) {
        StarRocksCdcSourceConfig config = null; // built on first use, so a reset never depends on the config parsing
        for (Map.Entry<Map<String, ?>, Map<String, ?>> entry : offsets.entrySet()) {
            Map<String, ?> offset = entry.getValue();
            if (offset == null) {
                // A reset, and DELETE /offsets sends every stored partition this way. Validating
                // those would leave a partition whose shape this connector no longer writes with no
                // supported way to clear it, since a reset is the only way.
                continue;
            }
            Map<String, ?> partition = entry.getKey();
            requirePartitionString(partition, OffsetState.KEY_DB);
            requirePartitionString(partition, OffsetState.KEY_TABLE);
            if (partition.size() != 2) {
                // A task looks its partition up by Map equality, so any extra key matches nothing.
                throw new ConnectException("partition " + partition + " must carry exactly "
                        + OffsetState.KEY_DB + " and " + OffsetState.KEY_TABLE + "; with any other key no task reads it");
            }
            if (config == null) {
                config = new StarRocksCdcSourceConfig(connectorConfig);
            }
            if (!config.databaseName().equals(partition.get(OffsetState.KEY_DB))
                    || !config.tableNames().contains(partition.get(OffsetState.KEY_TABLE))) {
                throw new ConnectException("partition " + partition + " names no table this connector captures ("
                        + StarRocksCdcSourceConfig.DATABASE_NAME + "=" + config.databaseName() + ", "
                        + StarRocksCdcSourceConfig.TABLE_NAMES + "=" + config.tableNames()
                        + "), so no task would read the offset");
            }
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

    /** {@code partition} itself may be null: Connect passes a request's {@code "partition": null} through as is. */
    private static void requirePartitionString(Map<String, ?> partition, String key) {
        Object value = partition == null ? null : partition.get(key);
        if (!(value instanceof String) || ((String) value).isEmpty()) {
            throw new ConnectException("partition " + partition + " must carry " + key
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

    /** Test injection point: test subclasses override to return a scripted fake. */
    protected CdcClient createClient(StarRocksCdcSourceConfig config) {
        return new StarRocksJdbcClient(config);
    }
}
