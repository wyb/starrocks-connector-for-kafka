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
 * <p>Preflight rejects, at {@link #start} rather than mid-stream: the UNIQUE KEY model, which
 * CHANGES does not support; a PRIMARY KEY table without {@code enable_change_data_capture} (the
 * message names the exact ALTER); a column colliding with {@code __CHANGE_TYPE__} or
 * {@code __ROW_VERSION__}; a column whose type cannot be exported at all; and bookmark meta
 * functions being disabled, proven by {@link #probeBookmarkFunctions}.</p>
 * Any other {@link SQLException} encountered while probing a table (e.g. the table does not
 * exist) is likewise wrapped into a {@link ConnectException} rather than left to propagate raw.
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
     * Creates and releases one short-TTL bookmark, proving the meta functions actually work.
     * {@code enable_bookmark_meta_functions} defaults to false, so this is the likeliest first-run
     * blocker; without the probe it surfaces two {@code getCause()} levels inside a poll failure.
     * The release is best-effort -- the create is what proves the gate is open.
     *
     * <p><b>The probe's holder must differ from the tasks' -- do not "simplify"
     * {@link #PROBE_HOLDER_SUFFIX} away.</b> {@code bookmark_create} is idempotent per holder: on an
     * unchanged table it returns the holder's <i>existing</i> bookmark. Sharing the holder would
     * hand the probe the task's committed bookmark {@code B}, and releasing it would delete
     * {@code B} -- so every restart of an idle table would then fail with "bookmark B not found",
     * or silently re-read the whole table under {@code policy=resnapshot}.
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
            // TABLE_MODEL is FE's KeysType enum rendered via toString(): the real runtime value
            // is "UNIQUE_KEYS" (InformationSchemaDataSource sets table_model from
            // olapTable.getKeysType().toString(), and the KeysType enum has no toString()
            // override), while some docs describe it as "UNQ_KEYS". Both spellings are checked
            // so this guard cannot go inert against either one.
            if (model != null && (model.contains("UNQ") || model.contains("UNIQUE"))) {
                throw new ConnectException(
                        "table " + db + "." + t + " uses UNIQUE KEY model, which CHANGES does not support");
            }
            if (model != null && (model.contains("PRIMARY") || model.contains("PRI"))) {
                if (!client.cdcPropertyEnabled(db, t)) {
                    throw new ConnectException(
                            "primary key table " + db + "." + t + " does not have change data capture enabled; "
                                    + "run: ALTER TABLE " + db + "." + t
                                    + " SET (\"enable_change_data_capture\" = \"true\")");
                }
            }
            for (ColumnMeta col : client.fetchColumns(db, t)) {
                if (CHANGE_TYPE_COLUMN.equals(col.name) || ROW_VERSION_COLUMN.equals(col.name)) {
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
                if ("unknown".equals(col.srDataType)) {
                    LOG.warn("Column {}.{}.{} has a type this connector does not recognise (declared as: {});"
                                    + " it will be carried as text. This usually means StarRocks added a type.",
                            db, t, col.name, col.srColumnType);
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
            // Defensive only: TABLE_NAMES is a required config, so StarRocksCdcSourceConfig's
            // ConfigDef validation already rejects an empty table list before start() completes.
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

    @Override
    public void stop() {
    }
}
