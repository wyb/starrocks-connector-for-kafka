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
 * Entry point for the StarRocks CDC source connector: validates configuration, runs fail-fast
 * preflight checks against every captured table, and shards the table list round-robin across
 * {@code maxTasks} {@link StarRocksCdcSourceTask} instances.
 *
 * <p>Preflight, run once from {@link #start}, rejects a configuration outright rather than
 * letting a task discover the problem mid-stream:
 * <ul>
 *   <li>a table using the UNIQUE KEY model, which CHANGES does not support;</li>
 *   <li>a PRIMARY KEY table that does not have {@code enable_change_data_capture} turned on --
 *       the error message names the exact {@code ALTER TABLE ... SET (...)} statement that fixes
 *       it, so the failure is directly actionable;</li>
 *   <li>a table with a column named {@code __CHANGE_TYPE__} or {@code __ROW_VERSION__}, which
 *       would collide with the CDC metadata pseudo-columns appended to every CHANGES read (see
 *       {@code StarRocksJdbcClient#streamChanges});</li>
 *   <li>bookmark meta functions being disabled cluster-wide, proven by actually creating and
 *       releasing one throwaway bookmark under a holder id of the probe's own -- see {@link
 *       #probeBookmarkFunctions}.</li>
 * </ul>
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
     * Proves that bookmark meta functions actually work, by creating one short-TTL bookmark on the
     * first captured table and releasing it again.
     *
     * <p>Worth its own round trip because {@code Config.enable_bookmark_meta_functions} defaults to
     * {@code false} in StarRocks, making this the single likeliest first-run blocker -- and without
     * this probe the configuration validates, tasks start, and the real cause only surfaces two
     * {@code getCause()} levels inside a per-table poll failure after the client's silent retries.
     * The release is best-effort: it is the create that proves the gate is open, and a leaked
     * probe bookmark expires within {@link #PROBE_BOOKMARK_TTL_MS}.
     *
     * <p><b>The probe's holder id must differ from the tasks' holder id -- do not "simplify" the
     * {@link #PROBE_HOLDER_SUFFIX} away.</b> {@code bookmark_create} is idempotent per holder: when
     * the table's partition meta is unchanged <i>and</i> the given holder already references the
     * table's newest bookmark, FE's {@code TableBookmarkTracker.create} raises
     * {@code AlreadyAtLatestException} and {@code MetaFunctions.bookmark_create} turns that into a
     * plain return of the <i>existing</i> bookmark id (this is exactly the connector's idle-dedup
     * behavior, see {@code StarRocksCdcSourceTask#poll}). So if the probe reused the task holder
     * {@code kc:<name>}, then on any restart of {@link #start} -- worker restart, config edit,
     * rebalance -- where the first captured table has produced no new version since its last
     * committed bookmark {@code B}, the probe would be handed back that still-held {@code B} and
     * its release would drop the task's own reference to it, deleting {@code B} once the tracker
     * empties. The task would then restore durable offset {@code B}, issue
     * {@code [_CHANGES_B_head_]}, and get "bookmark B not found": a task killed on every restart
     * under {@code policy=fail}, or a silent full re-read of the table under
     * {@code policy=resnapshot}. With a distinct holder, {@code create} takes the
     * {@code AcquireReference} branch instead and the release drops only the probe's own reference,
     * leaving the task's reference -- and therefore the bookmark itself -- intact.
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
