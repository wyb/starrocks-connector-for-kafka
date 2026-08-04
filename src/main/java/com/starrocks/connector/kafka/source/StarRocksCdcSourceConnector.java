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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

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
 *       {@code StarRocksJdbcClient#streamChanges}).</li>
 * </ul>
 * Any other {@link SQLException} encountered while probing a table (e.g. the table does not
 * exist) is likewise wrapped into a {@link ConnectException} rather than left to propagate raw.
 */
public class StarRocksCdcSourceConnector extends SourceConnector {

    // The two CDC metadata pseudo-columns appended to every CHANGES projection; see
    // StarRocksJdbcClient#streamChanges. A captured table must not declare a real column with
    // either name, or the two would be indistinguishable downstream.
    private static final String CHANGE_TYPE_COLUMN = "__CHANGE_TYPE__";
    private static final String ROW_VERSION_COLUMN = "__ROW_VERSION__";

    private Map<String, String> props;

    /** Test injection point: test subclasses override to return a scripted fake. */
    protected CdcClient createClient(StarRocksCdcSourceConfig cfg) {
        return new StarRocksJdbcClient(cfg);
    }

    @Override
    public String version() {
        return "1.0";
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
        CdcClient client = createClient(config);
        try {
            for (String t : config.tableNames()) {
                preflightCheckTable(client, db, t);
            }
        } finally {
            client.close();
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
            if (model != null && model.contains("UNQ")) {
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
