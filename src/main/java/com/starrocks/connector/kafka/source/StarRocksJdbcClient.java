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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * {@link CdcClient} over JDBC. The query layer only: {@link FeConnection},
 * {@link ColumnMetadataReader} and {@link RowExtractor} sit underneath it.
 *
 * <p><b>Not thread-safe</b>, because {@link FeConnection} is not; drive one client from one thread.
 * Connection behaviour is covered by the integration smoke test, not by unit tests.
 */
public class StarRocksJdbcClient implements CdcClient {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String CDC_PROPERTY = "enable_change_data_capture";

    private final FeConnection connection;
    private final ColumnMetadataReader columns;

    public StarRocksJdbcClient(StarRocksCdcSourceConfig config) {
        this.connection = new FeConnection(config);
        this.columns = new ColumnMetadataReader(connection);
    }

    @Override
    public long bookmarkCreate(String db, String table, String holder, long ttlMs) throws SQLException {
        String sql = SqlBuilder.bookmarkCreateSql(db, table, holder, ttlMs);
        String result = connection.executeOnLeader(sql);
        // executeOnLeader returns null for an empty result set, and a raw parseLong would then
        // throw NumberFormatException -- unchecked, so it escapes poll()'s catch (SQLException)
        // and kills the task instead of being handled as the SQL failure it is.
        if (result == null) {
            throw new SQLException("bookmark_create returned no bookmark id for " + db + "." + table);
        }
        try {
            return Long.parseLong(result.trim());
        } catch (NumberFormatException e) {
            throw new SQLException(
                    "bookmark_create returned a non-numeric bookmark id for " + db + "." + table + ": " + result, e);
        }
    }

    @Override
    public long bookmarkRenew(String db, String table, long bookmarkId, String holder, long ttlMs)
            throws SQLException {
        String sql = SqlBuilder.bookmarkRenewSql(db, table, bookmarkId, holder, ttlMs);
        String result = connection.executeOnLeader(sql);
        if (result == null) {
            throw new SQLException("bookmark_renew returned no effective ttl for " + db + "." + table
                    + " bookmark " + bookmarkId);
        }
        try {
            return Long.parseLong(result.trim());
        } catch (NumberFormatException e) {
            throw new SQLException("bookmark_renew returned a non-numeric effective ttl for " + db + "." + table
                    + " bookmark " + bookmarkId + ": " + result, e);
        }
    }

    @Override
    public void bookmarkRelease(String db, String table, long bookmarkId, String holder) throws SQLException {
        connection.executeOnLeader(SqlBuilder.bookmarkReleaseSql(db, table, bookmarkId, holder));
    }

    @Override
    public List<ColumnMeta> fetchColumns(String db, String table) throws SQLException {
        return columns.fetchColumns(db, table);
    }

    /**
     * The key columns of the table, whatever its model. AGG and DUP tables have key columns too --
     * FE just does not publish them through {@code tables_config.PRIMARY_KEY} -- and using them
     * puts every row of one key in one Kafka partition, which is what per-key ordering needs. A DUP
     * key is not unique; the preflight warning says so, because log compaction then is not safe.
     */
    @Override
    public List<String> fetchKeyColumns(String db, String table) throws SQLException {
        String sql = SqlBuilder.keyColumnsSql(db, table);
        try {
            Connection c = connection.get();
            try (Statement stmt = c.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                List<String> result = new ArrayList<>();
                while (rs.next()) {
                    String name = rs.getString("COLUMN_NAME");
                    if (name != null && !name.isEmpty()) {
                        result.add(name);
                    }
                }
                return result;
            }
        } catch (SQLException e) {
            connection.closeIfBroken(e);
            throw e;
        }
    }

    @Override
    public String fetchTableModel(String db, String table) throws SQLException {
        return fetchTableConfigRow(db, table).tableModel;
    }

    @Override
    public boolean cdcPropertyEnabled(String db, String table) throws SQLException {
        return changeDataCaptureEnabled(db, table, fetchTableConfigRow(db, table).properties);
    }

    /**
     * A missing key is false -- FE emits it only for a cloud-native primary-key table, which is the
     * only kind this is asked about. An unreadable value throws rather than answering false, which
     * would have preflight tell an operator to switch on a property that is already on.
     */
    static boolean changeDataCaptureEnabled(String db, String table, String propertiesJson) throws SQLException {
        if (propertiesJson == null || propertiesJson.trim().isEmpty()) {
            throw new SQLException("information_schema.tables_config.PROPERTIES is empty for " + db + "." + table
                    + ", so it cannot be told whether change data capture is enabled");
        }
        JsonNode root;
        try {
            root = JSON.readTree(propertiesJson);
        } catch (IOException e) {
            throw new SQLException("information_schema.tables_config.PROPERTIES for " + db + "." + table
                    + " is not readable as JSON: " + propertiesJson, e);
        }
        if (root == null || !root.isObject()) {
            throw new SQLException("information_schema.tables_config.PROPERTIES for " + db + "." + table
                    + " is not a JSON object: " + propertiesJson);
        }
        JsonNode value = root.get(CDC_PROPERTY);
        return value != null && "true".equalsIgnoreCase(value.asText());
    }

    @Override
    public void streamSnapshot(String db, String table, List<ColumnMeta> cols, long bookmarkId, RowConsumer consumer)
            throws SQLException, NonTrackableException {
        String sql = SqlBuilder.snapshotSql(db, table, columnNames(cols), bookmarkId);
        try {
            Connection c = connection.get();
            try (Statement stmt = c.createStatement()) {
                connection.applyStreamingFetchSize(stmt);
                Calendar utc = RowExtractor.newUtcCalendar();
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        consumer.accept(RowExtractor.extractRow(rs, cols, utc));
                    }
                }
            }
        } catch (SQLException e) {
            connection.closeIfBroken(e);
            // Same classification as streamChanges: a bookmark the snapshot pinned can be dropped,
            // rewritten or resharded under a long scan, and that is exactly what resnapshot exists
            // for. Rethrowing raw made policy=resnapshot inapplicable to the whole snapshot phase.
            NonTrackableException nte = NonTrackableException.classify(e);
            if (nte != null) {
                throw nte;
            }
            throw e;
        }
    }

    @Override
    public void streamChanges(String db, String table, List<ColumnMeta> cols, long base, long head,
                              ChangeRowConsumer consumer) throws SQLException, NonTrackableException {
        String sql = SqlBuilder.changesSql(db, table, columnNames(cols), base, head);
        try {
            Connection c = connection.get();
            try (Statement stmt = c.createStatement()) {
                connection.applyStreamingFetchSize(stmt);
                Calendar utc = RowExtractor.newUtcCalendar();
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    // The last two projected columns are always __CHANGE_TYPE__ (int) and
                    // __ROW_VERSION__ (long); extractRow only covers the leading business columns,
                    // so they are peeled off directly here.
                    int changeTypeIdx = cols.size() + 1;
                    int rowVersionIdx = cols.size() + 2;
                    while (rs.next()) {
                        Object[] row = RowExtractor.extractRow(rs, cols, utc);
                        int changeType = rs.getInt(changeTypeIdx);
                        long rowVersion = rs.getLong(rowVersionIdx);
                        consumer.accept(row, changeType, rowVersion);
                    }
                }
            }
        } catch (SQLException e) {
            connection.closeIfBroken(e);
            NonTrackableException nte = NonTrackableException.classify(e);
            if (nte != null) {
                throw nte;
            }
            throw e;
        }
    }

    @Override
    public void close() {
        connection.close();
    }

    private TableConfigRow fetchTableConfigRow(String db, String table) throws SQLException {
        String sql = SqlBuilder.tableConfigSql(db, table);
        try {
            Connection c = connection.get();
            try (Statement stmt = c.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (!rs.next()) {
                    throw new SQLException("table not found: " + db + "." + table);
                }
                return new TableConfigRow(rs.getString("TABLE_MODEL"),
                        rs.getString("PROPERTIES"));
            }
        } catch (SQLException e) {
            connection.closeIfBroken(e);
            throw e;
        }
    }

    static List<String> columnNames(List<ColumnMeta> cols) {
        List<String> names = new ArrayList<>(cols.size());
        for (ColumnMeta col : cols) {
            names.add(col.name);
        }
        return names;
    }

    private static final class TableConfigRow {
        final String tableModel;
        final String properties;

        TableConfigRow(String tableModel, String properties) {
            this.tableModel = tableModel;
            this.properties = properties;
        }
    }
}
