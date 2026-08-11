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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * {@link CdcClient} implementation talking to a StarRocks FE over JDBC. The query layer only:
 * {@link FeConnection}, {@link ColumnMetadataReader} and {@link RowExtractor} sit underneath it,
 * each documented on itself.
 *
 * <p><b>Not thread-safe</b>, because {@link FeConnection} is not. Drive one client from a single
 * thread; {@code StarRocksCdcSourceTask} keeps all of its JDBC on the poll thread for this reason.
 *
 * <p>Connection behaviour is covered by the integration smoke test, not by unit tests.
 */
public class StarRocksJdbcClient implements CdcClient {

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
    public void bookmarkRelease(String db, String table, long bookmarkId, String holder) throws SQLException {
        connection.executeOnLeader(SqlBuilder.bookmarkReleaseSql(db, table, bookmarkId, holder));
    }

    @Override
    public List<ColumnMeta> fetchColumns(String db, String table) throws SQLException {
        return columns.fetchColumns(db, table);
    }

    @Override
    public List<String> fetchPrimaryKeys(String db, String table) throws SQLException {
        String raw = fetchTableConfigRow(db, table).primaryKey;
        List<String> result = new ArrayList<>();
        if (raw == null || raw.trim().isEmpty()) {
            return result;
        }
        for (String part : raw.split(",")) {
            String cleaned = stripBackticksAndWhitespace(part);
            if (!cleaned.isEmpty()) {
                result.add(cleaned);
            }
        }
        return result;
    }

    @Override
    public String fetchTableModel(String db, String table) throws SQLException {
        return fetchTableConfigRow(db, table).tableModel;
    }

    @Override
    public boolean cdcPropertyEnabled(String db, String table) throws SQLException {
        String sql = SqlBuilder.showCreateTableSql(db, table);
        try {
            Connection c = connection.get();
            try (Statement stmt = c.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (!rs.next()) {
                    throw new SQLException("table not found: " + db + "." + table);
                }
                String ddl = rs.getString(2);
                return ddl != null && ddl.contains("\"enable_change_data_capture\" = \"true\"");
            }
        } catch (SQLException e) {
            connection.closeIfBroken(e);
            throw e;
        }
    }

    @Override
    public void streamSnapshot(String db, String table, List<ColumnMeta> cols, long bookmarkId, RowConsumer consumer)
            throws SQLException {
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
                return new TableConfigRow(rs.getString("TABLE_MODEL"), rs.getString("PRIMARY_KEY"));
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

    private static String stripBackticksAndWhitespace(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (ch != '`' && !Character.isWhitespace(ch)) {
                sb.append(ch);
            }
        }
        return sb.toString();
    }

    private static final class TableConfigRow {
        final String tableModel;
        final String primaryKey;

        TableConfigRow(String tableModel, String primaryKey) {
            this.tableModel = tableModel;
            this.primaryKey = primaryKey;
        }
    }
}
