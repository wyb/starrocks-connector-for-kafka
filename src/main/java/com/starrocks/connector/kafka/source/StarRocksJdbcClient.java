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
import java.util.List;

/**
 * {@link CdcClient} over JDBC. The query layer only: {@link FeConnection},
 * {@link ColumnMetaReader} and {@link ValueReader} sit underneath it.
 *
 * <p><b>Not thread-safe</b>, because {@link FeConnection} is not; drive one client from one thread.
 * Connection behaviour is covered by the integration smoke test, not by unit tests.
 */
public class StarRocksJdbcClient implements CdcClient {

    private final FeConnection connection;
    private final ColumnMetaReader columns;

    public StarRocksJdbcClient(StarRocksCdcSourceConfig config) {
        this.connection = new FeConnection(config);
        this.columns = new ColumnMetaReader(connection);
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
    public List<Long> fetchHeldBookmarks(String db, String table, String holder) throws SQLException {
        TableConfig tableConfig = readTableConfig(db, table);
        if (tableConfig == null) {
            return new ArrayList<>(); // dropped, so nothing references it any more
        }
        String sql = SqlBuilder.heldBookmarksSql(tableConfig.tableId, holder);
        try {
            Connection c = connection.get();
            try (Statement stmt = c.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                List<Long> ids = new ArrayList<>();
                while (rs.next()) {
                    ids.add(rs.getLong("BOOKMARK_ID"));
                }
                return ids;
            }
        } catch (SQLException e) {
            connection.closeIfBroken(e);
            throw e;
        }
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
    public TableConfig fetchTableConfig(String db, String table) throws SQLException {
        TableConfig tableConfig = readTableConfig(db, table);
        if (tableConfig == null) {
            throw new SQLException("table not found: " + db + "." + table);
        }
        return tableConfig;
    }

    /** The table's {@code tables_config} row, or null when it has none. */
    private TableConfig readTableConfig(String db, String table) throws SQLException {
        String sql = SqlBuilder.tableConfigSql(db, table);
        try {
            Connection c = connection.get();
            try (Statement stmt = c.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (!rs.next()) {
                    return null;
                }
                return new TableConfig(db, table, rs.getLong("TABLE_ID"), rs.getString("TABLE_MODEL"),
                        rs.getString("PROPERTIES"));
            }
        } catch (SQLException e) {
            connection.closeIfBroken(e);
            throw e;
        }
    }

    @Override
    public void streamSnapshot(String db, String table, List<ColumnMeta> cols, long bookmarkId, RowConsumer consumer)
            throws SQLException, NonTrackableException {
        String sql = SqlBuilder.snapshotSql(db, table, columnNames(cols), bookmarkId);
        try {
            Connection c = connection.get();
            try (Statement stmt = c.createStatement()) {
                connection.applyStreamingFetchSize(stmt);
                ValueReader reader = ValueReader.forTransport(connection.isArrowFlight());
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        consumer.accept(reader.readRow(rs, cols));
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
                ValueReader reader = ValueReader.forTransport(connection.isArrowFlight());
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    // changesSql appends __CHANGE_TYPE__ (int) and __ROW_VERSION__ (long) after the
                    // table's columns; readRow covers only those, so the two are read here by index.
                    int changeTypeIdx = cols.size() + 1;
                    int rowVersionIdx = cols.size() + 2;
                    while (rs.next()) {
                        Object[] row = reader.readRow(rs, cols);
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

    static List<String> columnNames(List<ColumnMeta> cols) {
        List<String> names = new ArrayList<>(cols.size());
        for (ColumnMeta col : cols) {
            names.add(col.name);
        }
        return names;
    }

}
