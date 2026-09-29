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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link CdcClient} over JDBC. The query layer only: {@link FeConnection},
 * {@link ColumnMetaReader} and {@link ValueReader} sit underneath it.
 *
 * <p><b>Not thread-safe</b>, because {@link FeConnection} is not; drive one client from one thread.
 * Connection behaviour is covered by the integration smoke test, not by unit tests.
 */
public class StarRocksJdbcClient implements CdcClient {

    private final StarRocksCdcSourceConfig config;
    private final FeConnection connection;
    private final ColumnMetaReader columnMetaReader;

    public StarRocksJdbcClient(StarRocksCdcSourceConfig config) {
        this.config = config;
        this.connection = new FeConnection(config);
        this.columnMetaReader = new ColumnMetaReader(connection);
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
        return columnMetaReader.fetchColumns(db, table);
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
    public SnapshotCursor openSnapshot(String db, String table, List<ColumnMeta> cols, long bookmarkId)
            throws SQLException, NonTrackableException {
        String sql = SqlBuilder.snapshotSql(db, table, columnNames(cols), bookmarkId);
        // A streaming ResultSet can occupy its JDBC connection until EOF. Bookmark renewals and
        // releases still need the task's regular connection between snapshot batches.
        FeConnection snapshotConnection = new FeConnection(config);
        try {
            Connection c = snapshotConnection.get();
            Statement stmt = c.createStatement();
            try {
                snapshotConnection.applyStreamingFetchSize(stmt);
                ResultSet rs = stmt.executeQuery(sql);
                return new JdbcSnapshotCursor(snapshotConnection, stmt, rs,
                        ValueReader.forTransport(snapshotConnection.isArrowFlight()), cols);
            } catch (SQLException e) {
                try {
                    stmt.close();
                } catch (SQLException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
                throw e;
            }
        } catch (SQLException e) {
            snapshotConnection.close();
            NonTrackableException nte = NonTrackableException.classify(e);
            if (nte != null) {
                throw nte;
            }
            throw e;
        }
    }

    private static final class JdbcSnapshotCursor implements SnapshotCursor {
        private final FeConnection connection;
        private final Statement stmt;
        private final ResultSet rs;
        private final ValueReader reader;
        private final List<ColumnMeta> cols;
        private final AtomicBoolean closed = new AtomicBoolean();

        private JdbcSnapshotCursor(FeConnection connection, Statement stmt, ResultSet rs,
                                   ValueReader reader, List<ColumnMeta> cols) {
            this.connection = connection;
            this.stmt = stmt;
            this.rs = rs;
            this.reader = reader;
            this.cols = cols;
        }

        @Override
        public Object[] next() throws SQLException, NonTrackableException {
            if (closed.get()) {
                throw new SQLException("Snapshot cursor is closed");
            }
            try {
                if (!rs.next()) {
                    close();
                    return null;
                }
                return reader.readRow(rs, cols);
            } catch (SQLException e) {
                close();
                NonTrackableException nte = NonTrackableException.classify(e);
                if (nte != null) {
                    throw nte;
                }
                throw e;
            }
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                // The connection owns this cursor. Closing it first gives an in-flight next() a
                // chance to unblock when stop() closes the cursor from another thread.
                connection.close();
                try {
                    rs.close();
                } catch (SQLException ignored) {
                    // Closing the connection above also disposes of the cursor.
                }
                try {
                    stmt.close();
                } catch (SQLException ignored) {
                    // Closing the connection above also disposes of the statement.
                }
            }
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
