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

import java.sql.SQLException;
import java.util.List;

/**
 * Defines bookmark, metadata, snapshot, and CHANGES operations used by the connector and task.
 * Implementations own their JDBC connections; callers close the client when finished.
 */
public interface CdcClient extends AutoCloseable {

    /**
     * Pins the current version. An unchanged table returns this holder's existing id without
     * extending its lease, so callers must renew it separately.
     */
    long bookmarkCreate(String db, String table, String holder, long ttlMs) throws SQLException;

    /**
     * Refreshes the lease on an existing bookmark.
     *
     * @return the granted TTL in ms ({@code -1} for no expiry), which a cluster-side ceiling may
     *         have capped below the requested value. Pace renewal against this grant.
     */
    long bookmarkRenew(String db, String table, long bookmarkId, String holder, long ttlMs) throws SQLException;

    void bookmarkRelease(String db, String table, long bookmarkId, String holder) throws SQLException;

    /** Ids of the bookmarks {@code holder} still references on the table, ascending; none for a table that
     *  no longer exists. */
    List<Long> fetchHeldBookmarks(String db, String table, String holder) throws SQLException;

    /** Columns in declaration order, including key flags. */
    List<ColumnMeta> fetchColumns(String db, String table) throws SQLException;

    /** The table's {@code tables_config} row. */
    TableConfig fetchTableConfig(String db, String table) throws SQLException;

    /** Opens a pinned snapshot cursor that can be consumed across several poll calls. The cursor
     *  owns its read connection so bookmark maintenance can still run between batches. */
    SnapshotCursor openSnapshot(String db, String table, List<ColumnMeta> cols, long bookmarkId)
            throws SQLException, NonTrackableException;

    /** @param cols from {@link #fetchColumns}; it both names the columns and types the values read
     *              back, so the read side never re-derives types from the driver. */
    default void streamSnapshot(String db, String table, List<ColumnMeta> cols, long bookmarkId, RowConsumer consumer)
            throws SQLException, NonTrackableException {
        try (SnapshotCursor cursor = openSnapshot(db, table, cols, bookmarkId)) {
            Object[] row;
            while ((row = cursor.next()) != null) {
                consumer.accept(row);
            }
        }
    }

    /**
     * Streams the half-open window {@code (base, head]}; the previous head is the next base.
     *
     * @param cols as for {@link #streamSnapshot}.
     */
    void streamChanges(String db, String table, List<ColumnMeta> cols, long base, long head, ChangeRowConsumer consumer)
            throws SQLException, NonTrackableException;

    @Override
    void close();

    interface RowConsumer {
        void accept(Object[] row);
    }

    interface SnapshotCursor extends AutoCloseable {
        /** Returns null only after the complete snapshot has been read. */
        Object[] next() throws SQLException, NonTrackableException;

        @Override
        void close();
    }

    interface ChangeRowConsumer {
        void accept(Object[] row, int changeType, long rowVersion);
    }
}
