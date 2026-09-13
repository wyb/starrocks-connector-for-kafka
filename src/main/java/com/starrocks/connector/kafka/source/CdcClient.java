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
 * StarRocks-facing operations needed by the CDC source task and connector: bookmark lifecycle,
 * table metadata discovery, and streaming reads of a snapshot or a CHANGES range.
 *
 * <p>Implementations own their JDBC connection lifecycle; callers must {@link #close()} the
 * client when done with it.
 */
public interface CdcClient extends AutoCloseable {

    /**
     * Pins the table's current version, or returns the id this holder already pinned -- create is
     * idempotent per holder, and on an unchanged table it hands the same id back <em>without</em>
     * refreshing its lease. Every caller depends on that: it is what makes an unchanged table look
     * idle, why {@link #bookmarkRenew} has to exist, and why a probe must not share a holder.
     */
    long bookmarkCreate(String db, String table, String holder, long ttlMs) throws SQLException;

    /**
     * Refreshes the lease on a bookmark this holder already has -- the only call that moves it.
     *
     * @return the granted TTL in ms ({@code -1} for no expiry), which a cluster-side ceiling may
     *         have capped below what was asked for. Pace the next renewal against this, not the
     *         request.
     */
    long bookmarkRenew(String db, String table, long bookmarkId, String holder, long ttlMs) throws SQLException;

    void bookmarkRelease(String db, String table, long bookmarkId, String holder) throws SQLException;

    List<ColumnMeta> fetchColumns(String db, String table) throws SQLException;

    List<String> fetchKeyColumns(String db, String table) throws SQLException;

    String fetchTableModel(String db, String table) throws SQLException;

    boolean cdcPropertyEnabled(String db, String table) throws SQLException;

    /** @param cols from {@link #fetchColumns}; it both names the columns and types the values read
     *              back, so the read side never re-derives types from the driver. */
    void streamSnapshot(String db, String table, List<ColumnMeta> cols, long bookmarkId, RowConsumer consumer)
            throws SQLException, NonTrackableException;

    /**
     * Streams the changes in the half-open window {@code (base, head]} -- base exclusive, head
     * inclusive, which is why the previous head becomes the next base and why the window opened
     * after a snapshot at {@code b0} does not re-deliver {@code b0}.
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

    interface ChangeRowConsumer {
        void accept(Object[] row, int changeType, long rowVersion);
    }
}
