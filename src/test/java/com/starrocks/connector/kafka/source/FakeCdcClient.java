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
import java.sql.Types;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Scripted-and-recording {@link CdcClient} test double, shared by the connector and task tests.
 *
 * <p>Scripting:
 * <ul>
 *   <li>{@link #bookmarkCreate} consumes the table's queue of {@link #enqueueHead} values, then
 *       repeats the last one it handed out -- that repeat is what makes a table look idle, with no
 *       new version to report. A table that was never scripted gets ids from a plain counter
 *       starting at 1, which is what the connector's preflight probe sees.</li>
 *   <li>{@link #streamChanges} replays the table's {@link #enqueueChanges} rows and clears them,
 *       unless the table was armed by {@link #failNextChangesWithNonTrackable} -- then it throws
 *       once and leaves the queued changes alone.</li>
 *   <li>{@link #fetchColumns} and {@link #fetchPrimaryKeys} fall back to a two-column table with no
 *       primary key when a test did not care to specify one.</li>
 * </ul>
 */
final class FakeCdcClient implements CdcClient {

    static final List<ColumnMeta> DEFAULT_COLS = Arrays.asList(
            new ColumnMeta("k", Types.INTEGER, 10, 0, false),
            new ColumnMeta("v", Types.BIGINT, 19, 0, true));

    // -- scripted inputs --
    final Map<String, String> modelByTable = new HashMap<>();
    final Map<String, Boolean> cdcEnabledByTable = new HashMap<>();
    final Map<String, List<ColumnMeta>> colsByTable = new HashMap<>();
    private final Map<String, List<String>> pksByTable = new HashMap<>();
    private final Map<String, Deque<Long>> queuedHeadsByTable = new HashMap<>();
    private final Map<String, Long> lastHeadByTable = new HashMap<>();
    private final Map<String, List<Object[]>> queuedSnapshotRowsByTable = new HashMap<>();
    private final Map<String, List<ChangeRow>> queuedChangesByTable = new HashMap<>();
    private final Set<String> failNextChangesTables = new HashSet<>();
    /** When set, every bookmarkCreate fails with it -- i.e. the FE gate is closed. */
    SQLException bookmarkCreateFailure;

    // -- recorded calls --
    final List<String> createdBookmarks = new ArrayList<>();
    final List<String> releasedBookmarks = new ArrayList<>();
    /** Holder ids, verbatim, as handed to bookmarkCreate / bookmarkRelease. */
    final List<String> createHolders = new ArrayList<>();
    final List<String> releaseHolders = new ArrayList<>();
    final List<String> streamedWindows = new ArrayList<>();
    int snapshotCalls = 0;

    private long nextBookmarkId = 1L;

    void setColumns(String table, List<ColumnMeta> cols) {
        colsByTable.put(table, cols);
    }

    void setPrimaryKeys(String table, List<String> pks) {
        pksByTable.put(table, pks);
    }

    void enqueueHead(String table, long id) {
        queuedHeadsByTable.computeIfAbsent(table, k -> new ArrayDeque<>()).addLast(id);
    }

    void enqueueSnapshotRows(String table, List<Object[]> rows) {
        queuedSnapshotRowsByTable.put(table, rows);
    }

    void enqueueChanges(String table, ChangeRow... changeRows) {
        queuedChangesByTable.put(table, new ArrayList<>(Arrays.asList(changeRows)));
    }

    void failNextChangesWithNonTrackable(String table) {
        failNextChangesTables.add(table);
    }

    @Override
    public long bookmarkCreate(String db, String table, String holder, long ttlMs) throws SQLException {
        createdBookmarks.add(db + "." + table + ":" + holder);
        createHolders.add(holder);
        if (bookmarkCreateFailure != null) {
            throw bookmarkCreateFailure;
        }
        Deque<Long> queue = queuedHeadsByTable.get(table);
        long value;
        if (queue != null && !queue.isEmpty()) {
            value = queue.pollFirst();
        } else if (lastHeadByTable.containsKey(table)) {
            value = lastHeadByTable.get(table);
        } else {
            value = nextBookmarkId++;
        }
        lastHeadByTable.put(table, value);
        return value;
    }

    @Override
    public void bookmarkRelease(String db, String table, long bookmarkId, String holder) {
        releasedBookmarks.add(db + "." + table + ":" + bookmarkId + ":" + holder);
        releaseHolders.add(holder);
    }

    @Override
    public List<ColumnMeta> fetchColumns(String db, String table) {
        List<ColumnMeta> cols = colsByTable.get(table);
        return cols != null ? cols : DEFAULT_COLS;
    }

    @Override
    public List<String> fetchPrimaryKeys(String db, String table) {
        List<String> pks = pksByTable.get(table);
        return pks != null ? pks : new ArrayList<>();
    }

    @Override
    public String fetchTableModel(String db, String table) {
        return modelByTable.get(table);
    }

    @Override
    public boolean cdcPropertyEnabled(String db, String table) {
        Boolean enabled = cdcEnabledByTable.get(table);
        return enabled != null && enabled;
    }

    @Override
    public void streamSnapshot(String db, String table, List<ColumnMeta> cols, long bookmarkId,
                               RowConsumer consumer) {
        snapshotCalls++;
        List<Object[]> queued = queuedSnapshotRowsByTable.get(table);
        if (queued != null) {
            for (Object[] row : queued) {
                consumer.accept(row);
            }
        }
    }

    @Override
    public void streamChanges(String db, String table, List<ColumnMeta> cols, long base, long head,
                              ChangeRowConsumer consumer) throws NonTrackableException {
        streamedWindows.add(base + "_" + head);
        if (failNextChangesTables.remove(table)) {
            throw new NonTrackableException("synthetic non-trackable window for " + table, null);
        }
        List<ChangeRow> queued = queuedChangesByTable.remove(table);
        if (queued != null) {
            for (ChangeRow row : queued) {
                consumer.accept(row.row, row.changeType, row.rowVersion);
            }
        }
    }

    @Override
    public void close() {
        // no resources held
    }

    static final class ChangeRow {
        final Object[] row;
        final int changeType;
        final long rowVersion;

        ChangeRow(Object[] row, int changeType, long rowVersion) {
            this.row = row;
            this.changeType = changeType;
            this.rowVersion = rowVersion;
        }
    }
}
