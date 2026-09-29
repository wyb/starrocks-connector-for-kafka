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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Scripts responses and records calls for connector and task tests. Once queued heads are
 * exhausted, bookmarkCreate repeats the last id to model an idle table. One-shot read failures
 * leave the affected rows queued for retry; other tables remain available.
 */
final class FakeCdcClient implements CdcClient {

    static final List<ColumnMeta> DEFAULT_COLS = Arrays.asList(
            new ColumnMeta("k", "int", "int(11)", 0, false),
            new ColumnMeta("v", "bigint", "bigint(20)", 0, true));

    // -- scripted inputs --
    final Map<String, String> modelByTable = new HashMap<>();
    final Map<String, Boolean> cdcEnabledByTable = new HashMap<>();
    final Map<String, List<ColumnMeta>> colsByTable = new HashMap<>();
    private final Map<String, Deque<Long>> queuedHeadsByTable = new HashMap<>();
    private final Map<String, Long> lastHeadByTable = new HashMap<>();
    private final Map<String, List<Object[]>> queuedSnapshotRowsByTable = new HashMap<>();
    private final Map<String, List<ChangeRow>> queuedChangesByTable = new HashMap<>();
    private final Map<String, List<Long>> heldBookmarksByTable = new HashMap<>();
    private final Set<String> failNextChangesTables = new HashSet<>();
    private final Set<String> failNextChangesAfterEmittingTables = new HashSet<>();
    /** Set to simulate a disabled bookmark function. */
    SQLException bookmarkCreateFailure;
    SQLException bookmarkRenewFailure;
    /** Releases are recorded before this failure is thrown. */
    SQLException bookmarkReleaseFailure;
    SQLException tableConfigFailure;
    SQLException heldBookmarksFailure;
    /** Thrown once by streamChanges for that table, after any queued rows -- a partial window. */
    final Map<String, SQLException> changesFailureByTable = new HashMap<>();
    /** Thrown once by a snapshot cursor for that table, after any queued rows. */
    final Map<String, SQLException> snapshotFailureByTable = new HashMap<>();
    String nonTrackableMessage;
    /** Renewals of these ids fail; others succeed -- a partial round. */
    final Set<Long> failRenewOfBookmarks = new HashSet<>();
    /** The TTL renewal reports back; null means "whatever was asked for", as an uncapped FE does. */
    Long grantedTtlMs;
    /** Per-bookmark grants, for a round whose ids do not all get the same lease. */
    final Map<Long, Long> grantedTtlByBookmark = new HashMap<>();

    // -- recorded calls --
    final List<String> createdBookmarks = new ArrayList<>();
    final List<String> releasedBookmarks = new ArrayList<>();
    final List<String> renewedBookmarks = new ArrayList<>();
    final List<String> heldBookmarkQueries = new ArrayList<>();
    final List<Long> requestedTtls = new ArrayList<>();
    final List<String> createHolders = new ArrayList<>();
    final List<String> releaseHolders = new ArrayList<>();
    final List<String> streamedWindows = new ArrayList<>();
    int snapshotCalls = 0;
    volatile int snapshotCursorCloses = 0;
    CountDownLatch snapshotCursorClosed;
    CountDownLatch snapshotNextEntered;
    CountDownLatch snapshotNextProceed;
    CountDownLatch snapshotOpenEntered;
    CountDownLatch snapshotOpenProceed;
    String snapshotCloseBlockTable;
    CountDownLatch snapshotCloseEntered;
    CountDownLatch snapshotCloseProceed;
    CountDownLatch clientClosed;

    private long nextBookmarkId = 1L;

    void setColumns(String table, List<ColumnMeta> cols) {
        colsByTable.put(table, cols);
    }

    void setHeldBookmarks(String table, Long... ids) {
        heldBookmarksByTable.put(table, Arrays.asList(ids));
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

    /** Delivers the queued rows, then fails -- what the BE does when it discovers an unreachable
     *  ancestor partway down the version chain. */
    void failNextChangesAfterEmitting(String table) {
        failNextChangesAfterEmittingTables.add(table);
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
    public long bookmarkRenew(String db, String table, long bookmarkId, String holder, long ttlMs)
            throws SQLException {
        renewedBookmarks.add(db + "." + table + ":" + bookmarkId + ":" + holder);
        requestedTtls.add(ttlMs);
        if (bookmarkRenewFailure != null) {
            throw bookmarkRenewFailure;
        }
        if (failRenewOfBookmarks.contains(bookmarkId)) {
            throw new SQLException("synthetic renew failure for bookmark " + bookmarkId);
        }
        Long perBookmark = grantedTtlByBookmark.get(bookmarkId);
        if (perBookmark != null) {
            return perBookmark;
        }
        return grantedTtlMs != null ? grantedTtlMs : ttlMs;
    }

    @Override
    public void bookmarkRelease(String db, String table, long bookmarkId, String holder) throws SQLException {
        releasedBookmarks.add(db + "." + table + ":" + bookmarkId + ":" + holder);
        releaseHolders.add(holder);
        if (bookmarkReleaseFailure != null) {
            throw bookmarkReleaseFailure;
        }
    }

    @Override
    public List<Long> fetchHeldBookmarks(String db, String table, String holder) throws SQLException {
        heldBookmarkQueries.add(db + "." + table + ":" + holder);
        if (heldBookmarksFailure != null) {
            throw heldBookmarksFailure;
        }
        List<Long> held = heldBookmarksByTable.get(table);
        return held != null ? new ArrayList<>(held) : new ArrayList<>();
    }

    @Override
    public List<ColumnMeta> fetchColumns(String db, String table) {
        List<ColumnMeta> cols = colsByTable.get(table);
        return cols != null ? cols : DEFAULT_COLS;
    }

    @Override
    public TableConfig fetchTableConfig(String db, String table) throws SQLException {
        if (tableConfigFailure != null) {
            throw tableConfigFailure;
        }
        Boolean enabled = cdcEnabledByTable.get(table);
        return new TableConfig(db, table, 0L, modelByTable.get(table),
                "{\"" + TableConfig.CDC_PROPERTY + "\":\"" + (enabled != null && enabled) + "\"}");
    }

    @Override
    public SnapshotCursor openSnapshot(String db, String table, List<ColumnMeta> cols, long bookmarkId) {
        snapshotCalls++;
        if (snapshotOpenEntered != null) {
            snapshotOpenEntered.countDown();
            try {
                snapshotOpenProceed.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while opening snapshot", e);
            }
        }
        List<Object[]> queued = queuedSnapshotRowsByTable.get(table);
        List<Object[]> rows = queued != null ? queued : new ArrayList<>();
        return new SnapshotCursor() {
            private int index;
            private volatile boolean closed;

            @Override
            public Object[] next() throws SQLException {
                if (snapshotNextEntered != null) {
                    snapshotNextEntered.countDown();
                    try {
                        snapshotNextProceed.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("Interrupted while reading snapshot", e);
                    }
                }
                if (closed) {
                    throw new SQLException("Snapshot cursor is closed");
                }
                if (index < rows.size()) {
                    return rows.get(index++);
                }
                SQLException scripted = snapshotFailureByTable.remove(table);
                if (scripted != null) {
                    throw scripted;
                }
                return null;
            }

            @Override
            public void close() {
                if (table.equals(snapshotCloseBlockTable)) {
                    snapshotCloseEntered.countDown();
                    try {
                        snapshotCloseProceed.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                if (!closed) {
                    closed = true;
                    snapshotCursorCloses++;
                    if (snapshotCursorClosed != null) {
                        snapshotCursorClosed.countDown();
                    }
                }
            }
        };
    }

    @Override
    public void streamChanges(String db, String table, List<ColumnMeta> cols, long base, long head,
                              ChangeRowConsumer consumer) throws SQLException, NonTrackableException {
        streamedWindows.add(base + "_" + head);
        if (failNextChangesTables.remove(table)) {
            // Built through classify, from a message FE really emits, so the exception carries the
            // same cause flag production would -- constructing it directly bypassed that and let a
            // test assert on a note the connector would never have produced.
            throw NonTrackableException.classify(new SQLException(nonTrackableMessage != null
                    ? nonTrackableMessage
                    : "bookmark " + base + " not found on table '" + table + "'"));
        }
        boolean failAfterEmitting = failNextChangesAfterEmittingTables.remove(table);
        List<ChangeRow> queued = queuedChangesByTable.remove(table);
        if (queued != null) {
            for (ChangeRow row : queued) {
                consumer.accept(row.row, row.changeType, row.rowVersion);
            }
        }
        if (failAfterEmitting) {
            throw new NonTrackableException("synthetic mid-window non-trackable failure for " + table, null);
        }
        SQLException scripted = changesFailureByTable.remove(table);
        if (scripted != null) {
            throw scripted;
        }
    }

    @Override
    public void close() {
        if (clientClosed != null) {
            clientClosed.countDown();
        }
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
