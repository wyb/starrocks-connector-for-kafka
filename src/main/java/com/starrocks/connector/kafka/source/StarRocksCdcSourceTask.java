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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * Poll loop and bookmark state machine for the StarRocks CDC source connector's tasks.
 *
 * <p>This class is the correctness core of the connector. Three invariants hold across every
 * code path below and are pinned down by {@code StarRocksCdcSourceTaskTest}:
 * <ol>
 *   <li><b>Release-after-commit.</b> A bookmark is only ever released from {@link #commit()},
 *       which Connect invokes only after the offsets carried by previously returned records have
 *       been durably flushed. {@link #poll()} itself never releases anything, and {@code
 *       commit()} always keeps the two most-recently-created live bookmarks per table so a
 *       release can never race an offset flush that is still in flight.</li>
 *   <li><b>Snapshot-crash semantics.</b> Every snapshot row's offset carries {@code
 *       snapshot_done=false}; every change record's offset carries {@code snapshot_done=true}.
 *       A crash at any point before the first change record's offset is durably committed
 *       therefore resumes with {@code snapshot_done=false} (or no offset at all), and {@link
 *       #restoreOffset} redoes the snapshot from scratch rather than resuming a half-delivered
 *       one -- at-least-once, never a silent gap.</li>
 *   <li><b>Idle dedup.</b> When {@code bookmarkCreate} reports back the table's already-committed
 *       bookmark, the table produced no new version since the last poll, so this poll contributes
 *       no records for it and opens no new CHANGES window.</li>
 * </ol>
 */
public class StarRocksCdcSourceTask extends SourceTask {

    static final class TableState {
        final String table;
        final String topic;
        List<ColumnMeta> cols;
        List<String> pks;
        ChangeRecordMapper mapper;
        long committedBookmark = -1L;
        boolean snapshotDone = false;
        // Set once bootstrap() finishes a snapshot. The snapshot_done flag handed to
        // toChangeRecord() is unconditionally true regardless of this field (change records are
        // only ever produced once snapshotDone is true) -- this is kept for readability of the
        // "first change record after a snapshot" narrative and possible future use, not because
        // poll() branches on it.
        boolean pendingSnapshotDoneFlag = false;
        final Deque<Long> liveBookmarks = new ArrayDeque<>();

        TableState(String table, String topic) {
            this.table = table;
            this.topic = topic;
        }
    }

    private StarRocksCdcSourceConfig config;
    private CdcClient client;
    private String db;
    private String holder;
    private long ttlMs;
    private long pollIntervalMs;
    private boolean snapshotInitial;
    private boolean tombstones;
    private boolean policyResnapshot;

    // Package-visible (not private) so tests can reach a started task's per-table state directly
    // via restoreOffset(), the same way start() itself does for the production offset-recovery
    // path -- see restoreOffset()'s javadoc.
    List<TableState> tables;

    /** Test injection point: test subclasses override to return a scripted fake. */
    protected CdcClient createClient(StarRocksCdcSourceConfig cfg) {
        return new StarRocksJdbcClient(cfg);
    }

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        config = new StarRocksCdcSourceConfig(props);
        db = config.databaseName();
        holder = config.holderId(props.getOrDefault("name", "default"));
        ttlMs = config.bookmarkTtlMs();
        pollIntervalMs = config.pollIntervalMs();
        snapshotInitial = "initial".equals(config.snapshotMode());
        tombstones = config.tombstonesOnDelete();
        policyResnapshot = "resnapshot".equals(config.nonTrackablePolicy());

        client = createClient(config);

        List<String> taskTables = parseTaskTables(props.get(StarRocksCdcSourceConfig.TASK_TABLES));
        // context is set by the framework via initialize(SourceTaskContext) before start(); it is
        // null in unit tests that construct/start a task directly, and every table is then
        // treated as fresh, same as a first-ever run.
        OffsetStorageReader reader = context == null ? null : context.offsetStorageReader();

        try {
            List<TableState> started = new ArrayList<>();
            for (String t : taskTables) {
                TableState ts = new TableState(t, config.topicFor(t));
                ts.cols = client.fetchColumns(db, t);
                ts.pks = client.fetchPrimaryKeys(db, t);
                ts.mapper = new ChangeRecordMapper(db, t, ts.topic, ts.cols, ts.pks);
                Map<String, Object> raw = reader == null ? null : reader.offset(OffsetState.sourcePartition(db, t));
                restoreOffset(ts, raw);
                started.add(ts);
            }
            tables = started;
        } catch (SQLException e) {
            throw new ConnectException("Failed to start CDC source task", e);
        }
    }

    /**
     * Restores one table's in-memory bookmark state from the offset last durably stored for it.
     *
     * <p>Package-visible (rather than private) so a test can drive it directly against a table
     * of a started task without needing a real {@code SourceTaskContext} / offset store -- {@link
     * #start} calls this exact same method for every table (with {@code raw} coming from {@code
     * context.offsetStorageReader()} instead of being supplied directly), so the path a direct
     * call exercises in tests is the same path production restarts go through.
     *
     * <p>Only an offset whose {@code snapshot_done} is {@code true} is trusted to resume
     * streaming; anything else -- including no prior offset -- leaves the table fresh so the next
     * {@link #poll()} redoes the snapshot, per the snapshot-crash invariant.
     */
    void restoreOffset(TableState t, Map<String, Object> raw) {
        OffsetState state = OffsetState.fromMap(raw);
        if (state.snapshotDone) {
            t.committedBookmark = state.bookmarkId;
            t.snapshotDone = true;
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> out = new ArrayList<>();
        for (TableState t : tables) {
            try {
                if (!t.snapshotDone) {
                    bootstrap(t, out);
                    continue;
                }
                long head = client.bookmarkCreate(db, t.table, holder, ttlMs);
                if (head == t.committedBookmark) {
                    continue; // idle dedup: no new version since the last poll
                }
                t.liveBookmarks.addLast(head);
                final long base = t.committedBookmark;
                client.streamChanges(db, t.table, colNames(t), base, head, (row, changeType, rowVersion) -> {
                    SourceRecord r = t.mapper.toChangeRecord(row, changeType, rowVersion, base, head,
                            consumeSnapshotDoneFlag(t));
                    out.add(r);
                    if (tombstones && changeType == 1) {
                        out.add(t.mapper.tombstoneFor(r));
                    }
                });
                t.committedBookmark = head;
            } catch (NonTrackableException e) {
                applyPolicy(t, e);
            } catch (SQLException e) {
                throw new ConnectException("CDC poll failed for table " + t.table, e);
            }
        }
        if (out.isEmpty()) {
            Thread.sleep(pollIntervalMs);
            return null;
        }
        return out;
    }

    private void bootstrap(TableState t, List<SourceRecord> out) throws SQLException {
        long b0 = client.bookmarkCreate(db, t.table, holder, ttlMs);
        t.liveBookmarks.addLast(b0);
        if (snapshotInitial) {
            client.streamSnapshot(db, t.table, colNames(t), b0, row -> out.add(t.mapper.toSnapshotRecord(row, b0)));
        }
        t.committedBookmark = b0;
        t.snapshotDone = true;
        t.pendingSnapshotDoneFlag = true;
    }

    private void applyPolicy(TableState t, NonTrackableException e) {
        if (policyResnapshot) {
            // Leave liveBookmarks untouched: the bookmark just opened for this failed window was
            // still really created server-side, so it is real and commit()'s retention rule (plus
            // TTL as a backstop) still owns cleaning it up.
            t.snapshotDone = false;
            t.committedBookmark = -1L;
            t.pendingSnapshotDoneFlag = false;
        } else {
            throw new ConnectException("CHANGES window not trackable for table " + t.table, e);
        }
    }

    @Override
    public void commit() {
        if (tables == null || client == null) {
            return;
        }
        for (TableState t : tables) {
            while (t.liveBookmarks.size() > 2) {
                Long old = t.liveBookmarks.pollFirst();
                try {
                    client.bookmarkRelease(db, t.table, old, holder);
                } catch (Exception ignore) {
                    // Best-effort: a release that fails here is cleaned up later by TTL expiry.
                }
            }
        }
    }

    @Override
    public void stop() {
        if (client != null) {
            client.close();
        }
    }

    /**
     * Change records are only ever produced once {@code t.snapshotDone} is true, so the flag
     * handed to {@link ChangeRecordMapper#toChangeRecord} is unconditionally {@code true}.
     * Clearing {@code pendingSnapshotDoneFlag} here is purely bookkeeping for readability -- it
     * does not gate the return value -- so every change record after a snapshot, first or not,
     * correctly carries {@code snapshot_done=true}.
     */
    private boolean consumeSnapshotDoneFlag(TableState t) {
        t.pendingSnapshotDoneFlag = false;
        return true;
    }

    private static List<String> colNames(TableState t) {
        List<String> names = new ArrayList<>(t.cols.size());
        for (ColumnMeta c : t.cols) {
            names.add(c.name);
        }
        return names;
    }

    private static List<String> parseTaskTables(String raw) {
        List<String> result = new ArrayList<>();
        if (raw == null) {
            return result;
        }
        for (String part : raw.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }
}
