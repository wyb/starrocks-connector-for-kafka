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

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Poll loop and bookmark state machine for the StarRocks CDC source connector's tasks.
 *
 * <p>This class is the correctness core of the connector. Three invariants hold across every
 * code path below and are pinned down by {@code StarRocksCdcSourceTaskTest}:
 * <ol>
 *   <li><b>Release-after-commit, fenced one commit cycle behind the acked offset.</b> Nothing is
 *       released until {@link #commit()} -- which Connect invokes only after an offset flush
 *       completed -- has authorized it, and {@code commit()} deliberately lags the acks by one
 *       full commit cycle. {@code lastAckedBookmark} (the newest bookmark id Connect confirmed for
 *       the table via {@link #commitRecord}) is <i>not</i> the version the durable offset names:
 *       {@code commitRecord} fires from the producer send callback, while the flush preceding
 *       {@code commit()} only persists the offsets {@code updateCommittableOffsets()} snapshotted
 *       earlier on the task thread -- so the acked bookmark can lead the durable one. Releasing
 *       everything below {@code lastAckedBookmark} would therefore unpin the bookmark that flush
 *       just made durable. Instead each {@code commit()} releases only bookmarks strictly older
 *       than {@code releaseFenceBookmark}, the {@code lastAckedBookmark} value observed at the
 *       <i>previous</i> {@code commit()}, and then advances the fence; before the second
 *       {@code commit()} has ever run, and while nothing has been acked, nothing is released. One
 *       intervening flush cycle is what makes the fence provably no newer than durability.
 *       {@code commit()} performs no SQL: it only hands the authorized ids to {@code
 *       pendingReleases}, and {@link #poll()} issues the actual {@code bookmarkRelease} calls on
 *       the task thread, so all JDBC stays on one thread (see {@code StarRocksJdbcClient}).</li>
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

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksCdcSourceTask.class);

    static final class TableState {
        final String table;
        final String topic;
        List<ColumnMeta> cols;
        List<String> pks;
        ChangeRecordMapper mapper;
        long committedBookmark = -1L;
        boolean snapshotDone = false;
        // The newest bookmark id Kafka Connect has confirmed a record for, per commitRecord();
        // -1 while nothing has been acked yet. Only written from commitRecord() and only read from
        // commit() -- both of which run on Connect's producer-callback / offset-committer threads
        // rather than the poll thread -- so it is guarded by synchronized (liveBookmarks) too.
        // NOTE: this is an ack watermark, not a durability watermark; it can lead the offset the
        // last flush persisted by one bookmark. See releaseFenceBookmark.
        long lastAckedBookmark = -1L;
        // The lastAckedBookmark value as observed at the PREVIOUS commit() call; -1 until commit()
        // has run at least once. commit() releases strictly below this, never below the current
        // lastAckedBookmark, which buys exactly one intervening offset-flush cycle: any record
        // acked before the previous commit() had its offset snapshotted by
        // updateCommittableOffsets() no later than the flush that precedes this commit(), so
        // everything below the fence is provably superseded by a durable offset. Guarded by
        // synchronized (liveBookmarks) along with lastAckedBookmark.
        long releaseFenceBookmark = -1L;
        // Every bookmark this task has created for the table and not yet released, oldest first.
        // Shared between two threads per the Kafka Connect runtime contract: the task's own poll
        // thread appends via addLast (poll()/bootstrap()), while SourceTaskOffsetCommitter's
        // single scheduled-executor thread drains the release-eligible prefix (commit(), scheduled
        // every offset.flush.interval.ms, default 60s) -- genuinely concurrent, not just
        // interleaved. Every access must hold synchronized (liveBookmarks); see poll(),
        // bootstrap(), commitRecord(), and commit().
        final Deque<Long> liveBookmarks = new ArrayDeque<>();
        // Bookmarks commit() has authorized for release, handed over for the poll thread to
        // actually release. commit() must not call into the JDBC client itself: the client is
        // single-threaded and the poll thread may be mid-scan on it (see StarRocksJdbcClient's
        // class javadoc), so the SQL round trip is deferred to the next poll() instead. Thread-safe
        // on its own, so unlike liveBookmarks it needs no external monitor.
        final Queue<Long> pendingReleases = new ConcurrentLinkedQueue<>();

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
            LOG.info("CDC source task started: tables={}, holder={}, snapshot.mode={}, nontrackable.policy={}, "
                            + "poll.intervalms={}, bookmark.ttlms={}",
                    taskTables, holder, config.snapshotMode(), config.nonTrackablePolicy(), pollIntervalMs, ttlMs);
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
            drainPendingReleases(t);
            try {
                if (!t.snapshotDone) {
                    bootstrap(t, out);
                    continue;
                }
                long head = client.bookmarkCreate(db, t.table, holder, ttlMs);
                if (head == t.committedBookmark) {
                    continue; // idle dedup: no new version since the last poll
                }
                synchronized (t.liveBookmarks) {
                    t.liveBookmarks.addLast(head);
                }
                final long base = t.committedBookmark;
                final int emittedBefore = out.size();
                client.streamChanges(db, t.table, colNames(t), base, head, (row, changeType, rowVersion) -> {
                    SourceRecord r = t.mapper.toChangeRecord(row, changeType, rowVersion, base, head, true);
                    out.add(r);
                    if (tombstones && changeType == 1) {
                        out.add(t.mapper.tombstoneFor(r));
                    }
                });
                t.committedBookmark = head;
                LOG.info("Emitted CDC window for {}.{}: bookmark {} -> {}, {} record(s)",
                        db, t.table, base, head, out.size() - emittedBefore);
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
        synchronized (t.liveBookmarks) {
            t.liveBookmarks.addLast(b0);
        }
        if (snapshotInitial) {
            LOG.info("Starting snapshot of {}.{} at bookmark {}", db, t.table, b0);
            int rowsBefore = out.size();
            client.streamSnapshot(db, t.table, colNames(t), b0, row -> out.add(t.mapper.toSnapshotRecord(row, b0)));
            LOG.info("Finished snapshot of {}.{} at bookmark {}: {} row(s)",
                    db, t.table, b0, out.size() - rowsBefore);
        } else {
            LOG.info("Snapshot skipped for {}.{}; streaming changes from bookmark {}", db, t.table, b0);
        }
        t.committedBookmark = b0;
        t.snapshotDone = true;
    }

    private void applyPolicy(TableState t, NonTrackableException e) {
        if (policyResnapshot) {
            // Leave liveBookmarks untouched: the bookmark just opened for this failed window was
            // still really created server-side, so it is real and commit()'s release rule (plus
            // TTL as a backstop) still owns cleaning it up.
            LOG.warn("CHANGES window not trackable for {}.{} from base bookmark {}; {}=resnapshot, so this table's "
                            + "position is discarded and the whole table is re-read on the next poll. Cause: {}",
                    db, t.table, t.committedBookmark, StarRocksCdcSourceConfig.NONTRACKABLE_POLICY, e.getMessage());
            t.snapshotDone = false;
            t.committedBookmark = -1L;
        } else {
            LOG.error("CHANGES window not trackable for {}.{} from base bookmark {}; {}=fail, so the task stops. "
                            + "Cause: {}",
                    db, t.table, t.committedBookmark, StarRocksCdcSourceConfig.NONTRACKABLE_POLICY, e.getMessage());
            throw new ConnectException("CHANGES window not trackable for table " + t.table, e);
        }
    }

    /**
     * Issues the {@code bookmarkRelease} calls {@link #commit()} authorized, on the poll thread.
     *
     * <p>{@code commit()} runs on Connect's offset-committer thread and must not touch the JDBC
     * client (single-threaded; the poll thread may be mid-scan on it), so it only enqueues ids and
     * this drains them at the top of each table's poll iteration. Best-effort by design: a failed
     * release leaks one pinned version until its TTL expires, which is survivable but invisible
     * otherwise, hence the WARN naming the table and the id.
     */
    private void drainPendingReleases(TableState t) {
        Long id;
        while ((id = t.pendingReleases.poll()) != null) {
            try {
                client.bookmarkRelease(db, t.table, id, holder);
            } catch (Exception e) {
                LOG.warn("Failed to release bookmark {} for {}.{} (holder {}); it stays pinned until its TTL "
                        + "expires", id, db, t.table, holder, e);
            }
        }
    }

    /**
     * Records the newest bookmark id Kafka Connect has actually confirmed a record for, per table.
     *
     * <p>This is what makes {@link #commit()} safe. Connect's {@code commit()} says only "some
     * offset flush completed", not which offset, and a bookmark can be created without ever
     * producing a record (an empty CHANGES window still advances {@code committedBookmark}), so
     * "keep the newest N" would eventually release the very bookmark the durable offset names --
     * after which a restart resumes from a released base and the read fails as non-trackable.
     * Tracking the acked id instead gives {@code commit()} something real to fence against.
     *
     * <p>It is only an <i>ack</i> watermark though, not a durability one: this callback runs from
     * the producer's send callback, ahead of the offset flush that will persist that record's
     * offset. {@code commit()} closes that gap by acting on the previous cycle's value rather than
     * this one -- see {@code TableState#releaseFenceBookmark}.
     */
    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
        super.commitRecord(record, metadata);
        if (tables == null || record == null) {
            return;
        }
        Map<String, ?> offset = record.sourceOffset();
        if (offset == null) {
            return;
        }
        Object rawBookmark = offset.get(OffsetState.KEY_BOOKMARK_ID);
        if (!(rawBookmark instanceof Number)) {
            return;
        }
        TableState t = tableOf(record);
        if (t == null) {
            return;
        }
        // Records for one table can be acked out of order, so take the max rather than the latest.
        long acked = ((Number) rawBookmark).longValue();
        synchronized (t.liveBookmarks) {
            if (acked > t.lastAckedBookmark) {
                t.lastAckedBookmark = acked;
            }
        }
    }

    /**
     * Authorizes the release of every bookmark strictly older than the table's release fence, then
     * advances that fence to the currently acked bookmark.
     *
     * <p>The fence is the {@code lastAckedBookmark} value observed at the <i>previous</i>
     * {@code commit()}, never the current one, so releases lag acknowledgements by one whole commit
     * cycle. That lag is the point, not an accident: Connect calls {@link #commitRecord} from the
     * producer's send callback, whereas the offset flush that precedes a {@code commit()} only
     * persists the offsets {@code updateCommittableOffsets()} snapshotted earlier on the task
     * thread. The acked bookmark can therefore be one ahead of the durable one, and releasing
     * everything below it would unpin exactly the bookmark that flush just made durable -- after
     * which a worker death before the next flush resumes from an unpinned base and the read fails
     * as non-trackable. Waiting one cycle guarantees the fence is never ahead of durability.
     *
     * <p>So: nothing acked yet, or {@code commit()} running for the first time, releases nothing at
     * all. The authorized ids are only enqueued here -- the {@code bookmarkRelease} round trips
     * happen on the poll thread, in {@link #drainPendingReleases}, because this method runs on
     * Connect's offset-committer thread and the JDBC client is single-threaded.
     */
    @Override
    public void commit() {
        if (tables == null) {
            return;
        }
        for (TableState t : tables) {
            synchronized (t.liveBookmarks) {
                long fence = t.releaseFenceBookmark;
                // Advance the fence for the *next* commit() before using it, so the value this
                // commit() acts on is always one cycle old even when it releases nothing.
                t.releaseFenceBookmark = t.lastAckedBookmark;
                if (fence < 0) {
                    continue;
                }
                Iterator<Long> it = t.liveBookmarks.iterator();
                while (it.hasNext()) {
                    Long live = it.next();
                    if (live < fence) {
                        it.remove();
                        t.pendingReleases.add(live);
                    }
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

    /** Resolves the {@link TableState} a record belongs to via its source partition, or null. */
    private TableState tableOf(SourceRecord record) {
        Map<String, ?> partition = record.sourcePartition();
        if (partition == null) {
            return null;
        }
        Object table = partition.get(OffsetState.KEY_TABLE);
        for (TableState t : tables) {
            if (t.table.equals(table)) {
                return t;
            }
        }
        return null;
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
