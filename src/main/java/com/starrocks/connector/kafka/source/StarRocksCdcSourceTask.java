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

import com.starrocks.connector.kafka.common.Version;

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
 * Poll loop and bookmark state machine -- the correctness core. Three invariants, all pinned by
 * {@code StarRocksCdcSourceTaskTest}:
 * <ol>
 *   <li><b>Releases lag acks by one commit cycle.</b> {@code commitRecord} fires from the producer
 *       send callback, so the acked bookmark can lead the durable offset; releasing everything
 *       below it would unpin the bookmark the last flush just made durable. Each {@code commit()}
 *       therefore releases only below the fence observed at the <i>previous</i> one. See
 *       {@link #commit()}.</li>
 *   <li><b>Snapshot rows carry {@code snapshot_done=false}, change records {@code true}.</b> A
 *       crash before the first change record's offset is durable redoes the whole snapshot rather
 *       than resuming a half-delivered one -- duplicates, never a silent gap.</li>
 *   <li><b>Idle dedup.</b> {@code bookmarkCreate} returning the committed bookmark means no new
 *       version, so the poll opens no window.</li>
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
        // Newest bookmark Connect has acked for the table. An ACK watermark, not a durability one:
        // it can lead the last flushed offset by one bookmark. See releaseFenceBookmark.
        long lastAckedBookmark = -1L;
        // lastAckedBookmark as of the PREVIOUS commit(). Releasing below this rather than below the
        // current value buys one intervening flush cycle, which is what makes the fence provably no
        // newer than durability.
        long releaseFenceBookmark = -1L;
        // Created and not yet released, oldest first. Genuinely concurrent: the poll thread appends,
        // SourceTaskOffsetCommitter's thread drains in commit(). Every access holds its monitor,
        // which also guards the two watermarks above.
        final Deque<Long> liveBookmarks = new ArrayDeque<>();
        // Authorized by commit(), released by poll(). commit() must issue no SQL: the JDBC client is
        // single-threaded and the poll thread may be mid-scan on it.
        final Queue<Long> pendingReleases = new ConcurrentLinkedQueue<>();
        // -1 = never renewed, so the first round runs whatever the clock reads. A sentinel rather
        // than 0, which would depend on the clock's origin being far from zero.
        long lastRenewMs = -1L;
        // The lease the server granted: -1 until one is known, 0 once it answers "no expiry".
        long effectiveTtlMs = -1L;

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

    // Package-visible so tests can reach a started task's per-table state directly.
    List<TableState> tables;

    /** Test injection point: test subclasses override to return a scripted fake. */
    protected CdcClient createClient(StarRocksCdcSourceConfig cfg) {
        return new StarRocksJdbcClient(cfg);
    }

    @Override
    public String version() {
        return Version.get();
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
     * Restores one table from its durable offset. Only {@code snapshot_done=true} is trusted;
     * anything else leaves the table fresh so the next poll redoes the snapshot.
     *
     * <p>Package-visible so tests can drive it without a real offset store; {@link #start} calls
     * the same method.
     */
    void restoreOffset(TableState t, Map<String, Object> raw) {
        OffsetState state = OffsetState.fromMap(raw);
        if (state.snapshotDone) {
            t.committedBookmark = state.bookmarkId;
            t.snapshotDone = true;
            // Not optional: without this the resumed bookmark is never released, leaking one per
            // table per restart until its TTL. Safe, because commit() only releases strictly below
            // the previous cycle's ack watermark, so it survives until a newer window is acked.
            retain(t, state.bookmarkId);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> out = new ArrayList<>();
        for (TableState t : tables) {
            drainPendingReleases(t);
            renewLiveBookmarks(t);
            final int emittedBefore = out.size();
            try {
                if (!t.snapshotDone) {
                    bootstrap(t, out);
                    continue;
                }
                long head = client.bookmarkCreate(db, t.table, holder, ttlMs);
                if (head == t.committedBookmark) {
                    continue; // idle dedup: no new version since the last poll
                }
                retain(t, head);
                final long base = t.committedBookmark;
                client.streamChanges(db, t.table, t.cols, base, head, (row, changeType, rowVersion) -> {
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
                // These records carry this window's head, so committing them would advance the
                // durable position past a window never read to the end. Empty today -- changesSql's
                // ORDER BY makes the scan blocking -- which is why the rule belongs here and not in
                // one clause of SqlBuilder.
                discardFrom(out, emittedBefore);
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
        retain(t, b0);
        if (snapshotInitial) {
            LOG.info("Starting snapshot of {}.{} at bookmark {}", db, t.table, b0);
            int rowsBefore = out.size();
            client.streamSnapshot(db, t.table, t.cols, b0, row -> out.add(t.mapper.toSnapshotRecord(row, b0)));
            LOG.info("Finished snapshot of {}.{} at bookmark {}: {} row(s)",
                    db, t.table, b0, out.size() - rowsBefore);
        } else {
            LOG.info("Snapshot skipped for {}.{}; streaming changes from bookmark {}", db, t.table, b0);
        }
        t.committedBookmark = b0;
        t.snapshotDone = true;
    }

    /**
     * Retains a bookmark, ignoring one already held. {@code bookmark_create} returns the holder's
     * existing bookmark on an unchanged table, so after a resnapshot {@link #bootstrap} reopens the
     * failed window's id. Held twice, it is released twice, and the second failure is logged as a
     * bookmark left pinned -- the opposite of what happened.
     */
    private static void retain(TableState t, long bookmarkId) {
        synchronized (t.liveBookmarks) {
            if (!t.liveBookmarks.contains(bookmarkId)) {
                t.liveBookmarks.addLast(bookmarkId);
            }
        }
    }

    /** Truncates {@code out} back to the size it had before the current table's window. */
    private static void discardFrom(List<SourceRecord> out, int from) {
        if (out.size() > from) {
            out.subList(from, out.size()).clear();
        }
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

    /** Overridden in tests to drive the renewal schedule without waiting out a real TTL. */
    long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Refreshes every bookmark the table holds -- not only {@code committedBookmark}, since the
     * others stay pinned and ageing while acks lag. Without this an idle table loses its position:
     * {@code bookmarkCreate} returns the same id on an unchanged table without moving the lease.
     *
     * <p>Paced off the lease the server granted, never {@code source.bookmark.ttlms}: a
     * cluster-side ceiling can cap it and is readable nowhere else. A third of it leaves room for
     * two failed rounds, which is why a failure here only waits for the next.
     *
     * <p>Never triggers {@code source.nontrackable.policy}. Reading a lost position out of a failed
     * renewal would mean matching FE's exception text and would give recovery a second entry point;
     * a bookmark that is really gone still surfaces at the next CHANGES read.
     */
    private void renewLiveBookmarks(TableState t) {
        if (ttlMs <= 0 || t.effectiveTtlMs == 0) {
            return;
        }
        long lease = t.effectiveTtlMs > 0 ? t.effectiveTtlMs : ttlMs;
        long now = currentTimeMillis();
        if (t.lastRenewMs >= 0 && now - t.lastRenewMs < lease / 3) {
            return;
        }
        List<Long> held;
        synchronized (t.liveBookmarks) {
            held = new ArrayList<>(t.liveBookmarks); // copy: commit() wants this monitor back
        }
        if (held.isEmpty()) {
            // The first poll runs before bootstrap opens a bookmark; starting the clock on an empty
            // set would push the first real round out by a whole interval.
            return;
        }
        boolean anyRenewed = false;
        for (Long id : held) {
            try {
                long granted = client.bookmarkRenew(db, t.table, id, holder, ttlMs);
                t.effectiveTtlMs = granted < 0 ? 0 : granted;
                anyRenewed = true;
            } catch (Exception e) {
                LOG.warn("Failed to renew bookmark {} for {}.{} (holder {}); the next poll retries",
                        id, db, t.table, holder, e);
            }
        }
        // Only a round that renewed something starts the clock. A wholly failed round must retry on
        // the next poll: until one succeeds the pacing above is off the configured TTL, which a
        // cluster ceiling may have capped far below -- waiting a third of it would outlast the lease.
        if (anyRenewed) {
            t.lastRenewMs = now;
        }
    }

    /**
     * Issues the releases {@link #commit()} authorized, on the poll thread because all JDBC lives
     * there. Best-effort: a failure leaks one pinned version until its TTL, hence the WARN.
     */
    private void drainPendingReleases(TableState t) {
        Long id;
        while ((id = t.pendingReleases.poll()) != null) {
            try {
                client.bookmarkRelease(db, t.table, id, holder);
                // INFO, not DEBUG: the only external sign reclamation is keeping up. Without it
                // "no failures" and "nothing attempted" look identical.
                LOG.info("Released bookmark {} for {}.{}", id, db, t.table);
            } catch (Exception e) {
                LOG.warn("Failed to release bookmark {} for {}.{} (holder {}); it stays pinned until its TTL "
                        + "expires", id, db, t.table, holder, e);
            }
        }
    }

    /**
     * Tracks the newest acked bookmark per table, which is what gives {@link #commit()} something
     * real to fence against: Connect's {@code commit()} says only "a flush completed", not which
     * offset, and an empty CHANGES window advances {@code committedBookmark} without producing any
     * record -- so "keep the newest N" would eventually release the base the durable offset names.
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
     * Authorizes release of everything strictly below the fence, then advances the fence.
     *
     * <p>The fence is the ack watermark from the <i>previous</i> commit, never the current one.
     * That lag is the whole mechanism: {@link #commitRecord} runs in the producer send callback,
     * ahead of the flush that persists the offset, so releasing below the current watermark would
     * unpin the bookmark that flush just made durable -- and a crash before the next flush would
     * then resume from a released base. Nothing acked, or the first commit, releases nothing.
     *
     * <p>Enqueues only; the SQL happens in {@link #drainPendingReleases} on the poll thread.
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
