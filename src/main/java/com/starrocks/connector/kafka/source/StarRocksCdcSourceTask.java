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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Poll loop and bookmark state machine -- the correctness core. Six invariants, all pinned by
 * {@code StarRocksCdcSourceTaskTest}:
 * <ol>
 *   <li>Releases are fenced by the durable offset, read back from the offset store, never by acks.
 *       See {@link #commit()} and {@link #durableBookmarks}.</li>
 *   <li>Snapshot rows carry {@code snapshot_done=false}, change records {@code true}, so a crash
 *       mid-snapshot redoes it whole rather than resuming a half-delivered one.</li>
 *   <li>Idle dedup: {@code bookmarkCreate} returning the committed bookmark opens no window; an
 *       empty window's head becomes the committed bookmark.</li>
 *   <li>Only a window's last record carries its head. See {@link #demoteAllButLast}.</li>
 *   <li>One table's failure never discards another's records. See {@link #poll()}.</li>
 *   <li>A tombstone follows a real deletion only, never the delete half of an update.
 *       See {@link #appendWindow}.</li>
 * </ol>
 */
public class StarRocksCdcSourceTask extends SourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksCdcSourceTask.class);

    /**
     * Caps the renewal interval whatever the lease says, because the cluster ceiling is mutable: a
     * lease learned once goes stale the moment an operator lowers it. Not a safe floor for the
     * ceiling itself -- renewal runs only at the top of a poll, so a usable ceiling must clear this
     * by a poll plus a round trip.
     */
    private static final long RENEW_MAX_INTERVAL_MS = 300_000L;

    static final class TableState {
        final String table;
        final List<ColumnMeta> cols;
        final ChangeRecordMapper mapper;
        // volatile: commit() reads it from the offset-committer thread to clamp the fence, while
        // the poll thread writes it. Apart from liveBookmarks and pendingReleases below, every
        // other TableState field stays poll-thread-only.
        volatile long committedBookmark = -1L;
        boolean snapshotDone = false;
        // Created and not yet released, oldest first. Genuinely concurrent: the poll thread appends,
        // SourceTaskOffsetCommitter's thread drains in commit(). Every access holds its monitor.
        final Deque<Long> liveBookmarks = new ArrayDeque<>();
        // Authorized by commit(), released by poll(). commit() must issue no SQL: the JDBC client is
        // single-threaded and the poll thread may be mid-scan on it.
        final Queue<Long> pendingReleases = new ConcurrentLinkedQueue<>();
        // -1 = no successful round yet, so the first runs whatever the clock reads. A sentinel
        // rather than 0, which would depend on the clock's origin being far from zero.
        long lastRenewMs = -1L;
        // The lease the server granted, or -1 when unknown or unbounded; a later round may return it to -1.
        long effectiveTtlMs = -1L;
        // Failure backoff, so a cluster without bookmark_renew is not retried on every poll:
        // executeOnLeader's retries would put ~1s of sleep per bookmark per poll on this thread.
        long lastRenewAttemptMs = -1L;
        long renewBackoffMs = 0L;
        // One warning per table, not one per round, when the poll interval cannot service the lease.
        boolean warnedPollOutpacesLease = false;
        // Monotonic ms of the first failed poll in the current run of failures; -1 after a success.
        long firstFailureMs = -1L;

        TableState(String table, List<ColumnMeta> cols, ChangeRecordMapper mapper) {
            this.table = table;
            this.cols = cols;
            this.mapper = mapper;
        }
    }

    /** Volatile: the API lets stop() run on another thread than the start() that set it. */
    private volatile CdcClient client;
    private String db;
    private String holder;
    private long ttlMs;
    private long pollIntervalMs;
    private long pollRetryTimeoutMs;
    private boolean snapshotInitial;
    private boolean tombstones;
    private boolean policyResnapshot;

    // Volatile: written once at the end of start() on the poll thread, read by commit() on the
    // offset committer's thread. Package-visible so tests can reach a started task's state.
    volatile List<TableState> tables;
    /** Origin of {@link #monotonicMs}, taken at its first reading. */
    private long monotonicOriginNanos = Long.MIN_VALUE;

    @Override
    public String version() {
        return Version.get();
    }

    @Override
    public void start(Map<String, String> props) {
        StarRocksCdcSourceConfig config = new StarRocksCdcSourceConfig(props);
        db = config.databaseName();
        holder = config.holderId();
        ttlMs = config.bookmarkTtlMs();
        pollIntervalMs = config.pollIntervalMs();
        pollRetryTimeoutMs = config.pollRetryTimeoutMs();
        snapshotInitial = StarRocksCdcSourceConfig.SNAPSHOT_MODE_INITIAL.equals(config.snapshotMode());
        tombstones = config.tombstonesOnDelete();
        policyResnapshot = StarRocksCdcSourceConfig.NONTRACKABLE_POLICY_RESNAPSHOT.equals(config.nonTrackablePolicy());

        List<String> taskTables = config.taskTables();
        client = createClient(config);

        try {
            List<TableState> started = tableStates(taskTables, config);
            Map<Map<String, String>, Map<String, Object>> durable = durableOffsets(started);
            for (TableState ts : started) {
                adoptHeldBookmarks(ts);
                restoreOffset(ts, durable.get(OffsetState.sourcePartition(db, ts.table)));
            }
            tables = started;
            // The config values were already logged by AbstractConfig when the config was built.
            LOG.info("CDC source task started: database={}, tables={}, holder={}", db, taskTables, holder);
        } catch (SQLException e) {
            throw new ConnectException("Failed to start CDC source task for " + db + " tables " + taskTables, e);
        }
    }

    /** One state per assigned table, its columns read from the server and its mapper built. */
    private List<TableState> tableStates(List<String> taskTables, StarRocksCdcSourceConfig config)
            throws SQLException {
        List<TableState> states = new ArrayList<>(taskTables.size());
        for (String t : taskTables) {
            List<ColumnMeta> cols = client.fetchColumns(db, t);
            states.add(new TableState(t, cols, new ChangeRecordMapper(db, t, config.topicFor(t), cols)));
        }
        return states;
    }

    /**
     * Every table's durable offset in one read: Connect's per-partition {@code offset} is a pass
     * over the offset topic each. {@code context} is set by the framework via
     * {@code initialize(SourceTaskContext)} before {@code start()}; it is null in unit tests that
     * construct a task directly, and every table is then fresh, as on a first-ever run.
     */
    private Map<Map<String, String>, Map<String, Object>> durableOffsets(List<TableState> states) {
        if (context == null || context.offsetStorageReader() == null) {
            return Collections.emptyMap();
        }
        return context.offsetStorageReader().offsets(partitionsOf(states));
    }

    /**
     * Re-enters every reference this holder still has on the table, so a predecessor's leftovers
     * are released by the fence instead of expiring by TTL, and so {@link #restoreOffset} can resume
     * from the oldest of them under {@code no_snapshot}. Best-effort.
     */
    private void adoptHeldBookmarks(TableState t) {
        List<Long> held;
        try {
            held = client.fetchHeldBookmarks(db, t.table, holder);
        } catch (SQLException e) {
            LOG.warn("Could not list the bookmarks {} holds on {}.{}; any left by a previous task expire by "
                    + "TTL instead of being released, and with no durable offset under no_snapshot the position "
                    + "an earlier start pinned is not recovered", holder, db, t.table, e);
            return;
        }
        for (Long id : held) {
            retain(t, id);
        }
        if (!held.isEmpty()) {
            LOG.info("Adopted {} bookmark(s) {} already held on {}.{}: {}", held.size(), holder, db, t.table, held);
        }
    }

    /**
     * Restores one table from its durable offset. Only {@code snapshot_done=true} is trusted;
     * anything else leaves the table fresh so the next poll redoes the snapshot -- except under
     * {@code no_snapshot}, where nothing is durable before the first change window: there the oldest
     * bookmark the holder still references, adopted by {@link #adoptHeldBookmarks}, is the position
     * an earlier start pinned, and resuming from it keeps every change since. A reset through the
     * offsets REST API releases those references first, so it still starts from the current
     * version. Logs where the table starts.
     */
    private void restoreOffset(TableState t, Map<String, Object> raw) {
        OffsetState state = OffsetState.fromMap(raw);
        if (state.snapshotDone) {
            t.committedBookmark = state.bookmarkId;
            t.snapshotDone = true;
            // Not optional: without this the resumed bookmark is never released, leaking one per
            // table per restart until its TTL. Safe, because commit() releases strictly below the
            // durable offset, and at restore time this id is exactly that offset.
            retain(t, state.bookmarkId);
            LOG.info("Table {}.{} starts at bookmark {}, the durable offset", db, t.table, state.bookmarkId);
            return;
        }
        if (snapshotInitial) {
            LOG.info("Table {}.{} starts fresh: the first poll takes the snapshot", db, t.table);
            return;
        }
        Long oldest;
        synchronized (t.liveBookmarks) {
            oldest = t.liveBookmarks.isEmpty() ? null : Collections.min(t.liveBookmarks);
        }
        if (oldest == null) {
            LOG.info("Table {}.{} starts fresh: the first poll pins the current version and streams from there",
                    db, t.table);
            return;
        }
        t.committedBookmark = oldest;
        t.snapshotDone = true;
        LOG.info("Table {}.{} starts at bookmark {}, the oldest reference {} still holds from an earlier start",
                db, t.table, oldest, holder);
    }

    /**
     * Reads one round over every table. A table that fails is skipped, not thrown from: the batch
     * holds records whose tables have already advanced {@code committedBookmark} in memory, and
     * Connect discards the return value of a poll that throws -- those records would never reach
     * Kafka and never be re-read. The failure surfaces only once the batch is empty, except a table
     * failing for longer than {@code source.poll.retry.timeout.ms}: that is fatal and thrown at once.
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> out = new ArrayList<>();
        SQLException failure = null;
        String failedTable = null;
        for (TableState t : tables) {
            drainPendingReleases(t);
            renewLiveBookmarks(t);
            final int emittedBefore = out.size();
            try {
                pollTable(t, out);
                t.firstFailureMs = -1L;
            } catch (NonTrackableException e) {
                // These records carry this window's head; committing them would advance the durable
                // position past a window never read to the end.
                discardFrom(out, emittedBefore);
                applyPolicy(t, e);
            } catch (SQLException e) {
                // Partial window: every record still carries head, so an ack would declare a window
                // durable that was never read to the end.
                discardFrom(out, emittedBefore);
                failUnlessRetriable(t, e);
                if (failure == null) {
                    failure = e;
                    failedTable = t.table;
                }
                LOG.warn("CDC poll failed for {}.{}; the remaining tables continue and this one is "
                        + "retried on the next poll", db, t.table, e);
            }
        }
        if (out.isEmpty() && failure != null) {
            // Connect retries only RetriableException and kills the task for anything else; it also
            // re-polls at once after one, so the pacing has to happen here.
            idleSleep(pollIntervalMs);
            throw new RetriableException("CDC poll failed for table " + db + "." + failedTable, failure);
        }
        if (out.isEmpty()) {
            idleSleep(pollIntervalMs);
            return null;
        }
        return out;
    }

    /** One table's turn: bootstrap, or open the window above the committed bookmark and emit it. */
    private void pollTable(TableState t, List<SourceRecord> out) throws SQLException, NonTrackableException {
        if (!t.snapshotDone) {
            bootstrap(t, out);
            return;
        }
        long head = client.bookmarkCreate(db, t.table, holder, ttlMs);
        if (head == t.committedBookmark) {
            return; // idle dedup: no new version since the last poll
        }
        retain(t, head);
        final long base = t.committedBookmark;
        final List<WindowRow> window = new ArrayList<>();
        client.streamChanges(db, t.table, t.cols, base, head,
                (row, changeType, rowVersion) -> window.add(new WindowRow(
                        t.mapper.toChangeRecord(row, changeType, rowVersion, base, head, true),
                        changeType, rowVersion)));
        if (window.isEmpty()) {
            // A version with no change rows (a compaction, say) still moves the head. Keep it as the
            // base: FE dedups create against the newest bookmark only, so releasing it meant create,
            // empty scan and release on every poll. The next real window names it, so the fence can release it.
            t.committedBookmark = head;
            LOG.debug("Empty CDC window for {}.{}: bookmark {} -> {}, base moved to head",
                    db, t.table, base, head);
            return;
        }
        final int windowStart = out.size();
        appendWindow(t, out, window);
        demoteAllButLast(out, windowStart, base);
        t.committedBookmark = head;
        LOG.info("Emitted CDC window for {}.{}: bookmark {} -> {}, {} record(s)",
                db, t.table, base, head, out.size() - windowStart);
    }

    private void bootstrap(TableState t, List<SourceRecord> out) throws SQLException, NonTrackableException {
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

    /** One CHANGES row, buffered so a deletion can be told from the delete half of an update. */
    private static final class WindowRow {
        final SourceRecord record;
        final int changeType;
        final long rowVersion;

        WindowRow(SourceRecord record, int changeType, long rowVersion) {
            this.record = record;
            this.changeType = changeType;
            this.rowVersion = rowVersion;
        }
    }

    /**
     * Appends the window, following each <em>real</em> deletion with a tombstone when tombstones are
     * on. StarRocks renders an update as DELETE(before) + INSERT(after) at one row version, so a
     * delete whose key comes back as an insert at that same version is half an update, not a
     * deletion -- and a tombstone there tells every compacted topic, KTable and delete-enabled sink
     * downstream to drop a row that still exists. A record with no key gets none either: a tombstone
     * identifies its row by key, so a keyless one deletes nothing and only adds noise.
     */
    private void appendWindow(TableState t, List<SourceRecord> out, List<WindowRow> window) {
        Set<List<Object>> updatedKeys = new HashSet<>();
        if (tombstones) {
            for (WindowRow w : window) {
                if (w.changeType != ChangeRecordMapper.CHANGE_TYPE_DELETE && w.record.key() != null) {
                    updatedKeys.add(Arrays.asList(w.record.key(), w.rowVersion));
                }
            }
        }
        for (WindowRow w : window) {
            out.add(w.record);
            if (tombstones && w.changeType == ChangeRecordMapper.CHANGE_TYPE_DELETE && w.record.key() != null
                    && !updatedKeys.contains(Arrays.asList(w.record.key(), w.rowVersion))) {
                out.add(t.mapper.tombstoneFor(w.record));
            }
        }
    }

    /**
     * Leaves only the window's last record carrying {@code head}; the rest carry {@code base}.
     * Connect commits the longest <em>acked prefix</em>, not the batch, so with every record naming
     * {@code head} an ack of the first alone would declare the window durable and a crash drop the
     * rest. Naming {@code base} costs a re-read bounded by one window.
     */
    private static void demoteAllButLast(List<SourceRecord> out, int windowStart, long base) {
        if (out.size() - windowStart < 2) {
            return;
        }
        Map<String, Object> baseOffset = OffsetState.sourceOffset(base, true);
        for (int i = windowStart; i < out.size() - 1; i++) {
            SourceRecord r = out.get(i);
            out.set(i, new SourceRecord(r.sourcePartition(), baseOffset, r.topic(),
                    r.keySchema(), r.key(), r.valueSchema(), r.value()));
        }
    }

    /**
     * Retains a bookmark, ignoring one already held. {@code bookmark_create} returns the holder's
     * existing bookmark on an unchanged table, so after a resnapshot {@link #bootstrap} reopens the
     * failed window's id; held twice, it would be released twice.
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

    /**
     * Turns a run of failed polls fatal once it has lasted {@code source.poll.retry.timeout.ms};
     * without a limit a dropped column or a revoked privilege retries forever as RUNNING.
     */
    private void failUnlessRetriable(TableState t, SQLException e) {
        long now = monotonicMs();
        if (t.firstFailureMs < 0) {
            t.firstFailureMs = now;
        }
        long failingForMs = now - t.firstFailureMs;
        if (pollRetryTimeoutMs >= 0 && failingForMs >= pollRetryTimeoutMs) {
            throw new ConnectException("CDC reads of table " + db + "." + t.table + " have failed for " + failingForMs
                    + " ms, longer than " + StarRocksCdcSourceConfig.POLL_RETRY_TIMEOUT_MS + "="
                    + pollRetryTimeoutMs, e);
        }
    }

    private void applyPolicy(TableState t, NonTrackableException e) {
        String note = leaseStateNote(t, monotonicMs());
        if (policyResnapshot) {
            // Leave liveBookmarks untouched: the failed window's bookmark really was created
            // server-side, and commit()'s release rule owns cleaning it up.
            LOG.warn("CHANGES window not trackable for {}.{} from base bookmark {}; {}=resnapshot, so this table's "
                            + "position is discarded and the whole table is re-read on the next poll. Cause: {}",
                    db, t.table, t.committedBookmark, StarRocksCdcSourceConfig.NONTRACKABLE_POLICY,
                    e.getMessage() + note);
            t.snapshotDone = false;
            t.committedBookmark = -1L;
        } else {
            LOG.error("CHANGES window not trackable for {}.{} from base bookmark {}; {}=fail, so the task stops. "
                            + "Cause: {}",
                    db, t.table, t.committedBookmark, StarRocksCdcSourceConfig.NONTRACKABLE_POLICY,
                    e.getMessage() + note);
            throw new ConnectException(
                    "CHANGES window not trackable for table " + db + "." + t.table + "." + note, e);
        }
    }

    /**
     * The lease as it stood at the failure, stated as fact, not as a cause: the FE messages naming
     * a bookmark are dropped partitions, rewrites and reshards, while a lapsed lease surfaces as a
     * BE ancestor-chain error.
     */
    private String leaseStateNote(TableState t, long nowMs) {
        if (t.effectiveTtlMs <= 0 || t.lastRenewMs < 0) {
            return "";
        }
        return " Lease at the time of failure: last renewed " + (nowMs - t.lastRenewMs)
                + " ms ago, server granted " + t.effectiveTtlMs + " ms.";
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
                // INFO: the only external sign that reclamation keeps up.
                LOG.info("Released bookmark {} for {}.{}", id, db, t.table);
            } catch (Exception e) {
                LOG.warn("Failed to release bookmark {} for {}.{} (holder {}); it stays pinned until its TTL "
                        + "expires", id, db, t.table, holder, e);
            }
        }
    }

    /**
     * Refreshes every bookmark the table holds, not only {@code committedBookmark}: any of them can
     * become the offset a restart resumes from, and {@code bookmarkCreate} returns the same id on an
     * unchanged table without moving the lease.
     *
     * <p>Paced off the lease the server granted, falling back to {@code source.bookmark.ttl.ms}
     * until one is known. Nothing latches renewal off, not even a "no expiry" answer: the ceiling
     * that bounds it is mutable.
     *
     * <p>Two known gaps, both needing per-bookmark scheduling: a poll interval at or above the lease
     * lapses it however the pacing is computed, and a round that renews anything counts as a
     * success, so one failing id waits a full pace.
     *
     * <p>Never triggers {@code source.nontrackable.policy}; a lost position surfaces at the next
     * CHANGES read.
     */
    private void renewLiveBookmarks(TableState t) {
        long lease = t.effectiveTtlMs > 0 ? t.effectiveTtlMs : ttlMs;
        long pace = lease > 0 ? Math.min(lease / 3, RENEW_MAX_INTERVAL_MS) : RENEW_MAX_INTERVAL_MS;
        long now = monotonicMs();
        if (t.lastRenewMs >= 0 && now - t.lastRenewMs < pace) {
            return;
        }
        if (t.renewBackoffMs > 0 && now - t.lastRenewAttemptMs < t.renewBackoffMs) {
            return;
        }
        List<Long> held;
        synchronized (t.liveBookmarks) {
            // The whole deque, not head-and-tail: the durable offset frequently names neither end,
            // and everything above it can still become the resume point.
            held = new ArrayList<>(t.liveBookmarks); // copy: commit() wants this monitor back
        }
        if (held.isEmpty()) {
            // Nothing to renew before bootstrap opens the first bookmark. Returning here keeps the
            // attempt clock and the failure backoff from treating "nothing held" as a failed round.
            return;
        }
        t.lastRenewAttemptMs = now;
        boolean anyRenewed = false;
        long roundLease = -1; // shortest finite grant of this round; -1 until one arrives
        for (Long id : held) {
            try {
                long granted = client.bookmarkRenew(db, t.table, id, holder, ttlMs);
                if (granted == 0) {
                    // -1 is the only "no expiry" the contract defines. Reading a 0 that way would
                    // stop renewal for the life of the task, silently, so call this one failed.
                    LOG.warn("Renewal of bookmark {} for {}.{} answered 0, which is not a lease; "
                            + "a later round retries", id, db, t.table);
                    continue;
                }
                if (granted > 0) {
                    roundLease = roundLease > 0 ? Math.min(roundLease, granted) : granted;
                }
                anyRenewed = true;
            } catch (Exception e) {
                LOG.warn("Failed to renew bookmark {} for {}.{} (holder {}); a later round retries",
                        id, db, t.table, holder, e);
            }
        }
        // Only a renewing round starts the pacing clock; a wholly failed one backs off instead,
        // from one poll interval, doubling to the cap below.
        if (anyRenewed) {
            if (roundLease > 0 && !t.warnedPollOutpacesLease && pollIntervalMs * 3 >= roundLease) {
                // Nothing else reports this: pacing is computed correctly and still cannot run,
                // because a round only happens at the top of a poll. The lease then lapses with no
                // signal until the next CHANGES read fails as non-trackable.
                t.warnedPollOutpacesLease = true;
                LOG.warn("source.poll.interval.ms={} cannot service the {} ms lease granted for {}.{}: "
                        + "renewal runs only once per poll, so fewer than three attempts fit in a "
                        + "lease. Lower the poll interval or raise bookmark_reference_max_ttl_ms.",
                        pollIntervalMs, roundLease, db, t.table);
            }
            // Grants within one round need not agree; the shortest binds. A -1 grant leaves the
            // lease unbounded rather than known, so it stays -1.
            t.effectiveTtlMs = roundLease;
            t.lastRenewMs = now;
            t.renewBackoffMs = 0;
        } else {
            // Before any grant, `lease` is only the configured guess -- and a non-positive one
            // says nothing at all, since the ceiling that will bound it is server-side. Cap at a
            // minute; a third of a 7-day guess would outwait a ceiling-capped lease entirely.
            long cap = 60_000L;
            if (t.effectiveTtlMs > 0) {
                cap = lease / 3;
            } else if (lease > 0) {
                cap = Math.min(lease / 3, 60_000L);
            }
            cap = Math.max(1L, cap); // a sub-3ms lease truncates to 0 and would disable the gate
            t.renewBackoffMs = Math.min(Math.max(Math.max(1L, pollIntervalMs), t.renewBackoffMs * 2), cap);
        }
    }

    @Override
    public void commit() {
        if (tables == null) {
            return;
        }
        Map<String, Long> durable = durableBookmarks();
        if (durable == null) {
            return;
        }
        for (TableState t : tables) {
            // Never above this task's own position. The offset store is keyed by connector name,
            // not by task generation, so a zombie predecessor still flushing would otherwise let
            // this task release the very bookmark it resumed from.
            long fence = Math.min(durable.get(t.table), t.committedBookmark);
            if (fence < 0) {
                continue;
            }
            synchronized (t.liveBookmarks) {
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

    /**
     * The bookmark Connect has actually made durable, per table, {@code -1} where it has none; null
     * when the store could not be read, which defers every release to the next commit. One read
     * for all tables: Connect's per-partition {@code offset} is a pass over the offset topic each.
     * Read from the offset store rather than tracked here: a max over acked records is an upper
     * bound on durability, never a lower one, and {@code commitRecord} also fires for records a
     * transformation filtered or {@code errors.tolerance=all} dropped, which are never written.
     */
    private Map<String, Long> durableBookmarks() {
        Map<Map<String, String>, Map<String, Object>> offsets;
        try {
            offsets = durableOffsets(tables);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof InterruptedException) {
                // Connect's reader wraps the interrupt without restoring the flag.
                Thread.currentThread().interrupt();
            } else {
                LOG.warn("Could not read the durable offsets; releases wait for the next commit", e);
            }
            return null;
        }
        Map<String, Long> result = new HashMap<>();
        for (TableState t : tables) {
            Map<String, Object> offset = offsets.get(OffsetState.sourcePartition(db, t.table));
            Object raw = offset == null ? null : offset.get(OffsetState.KEY_BOOKMARK_ID);
            result.put(t.table, raw instanceof Number ? ((Number) raw).longValue() : -1L);
        }
        return result;
    }

    /** Every table's partition, for the one read the offset store makes per call. */
    private List<Map<String, String>> partitionsOf(List<TableState> states) {
        List<Map<String, String>> partitions = new ArrayList<>(states.size());
        for (TableState t : states) {
            partitions.add(OffsetState.sourcePartition(db, t.table));
        }
        return partitions;
    }

    @Override
    public void stop() {
        if (client != null) {
            client.close();
        }
    }

    /** Test injection point: test subclasses override to return a scripted fake. */
    protected CdcClient createClient(StarRocksCdcSourceConfig config) {
        return new StarRocksJdbcClient(config);
    }

    /** Overridden in tests so idle polls do not really sleep. */
    void idleSleep(long ms) throws InterruptedException {
        Thread.sleep(ms);
    }

    /**
     * Monotonic milliseconds, overridden in tests to drive the renewal schedule without waiting out
     * a real TTL. Wall time would let a backward clock step stretch an interval past the lease it
     * paces. Measured from the first reading because nanoTime's origin may be negative, which would
     * sink {@code lastRenewMs} below the {@code -1} sentinel and disable the pacing gate.
     */
    long monotonicMs() {
        long n = nanoTime();
        if (monotonicOriginNanos == Long.MIN_VALUE) {
            monotonicOriginNanos = n;
        }
        return (n - monotonicOriginNanos) / 1_000_000L;
    }

    /** Seam for the one property {@link #monotonicMs} cannot show on a JVM whose origin is positive. */
    long nanoTime() {
        return System.nanoTime();
    }
}
