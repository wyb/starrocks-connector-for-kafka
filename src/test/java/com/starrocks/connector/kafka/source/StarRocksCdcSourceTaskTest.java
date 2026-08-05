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

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import java.sql.Types;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Correctness-core tests for {@link StarRocksCdcSourceTask}: the poll loop and the bookmark
 * state machine. Each test pins one of the three invariants documented on the task itself
 * (release-after-commit, snapshot-crash semantics, idle dedup) or one concrete branch of the
 * poll/bootstrap/commit control flow.
 */
public class StarRocksCdcSourceTaskTest {

    private static final List<ColumnMeta> ORDERS_COLS = Arrays.asList(
            new ColumnMeta("id", Types.INTEGER, 10, 0, false),
            new ColumnMeta("amount", Types.BIGINT, 19, 0, true));
    private static final List<String> ORDERS_PKS = Collections.singletonList("id");

    private FakeCdcClient fake;
    private StarRocksCdcSourceTask task;

    @Before
    public void setUp() {
        fake = new FakeCdcClient();
        fake.setColumns("orders", ORDERS_COLS);
        fake.setPrimaryKeys("orders", ORDERS_PKS);
        task = newTask(fake);
    }

    private StarRocksCdcSourceTask newTask(final FakeCdcClient fake) {
        return new StarRocksCdcSourceTask() {
            @Override
            protected CdcClient createClient(StarRocksCdcSourceConfig cfg) {
                return fake;
            }
        };
    }

    private Map<String, String> baseProps() {
        Map<String, String> m = new HashMap<>();
        m.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:mysql://fe1:9030,fe2:9030");
        m.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db1");
        m.put(StarRocksCdcSourceConfig.USERNAME, "root");
        m.put(StarRocksCdcSourceConfig.PASSWORD, "");
        m.put(StarRocksCdcSourceConfig.TABLE_NAMES, "orders");
        m.put(StarRocksCdcSourceConfig.TASK_TABLES, "orders");
        m.put(StarRocksCdcSourceConfig.POLL_INTERVALMS, "1");
        m.put("name", "c1");
        return m;
    }

    private static List<Object[]> rows(Object[]... rows) {
        return Arrays.asList(rows);
    }

    // ------------------------------------------------------------------
    // Invariant 2: snapshot rows carry snapshot_done=false; the first change record (and every
    // one thereafter) carries snapshot_done=true.
    // ------------------------------------------------------------------

    @Test
    public void testSnapshotRowsCarrySnapshotNotDoneOffsets() throws Exception {
        fake.enqueueHead("orders", 100L);
        fake.enqueueSnapshotRows("orders", rows(new Object[]{1, 100L}, new Object[]{2, 200L}));

        task.start(baseProps());
        List<SourceRecord> out = task.poll();

        assertNotNull(out);
        assertEquals(2, out.size());
        for (SourceRecord r : out) {
            Struct value = (Struct) r.value();
            assertEquals("r", value.getString("op"));
            assertEquals(Boolean.FALSE, r.sourceOffset().get("snapshot_done"));
            assertEquals(100L, r.sourceOffset().get("bookmark_id"));
        }
    }

    @Test
    public void testFirstChangeRecordCarriesSnapshotDoneTrue() throws Exception {
        fake.enqueueHead("orders", 100L);
        task.start(baseProps());
        task.poll(); // snapshot round: no rows queued, just completes bootstrap

        fake.enqueueHead("orders", 101L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 5001L));
        List<SourceRecord> out = task.poll();

        assertNotNull(out);
        assertEquals(1, out.size());
        SourceRecord r = out.get(0);
        assertEquals(Boolean.TRUE, r.sourceOffset().get("snapshot_done"));
        assertEquals(101L, r.sourceOffset().get("bookmark_id"));
        assertTrue(fake.streamedWindows.contains("100_101"));
    }

    @Test
    public void testNoSnapshotModeSkipsSnapshot() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.SNAPSHOT_MODE, "no_snapshot");
        fake.enqueueHead("orders", 100L);

        task.start(props);
        List<SourceRecord> out = task.poll();

        assertNull(out);
        assertEquals(0, fake.snapshotCalls);

        fake.enqueueHead("orders", 101L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 5001L));
        task.poll();
        assertTrue(fake.streamedWindows.contains("100_101"));
    }

    // ------------------------------------------------------------------
    // Invariant 3: bookmarkCreate returning the already-committed bookmark means idle -> nothing
    // emitted, no new CHANGES window opened.
    // ------------------------------------------------------------------

    @Test
    public void testIdleDedupEmitsNothing() throws Exception {
        fake.enqueueHead("orders", 100L);
        task.start(baseProps());
        task.poll(); // snapshot round completes; committedBookmark == 100

        int windowsBefore = fake.streamedWindows.size();
        List<SourceRecord> out = task.poll(); // no new head queued -> fake repeats 100

        assertNull(out);
        assertEquals(windowsBefore, fake.streamedWindows.size());
    }

    @Test
    public void testChangeRecordsCarryHeadBookmark() throws Exception {
        fake.enqueueHead("orders", 100L);
        task.start(baseProps());
        task.poll();

        fake.enqueueHead("orders", 101L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 5001L));
        List<SourceRecord> out1 = task.poll();
        assertEquals(1, out1.size());
        SourceRecord r1 = out1.get(0);
        assertEquals(101L, r1.sourceOffset().get("bookmark_id"));
        Struct bookmark1 = (Struct) ((Struct) ((Struct) r1.value()).get("source")).get("bookmark");
        assertEquals(100L, bookmark1.getInt64("base").longValue());
        assertEquals(101L, bookmark1.getInt64("head").longValue());

        fake.enqueueHead("orders", 102L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{2, 200L}, 0, 5002L));
        List<SourceRecord> out2 = task.poll();
        assertEquals(1, out2.size());
        SourceRecord r2 = out2.get(0);
        assertEquals(102L, r2.sourceOffset().get("bookmark_id"));
        Struct bookmark2 = (Struct) ((Struct) ((Struct) r2.value()).get("source")).get("bookmark");
        assertEquals(101L, bookmark2.getInt64("base").longValue());
        assertEquals(102L, bookmark2.getInt64("head").longValue());
    }

    // ------------------------------------------------------------------
    // Invariant 1: a bookmark is released only once commit() has authorized it, and commit()
    // authorizes strictly below the ack watermark as it stood at the PREVIOUS commit() -- one whole
    // commit cycle behind, so the fence can never be ahead of what the last flush made durable.
    // ------------------------------------------------------------------

    /** Acks every record as Kafka Connect would once its producer send completed. */
    private void ack(List<SourceRecord> records) throws Exception {
        for (SourceRecord r : records) {
            task.commitRecord(r, null);
        }
    }

    /**
     * The release SQL is issued from the poll thread, so an authorized release only reaches the
     * client on the next poll; this drives one idle poll to flush it.
     */
    private void pollToFlushReleases() throws Exception {
        task.poll();
    }

    private static List<Long> liveBookmarksOf(StarRocksCdcSourceTask task) {
        return new ArrayList<>(task.tables.get(0).liveBookmarks);
    }

    /**
     * The release fence lags the acks by one commit cycle, deliberately.
     *
     * <p>Connect calls {@code commitRecord} from the producer send callback, but the flush that
     * precedes a {@code commit()} only persists the offsets snapshotted earlier on the task thread.
     * So at the moment {@code commit()} runs, the ack watermark can already name a bookmark newer
     * than the durable offset -- and releasing everything below the ack watermark would drop
     * exactly the bookmark that flush just made durable. Acting on the previous cycle's watermark
     * instead keeps the fence at or behind durability: the first {@code commit()} after acks
     * arrive releases nothing, the second releases what has by then provably been superseded.
     */
    @Test
    public void testCommitLagsOneCycleBehindAcks() throws Exception {
        fake.enqueueHead("orders", 100L); // B1
        fake.enqueueSnapshotRows("orders", rows(new Object[]{1, 100L}));
        task.start(baseProps());
        List<SourceRecord> b1Records = task.poll();

        fake.enqueueHead("orders", 101L); // B2
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 5001L));
        List<SourceRecord> b2Records = task.poll();

        assertEquals(Arrays.asList(100L, 101L), liveBookmarksOf(task));

        // Both windows' records have been acked, so the ack watermark is 101 -- but the flush that
        // is about to trigger this commit() may only have persisted the offset naming 100.
        ack(b1Records);
        ack(b2Records);

        task.commit();
        pollToFlushReleases();

        assertTrue("first commit after acks released " + fake.released, fake.released.isEmpty());
        assertEquals(Arrays.asList(100L, 101L), liveBookmarksOf(task));

        // One more commit cycle has now elapsed, so 101's offset is durable too and 100 is safe.
        task.commit();
        pollToFlushReleases();

        assertEquals(Collections.singletonList("db1.orders:100:kc:c1"), fake.released);
        assertEquals(Collections.singletonList(101L), liveBookmarksOf(task));

        // Still nothing newer: 101 is the ack watermark and stays pinned however often commit runs.
        task.commit();
        pollToFlushReleases();
        task.commit();
        pollToFlushReleases();
        assertEquals(Collections.singletonList("db1.orders:100:kc:c1"), fake.released);
        assertEquals(Collections.singletonList(101L), liveBookmarksOf(task));
    }

    /**
     * Safety property: whatever else commit() releases, it must never release the bookmark the
     * durably-committed offset names -- releasing it unpins that version for vacuum, and a restart
     * then resumes from a base StarRocks can no longer read ("bookmark N not found"), which
     * {@code policy=fail} turns into a permanently dead task and {@code policy=resnapshot} into a
     * silent full re-read. Commits are repeated here so the assertions bite after the one-cycle
     * release lag has been paid off, not merely because the fence had not caught up yet.
     */
    @Test
    public void testCommitNeverReleasesTheAckedBookmark() throws Exception {
        // (a) Four bookmarks live, acked only through B2=101: B1=100 is releasable, B2 is not.
        fake.enqueueHead("orders", 100L); // B1
        fake.enqueueSnapshotRows("orders", rows(new Object[]{1, 100L}));
        task.start(baseProps());
        List<SourceRecord> b1Records = task.poll();

        fake.enqueueHead("orders", 101L); // B2
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 5001L));
        List<SourceRecord> b2Records = task.poll();

        fake.enqueueHead("orders", 102L); // B3
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{2, 200L}, 0, 5002L));
        task.poll();

        fake.enqueueHead("orders", 103L); // B4
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{3, 300L}, 0, 5003L));
        task.poll();

        assertEquals(Arrays.asList(100L, 101L, 102L, 103L), liveBookmarksOf(task));

        // Kafka Connect confirmed records up to and including B2's; B3/B4's are still in flight.
        ack(b1Records);
        ack(b2Records);

        // First commit only arms the fence at the ack watermark; the second is the one that acts.
        task.commit();
        pollToFlushReleases();
        task.commit();
        pollToFlushReleases();

        assertEquals(Collections.singletonList("db1.orders:100:kc:c1"), fake.released);
        assertEquals(Arrays.asList(101L, 102L, 103L), liveBookmarksOf(task));

        // Repeating commit() must not start releasing the acked bookmark either, however many
        // further cycles run with no newer ack.
        task.commit();
        pollToFlushReleases();
        task.commit();
        pollToFlushReleases();
        assertEquals(Collections.singletonList("db1.orders:100:kc:c1"), fake.released);
        assertEquals(Arrays.asList(101L, 102L, 103L), liveBookmarksOf(task));

        // (b) Zero-row windows: two polls advance the position and append to the deque without
        // emitting any record, so no newer offset can ever become durable. The acked bookmark is
        // then the oldest live one and nothing at all may be released -- releasing "all but the
        // newest two" here would release exactly the acked bookmark.
        FakeCdcClient zeroRowFake = new FakeCdcClient();
        zeroRowFake.setColumns("orders", ORDERS_COLS);
        zeroRowFake.setPrimaryKeys("orders", ORDERS_PKS);
        StarRocksCdcSourceTask zeroRowTask = newTask(zeroRowFake);

        zeroRowFake.enqueueHead("orders", 200L); // acked bookmark
        zeroRowFake.enqueueSnapshotRows("orders", rows(new Object[]{7, 700L}));
        zeroRowTask.start(baseProps());
        for (SourceRecord r : zeroRowTask.poll()) {
            zeroRowTask.commitRecord(r, null);
        }

        zeroRowFake.enqueueHead("orders", 201L);
        assertNull(zeroRowTask.poll()); // window folded to zero rows: nothing emitted
        zeroRowFake.enqueueHead("orders", 202L);
        assertNull(zeroRowTask.poll());

        assertEquals(Arrays.asList(200L, 201L, 202L), liveBookmarksOf(zeroRowTask));

        // Run several commit cycles so the one-cycle release lag is fully paid off: the acked
        // bookmark must still be there afterwards, because it is the oldest live one.
        for (int cycle = 0; cycle < 3; cycle++) {
            zeroRowTask.commit();
            zeroRowTask.poll();
        }

        assertTrue("released " + zeroRowFake.released, zeroRowFake.released.isEmpty());
        assertEquals(Arrays.asList(200L, 201L, 202L), liveBookmarksOf(zeroRowTask));
    }

    @Test
    public void testCommitReleasesNothingBeforeAnyAck() throws Exception {
        fake.enqueueHead("orders", 100L);
        fake.enqueueSnapshotRows("orders", rows(new Object[]{1, 100L}));
        task.start(baseProps());
        task.poll();

        fake.enqueueHead("orders", 101L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 5001L));
        task.poll();

        fake.enqueueHead("orders", 102L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{2, 200L}, 0, 5002L));
        task.poll();

        // No record has been acked, so no offset is known to be durable and nothing may be dropped,
        // however many bookmarks have piled up -- and however many commit cycles run, so that this
        // pins the no-ack rule itself rather than just the one-cycle release lag.
        for (int cycle = 0; cycle < 3; cycle++) {
            task.commit();
            pollToFlushReleases();
        }

        assertTrue("released " + fake.released, fake.released.isEmpty());
        assertEquals(Arrays.asList(100L, 101L, 102L), liveBookmarksOf(task));
    }

    @Test
    public void testRestartResumesFromCommittedOffset() throws Exception {
        task.start(baseProps());
        StarRocksCdcSourceTask.TableState t = task.tables.get(0);
        task.restoreOffset(t, OffsetState.sourceOffset(11952L, true));

        fake.enqueueHead("orders", 11955L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 9001L));

        List<SourceRecord> out = task.poll();

        assertNotNull(out);
        assertEquals(1, out.size());
        assertTrue(fake.streamedWindows.contains("11952_11955"));
        assertEquals(0, fake.snapshotCalls);
        assertTrue(fake.released.isEmpty());
    }

    /**
     * A restart used to orphan the bookmark it resumed from: restoreOffset set committedBookmark
     * but never put the id back in the retention deque, so no commit cycle could reach it and it
     * stayed pinned against vacuum for the whole TTL -- once per rebalance, restart or config
     * edit, per table. Seen live in a cluster smoke run: bookmark 21275 survived untouched while
     * its neighbours 21253, 21314 and 21389 were all released.
     */
    @Test
    public void testRestartedBookmarkIsEventuallyReleased() throws Exception {
        task.start(baseProps());
        StarRocksCdcSourceTask.TableState t = task.tables.get(0);
        task.restoreOffset(t, OffsetState.sourceOffset(11952L, true));

        // Retained rather than dropped on the floor.
        assertEquals(Collections.singletonList(11952L), liveBookmarksOf(task));

        fake.enqueueHead("orders", 11955L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 9001L));
        ack(task.poll());

        // Still fenced: it survives until a newer window's records are acknowledged AND a full
        // commit cycle has elapsed -- the same protection every crash-replay base gets.
        task.commit();
        pollToFlushReleases();
        assertTrue("released too early: " + fake.released, fake.released.isEmpty());

        task.commit();
        pollToFlushReleases();
        assertEquals(Collections.singletonList("db1.orders:11952:kc:c1"), fake.released);
        assertEquals(Collections.singletonList(11955L), liveBookmarksOf(task));
    }

    // ------------------------------------------------------------------
    // Non-trackable CHANGES window: policy-driven fail vs. resnapshot.
    // ------------------------------------------------------------------

    @Test(expected = ConnectException.class)
    public void testNonTrackableWindowFailPolicyThrows() throws Exception {
        fake.enqueueHead("orders", 100L);
        task.start(baseProps());
        task.poll();

        fake.enqueueHead("orders", 101L);
        fake.failNextChangesWithNonTrackable("orders");
        task.poll();
    }

    @Test
    public void testNonTrackableWindowResnapshotPolicyResets() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.NONTRACKABLE_POLICY, "resnapshot");
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll();

        fake.enqueueHead("orders", 101L);
        fake.failNextChangesWithNonTrackable("orders");
        List<SourceRecord> out2 = task.poll();
        assertNull(out2);

        fake.enqueueHead("orders", 102L);
        fake.enqueueSnapshotRows("orders", rows(new Object[]{9, 900L}));
        List<SourceRecord> out3 = task.poll();

        assertNotNull(out3);
        assertEquals(1, out3.size());
        Struct value = (Struct) out3.get(0).value();
        assertEquals("r", value.getString("op"));
    }

    // ------------------------------------------------------------------
    // Tombstones on delete, when enabled.
    // ------------------------------------------------------------------

    @Test
    public void testTombstoneEmittedAfterDeleteWhenEnabled() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.TOMBSTONES_ON_DELETE, "true");
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll();

        fake.enqueueHead("orders", 101L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 1, 5001L));
        List<SourceRecord> out = task.poll();

        assertNotNull(out);
        assertEquals(2, out.size());
        SourceRecord deleteRecord = out.get(0);
        Struct value = (Struct) deleteRecord.value();
        assertEquals("d", value.getString("op"));

        SourceRecord tombstone = out.get(1);
        assertNull(tombstone.value());
        assertNull(tombstone.valueSchema());
        assertEquals(deleteRecord.key(), tombstone.key());
        assertFalse(deleteRecord == tombstone);
    }

    /**
     * Scripted-and-recording {@link CdcClient} test double. bookmarkCreate() consumes a
     * per-table queue of enqueued head values, repeating the last dequeued value once the queue
     * runs dry (simulating an idle table: no new bookmark version to report). streamChanges()
     * consumes and clears the table's queued {@link ChangeRow}s after replaying them, unless the
     * table was armed via {@link #failNextChangesWithNonTrackable} -- then it throws once and
     * leaves any queued changes untouched.
     */
    static final class FakeCdcClient implements CdcClient {

        private final Map<String, List<ColumnMeta>> columnsByTable = new HashMap<>();
        private final Map<String, List<String>> pksByTable = new HashMap<>();
        private final Map<String, Deque<Long>> queuedHeadsByTable = new HashMap<>();
        private final Map<String, Long> lastHeadByTable = new HashMap<>();
        private final Map<String, List<Object[]>> queuedSnapshotRowsByTable = new HashMap<>();
        private final Map<String, List<ChangeRow>> queuedChangesByTable = new HashMap<>();
        private final Set<String> failNextChangesTables = new HashSet<>();

        final List<String> released = new ArrayList<>();
        final List<String> createdHolders = new ArrayList<>();
        final List<String> streamedWindows = new ArrayList<>();
        int snapshotCalls = 0;

        void setColumns(String table, List<ColumnMeta> cols) {
            columnsByTable.put(table, cols);
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
        public long bookmarkCreate(String db, String table, String holder, long ttlMs) {
            createdHolders.add(holder);
            Deque<Long> queue = queuedHeadsByTable.get(table);
            long value;
            if (queue != null && !queue.isEmpty()) {
                value = queue.pollFirst();
            } else {
                value = lastHeadByTable.containsKey(table) ? lastHeadByTable.get(table) : -1L;
            }
            lastHeadByTable.put(table, value);
            return value;
        }

        @Override
        public void bookmarkRelease(String db, String table, long bookmarkId, String holder) {
            released.add(db + "." + table + ":" + bookmarkId + ":" + holder);
        }

        @Override
        public List<ColumnMeta> fetchColumns(String db, String table) {
            return columnsByTable.get(table);
        }

        @Override
        public List<String> fetchPrimaryKeys(String db, String table) {
            return pksByTable.get(table);
        }

        @Override
        public String fetchTableModel(String db, String table) {
            return "DUP_KEYS";
        }

        @Override
        public boolean cdcPropertyEnabled(String db, String table) {
            return true;
        }

        @Override
        public void streamSnapshot(String db, String table, List<String> cols, long bookmarkId, RowConsumer consumer) {
            snapshotCalls++;
            List<Object[]> queued = queuedSnapshotRowsByTable.get(table);
            if (queued != null) {
                for (Object[] row : queued) {
                    consumer.accept(row);
                }
            }
        }

        @Override
        public void streamChanges(String db, String table, List<String> cols, long base, long head,
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
}
