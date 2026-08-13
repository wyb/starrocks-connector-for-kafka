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

import java.sql.SQLException;
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

    private long fakeNowMs;

    private StarRocksCdcSourceTask newTask(final FakeCdcClient fake) {
        return new StarRocksCdcSourceTask() {
            @Override
            protected CdcClient createClient(StarRocksCdcSourceConfig cfg) {
                return fake;
            }

            @Override
            long monotonicMs() {
                return fakeNowMs;
            }

            @Override
            void idleSleep(long ms) {
                // the fake clock drives the schedule; real sleeping only slows the suite
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

        assertTrue("first commit after acks released " + fake.releasedBookmarks, fake.releasedBookmarks.isEmpty());
        assertEquals(Arrays.asList(100L, 101L), liveBookmarksOf(task));

        // One more commit cycle has now elapsed, so 101's offset is durable too and 100 is safe.
        task.commit();
        pollToFlushReleases();

        assertEquals(Collections.singletonList("db1.orders:100:kc:c1"), fake.releasedBookmarks);
        assertEquals(Collections.singletonList(101L), liveBookmarksOf(task));

        // Still nothing newer: 101 is the ack watermark and stays pinned however often commit runs.
        task.commit();
        pollToFlushReleases();
        task.commit();
        pollToFlushReleases();
        assertEquals(Collections.singletonList("db1.orders:100:kc:c1"), fake.releasedBookmarks);
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

        assertEquals(Collections.singletonList("db1.orders:100:kc:c1"), fake.releasedBookmarks);
        assertEquals(Arrays.asList(101L, 102L, 103L), liveBookmarksOf(task));

        // Repeating commit() must not start releasing the acked bookmark either, however many
        // further cycles run with no newer ack.
        task.commit();
        pollToFlushReleases();
        task.commit();
        pollToFlushReleases();
        assertEquals(Collections.singletonList("db1.orders:100:kc:c1"), fake.releasedBookmarks);
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

        assertTrue("released " + zeroRowFake.releasedBookmarks, zeroRowFake.releasedBookmarks.isEmpty());
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

        assertTrue("released " + fake.releasedBookmarks, fake.releasedBookmarks.isEmpty());
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
        assertTrue(fake.releasedBookmarks.isEmpty());
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
        assertTrue("released too early: " + fake.releasedBookmarks, fake.releasedBookmarks.isEmpty());

        task.commit();
        pollToFlushReleases();
        assertEquals(Collections.singletonList("db1.orders:11952:kc:c1"), fake.releasedBookmarks);
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

    /**
     * The resnapshot deliberately leaves the failed window's bookmark retained, and the bootstrap
     * that follows asks for a new one -- but on a table that has not changed since,
     * {@code bookmark_create} hands back that very id. Retaining it twice makes commit() release it
     * twice; the second call fails against a real FE and is logged as a bookmark left pinned, which
     * is the opposite of what happened.
     */
    @Test
    public void testResnapshotReopeningTheSameBookmarkReleasesItOnce() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.NONTRACKABLE_POLICY, "resnapshot");
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll();

        fake.enqueueHead("orders", 101L);
        fake.failNextChangesWithNonTrackable("orders");
        task.poll();

        // No head queued, so the fake repeats 101 -- what an unchanged table really does here.
        fake.enqueueSnapshotRows("orders", rows(new Object[]{9, 900L}));
        task.poll();
        assertEquals("101 must be retained once, not once per reopen",
                Arrays.asList(100L, 101L), liveBookmarksOf(task));

        // Drive 100 and 101 below the fence, then check each was released exactly once.
        fake.enqueueHead("orders", 102L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 5001L));
        ack(task.poll());
        task.commit();
        task.commit();
        pollToFlushReleases();

        assertEquals("released: " + fake.releasedBookmarks, 1, releaseCountOf(101L));
        assertEquals("released: " + fake.releasedBookmarks, 1, releaseCountOf(100L));
    }

    private int releaseCountOf(long bookmarkId) {
        int count = 0;
        for (String released : fake.releasedBookmarks) {
            if (released.contains(":" + bookmarkId + ":")) {
                count++;
            }
        }
        return count;
    }

    /**
     * A window that delivered rows and then failed must emit none of them. Their offsets name this
     * window's head, so committing them would move the durable position past a window that was
     * never read to the end, while the resnapshot discards the in-memory position that would have
     * redone it -- a crash in between then resumes from head and the unread tail is gone.
     */
    @Test
    public void testResnapshotEmitsNothingFromTheWindowThatFailedPartWay() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.NONTRACKABLE_POLICY, "resnapshot");
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll();

        fake.enqueueHead("orders", 101L);
        fake.enqueueChanges("orders",
                new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 5001L),
                new FakeCdcClient.ChangeRow(new Object[]{2, 200L}, 0, 5002L));
        fake.failNextChangesAfterEmitting("orders");
        assertNull("rows from the failed window must not reach Kafka", task.poll());
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
    // Lease renewal: an idle table must not lose its position to TTL expiry.
    // ------------------------------------------------------------------

    private int renewCountOf(long bookmarkId) {
        int count = 0;
        for (String renewed : fake.renewedBookmarks) {
            if (renewed.contains(":" + bookmarkId + ":")) {
                count++;
            }
        }
        return count;
    }

    /** Older windows stay pinned while acks lag, and their leases run down just like the newest. */
    @Test
    public void testRenewCoversEveryLiveBookmarkNotJustCommitted() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTLMS, "900000");
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll();

        // Two more windows, none acked, so all three stay live.
        fake.enqueueHead("orders", 101L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 5001L));
        task.poll();
        fake.enqueueHead("orders", 102L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{2, 200L}, 0, 5002L));
        task.poll();
        assertEquals(Arrays.asList(100L, 101L, 102L), liveBookmarksOf(task));

        fake.renewedBookmarks.clear();
        fakeNowMs += 900000L / 3;
        task.poll();

        assertEquals(1, renewCountOf(100L));
        assertEquals(1, renewCountOf(101L));
        assertEquals(1, renewCountOf(102L));
    }

    /** A ceiling can cap the lease well below what was asked for, and is readable nowhere else. */
    @Test
    public void testRenewIntervalFollowsServerReportedTtlNotConfiguredTtl() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTLMS, "900000");
        fake.grantedTtlMs = 30000L; // the cluster caps every lease at 30s
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll(); // bootstrap; nothing held yet, so nothing renewed
        fakeNowMs += 1;
        task.poll(); // first renewal: learns the granted lease

        // Past a third of the granted lease (10s), nowhere near a third of the configured one
        // (300s): only one of the two pacings renews here.
        fake.renewedBookmarks.clear();
        fakeNowMs += 30000L / 3 + 1;
        task.poll();
        assertEquals("must pace off the granted lease, not the configured one", 1, renewCountOf(100L));

        // And not more often than that lease calls for.
        fake.renewedBookmarks.clear();
        fakeNowMs += 30000L / 3 - 1;
        task.poll();
        assertEquals(0, renewCountOf(100L));
    }

    /**
     * A round where every renewal failed must retry on the next poll, not a third of the configured
     * TTL later: until one succeeds the pacing is off the configured value, which the cluster may
     * have capped far below.
     */
    @Test
    public void testFailedRoundRetriesOnTheNextPoll() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTLMS, "900000");
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll();

        fake.bookmarkRenewFailure = new SQLException("synthetic renew failure");
        fakeNowMs += 1;
        task.poll();
        assertEquals(1, renewCountOf(100L));

        fake.renewedBookmarks.clear();
        fakeNowMs += 1;
        task.poll();
        assertEquals("a wholly failed round must not start the clock", 1, renewCountOf(100L));
    }

    /** The request always carries the configured TTL; sending the granted one ratchets it down. */
    @Test
    public void testRenewAlwaysRequestsTheConfiguredTtl() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTLMS, "900000");
        fake.grantedTtlMs = 30000L;
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll();
        fakeNowMs += 1;
        task.poll();

        fake.requestedTtls.clear();
        fakeNowMs += 30000L / 3 + 1;
        task.poll();
        assertEquals(Collections.singletonList(900000L), fake.requestedTtls);
    }

    /** A lease shortened server-side mid-run must re-pace; latching the first grant misses it. */
    @Test
    public void testRenewRepacesWhenTheGrantChanges() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTLMS, "900000");
        fake.grantedTtlMs = 600000L;
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll();
        fakeNowMs += 1;
        task.poll(); // learns 600000

        fake.grantedTtlMs = 30000L;
        fake.renewedBookmarks.clear();
        fakeNowMs += 600000L / 3;
        task.poll(); // renews on the old pace, learns 30000
        assertEquals(1, renewCountOf(100L));

        fake.renewedBookmarks.clear();
        fakeNowMs += 30000L / 3 + 1;
        task.poll();
        assertEquals("must re-pace onto the shortened lease", 1, renewCountOf(100L));
    }

    /**
     * The headline scenario of the clock rule: a round that succeeded, then a wholly failed one --
     * the failed round must not restart the lease/3 wait. Kills the mutant that also stamps the
     * clock whenever a grant was ever learned.
     */
    @Test
    public void testWhollyFailedRoundAfterASuccessRetriesPromptly() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTLMS, "900000");
        fake.grantedTtlMs = 30000L;
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll();
        fakeNowMs += 1;
        task.poll(); // successful round, learns the 30s lease

        fake.bookmarkRenewFailure = new SQLException("synthetic renew failure");
        fakeNowMs += 30000L / 3;
        task.poll(); // wholly failed round
        fake.renewedBookmarks.clear();

        fakeNowMs += 1; // one poll interval (baseProps sets 1ms)
        task.poll();
        assertEquals("the failed round must not have restarted the lease/3 wait", 1, renewCountOf(100L));
    }

    /** Consecutive wholly failed rounds back off instead of hammering every poll. */
    @Test
    public void testRepeatedFailuresBackOff() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.POLL_INTERVALMS, "1000");
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTLMS, "900000");
        fake.bookmarkRenewFailure = new SQLException("no bookmark_renew on this cluster");
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll(); // bootstrap; nothing held yet

        fakeNowMs += 1;
        task.poll(); // first attempt fails -> backoff = 1000
        assertEquals(1, renewCountOf(100L));

        fakeNowMs += 999;
        task.poll();
        assertEquals("inside the backoff window, no retry", 1, renewCountOf(100L));

        fakeNowMs += 1;
        task.poll(); // second attempt -> backoff = 2000
        assertEquals(2, renewCountOf(100L));

        fakeNowMs += 1999;
        task.poll();
        assertEquals("the backoff doubles", 2, renewCountOf(100L));

        fakeNowMs += 1;
        task.poll();
        assertEquals(3, renewCountOf(100L));
    }

    /** A success must clear backoff built under the old lease, or it outgates the new one. */
    @Test
    public void testSuccessClearsStaleBackoff() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.POLL_INTERVALMS, "20000");
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTLMS, "900000");
        fake.bookmarkRenewFailure = new SQLException("synthetic renew failure");
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll(); // bootstrap

        fakeNowMs += 1;
        task.poll(); // fail -> backoff 20000
        fakeNowMs += 20000;
        task.poll(); // fail -> backoff 40000

        fake.bookmarkRenewFailure = null;
        fake.grantedTtlMs = 30000L;
        fakeNowMs += 40000;
        task.poll(); // success: learns a 30s lease, must also reset the 40s backoff
        fake.renewedBookmarks.clear();

        fakeNowMs += 30000L / 3 + 1;
        task.poll();
        assertEquals("stale backoff must not outgate the granted lease's schedule", 1, renewCountOf(100L));
    }

    /** Backoff is capped: at a minute before any grant, at lease/3 after one. */
    @Test
    public void testBackoffCaps() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.POLL_INTERVALMS, "40000");
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTLMS, "900000");
        fake.bookmarkRenewFailure = new SQLException("synthetic renew failure");
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll(); // bootstrap

        fakeNowMs += 1;
        task.poll(); // fail -> backoff 40000
        fakeNowMs += 40000;
        task.poll(); // fail -> backoff would double to 80000, but the unknown-phase cap is 60000
        assertEquals(2, renewCountOf(100L));

        fake.renewedBookmarks.clear();
        fakeNowMs += 59999;
        task.poll();
        assertEquals("capped at one minute while the lease is unknown", 0, renewCountOf(100L));
        fakeNowMs += 1;
        task.poll();
        assertEquals(1, renewCountOf(100L));

        // Learn a lease, then rebuild backoff: the cap becomes lease/3.
        fake.bookmarkRenewFailure = null;
        fake.grantedTtlMs = 9000L;
        fakeNowMs += 60000;
        task.poll(); // success, lease 9000
        fake.bookmarkRenewFailure = new SQLException("synthetic renew failure");
        fake.renewedBookmarks.clear();
        fakeNowMs += 3000;
        task.poll(); // fail -> backoff 40000 -> capped to 3000
        assertEquals(1, renewCountOf(100L));
        fakeNowMs += 3000;
        task.poll();
        assertEquals("granted-phase cap is lease/3", 2, renewCountOf(100L));
    }

    /** A partial round stamps the clock: the failed id waits for the next scheduled round. */
    @Test
    public void testPartialRoundStampsTheClock() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTLMS, "900000");
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll();
        fake.enqueueHead("orders", 101L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 5001L));
        task.poll(); // two live bookmarks, none acked

        fake.failRenewOfBookmarks.add(100L);
        fakeNowMs += 900000L / 3;
        task.poll(); // 101 renews, 100 fails -> clock stamped
        fake.renewedBookmarks.clear();

        fakeNowMs += 1;
        task.poll();
        assertTrue("a partial round schedules the next one normally", fake.renewedBookmarks.isEmpty());
    }

    /** A 1-2ms lease truncates its own third to zero; the cap floor keeps the gate alive. */
    @Test
    public void testSubThreeMillisecondLeaseStillBacksOff() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTLMS, "2");
        props.put(StarRocksCdcSourceConfig.POLL_INTERVALMS, "0");
        fake.bookmarkRenewFailure = new SQLException("synthetic renew failure");
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll(); // bootstrap

        fakeNowMs += 1;
        task.poll();
        assertEquals(1, renewCountOf(100L));
        task.poll(); // same millisecond
        assertEquals("a zero cap would hammer renewal every poll", 1, renewCountOf(100L));
    }

    /** nanoTime's origin may be negative, which would sink lastRenewMs below its -1 sentinel. */
    @Test
    public void testMonotonicClockIsNonNegativeFromAnyOrigin() {
        StarRocksCdcSourceTask real = new StarRocksCdcSourceTask() {
            @Override
            long nanoTime() {
                return Long.MIN_VALUE / 2;
            }
        };
        assertTrue("a negative clock disables the pacing gate", real.monotonicMs() >= 0);
    }

    /** {@code poll.intervalms=0} is a legal max-throughput setting; the backoff floor outlives it. */
    @Test
    public void testZeroPollIntervalStillBacksOff() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.POLL_INTERVALMS, "0");
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTLMS, "900000");
        fake.bookmarkRenewFailure = new SQLException("synthetic renew failure");
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll(); // bootstrap

        fakeNowMs += 1;
        task.poll();
        assertEquals(1, renewCountOf(100L));
        task.poll(); // same millisecond
        assertEquals("a zero floor would hammer renewal every poll", 1, renewCountOf(100L));
    }

    /** Only -1 means "no expiry"; a 0 must not latch renewal off for the life of the task. */
    @Test
    public void testZeroGrantIsNotReadAsNoExpiry() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTLMS, "900000");
        fake.grantedTtlMs = 0L;
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll(); // bootstrap

        fakeNowMs += 1;
        task.poll();
        assertEquals(1, renewCountOf(100L));
        fakeNowMs += 60000L;
        task.poll();
        assertEquals("a 0 answer must leave renewal running", 2, renewCountOf(100L));
    }

    /** Grants within a round need not agree -- the shortest one has to set the pace. */
    @Test
    public void testRoundPacesOffItsShortestGrant() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTLMS, "900000");
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll(); // bootstrap

        fake.enqueueHead("orders", 101L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 5001L));
        task.poll();
        assertEquals(Arrays.asList(100L, 101L), liveBookmarksOf(task));

        // The longer grant is last, so last-write-wins would pick it and pace 120x too slowly.
        fake.grantedTtlByBookmark.put(100L, 30000L);
        fake.grantedTtlByBookmark.put(101L, 3600000L);
        fake.renewedBookmarks.clear();
        fakeNowMs += 900000L;
        task.poll();
        assertEquals(1, renewCountOf(100L));
        assertEquals(1, renewCountOf(101L));

        fake.renewedBookmarks.clear();
        fakeNowMs += 30000L / 3 - 1;
        task.poll();
        assertTrue("the 1h grant must not set the pace", fake.renewedBookmarks.isEmpty());
        fakeNowMs += 1;
        task.poll();
        assertEquals("the shortest grant of the round binds", 1, renewCountOf(100L));
    }

    /** A non-positive ttl leaves the lease unknown, so the cap must not collapse to zero. */
    @Test
    public void testNonPositiveTtlStillBacksOffOnFailure() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTLMS, "0");
        props.put(StarRocksCdcSourceConfig.POLL_INTERVALMS, "1000");
        fake.bookmarkRenewFailure = new SQLException("synthetic renew failure");
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll(); // bootstrap

        fakeNowMs += 1;
        task.poll();
        assertEquals(1, renewCountOf(100L));

        fakeNowMs += 999;
        task.poll();
        assertEquals("a zero cap would retry every poll", 1, renewCountOf(100L));
        fakeNowMs += 1;
        task.poll();
        assertEquals(2, renewCountOf(100L));
    }

    /**
     * A non-positive {@code source.bookmark.ttlms} drops the per-reference limit, not the cluster
     * ceiling that outlives it, so the connector has to ask -- and keep asking, since that ceiling
     * is a mutable config that can appear after the first answer.
     */
    @Test
    public void testNonPositiveTtlRenewsAndKeepsReProbingNoExpiry() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTLMS, "0");
        fake.grantedTtlMs = 3600000L; // the cluster ceiling the connector cannot see
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll();

        fakeNowMs += 1;
        task.poll();
        assertEquals("ttl<=0 leaves the ceiling in force, so renewal must still run", 1, renewCountOf(100L));

        fakeNowMs += 3600000L / 3 - 1;
        task.poll();
        assertEquals("and is then paced off the granted ceiling", 1, renewCountOf(100L));
        fakeNowMs += 1;
        task.poll();
        assertEquals(2, renewCountOf(100L));

        // A -1 answer is only true of the ceiling as it stands, so re-probing continues.
        FakeCdcClient uncapped = new FakeCdcClient();
        uncapped.setColumns("orders", ORDERS_COLS);
        uncapped.setPrimaryKeys("orders", ORDERS_PKS);
        uncapped.grantedTtlMs = -1L;
        StarRocksCdcSourceTask other = newTask(uncapped);
        uncapped.enqueueHead("orders", 200L);
        other.start(baseProps());
        other.poll();
        fakeNowMs += 1;
        other.poll();  // the server answers "no expiry"
        int afterLearning = uncapped.renewedBookmarks.size();
        assertEquals(1, afterLearning);

        fakeNowMs += 900000L / 3 - 1;
        other.poll();
        assertEquals("the re-probe is paced, not run on every poll",
                afterLearning, uncapped.renewedBookmarks.size());
        fakeNowMs += 1;
        other.poll();
        assertEquals("a mutable ceiling means \"no expiry\" has to be re-probed",
                afterLearning + 1, uncapped.renewedBookmarks.size());
        other.stop();
    }

    /** Renewal must not decide a position is lost: that belongs to the CHANGES read alone. */
    @Test
    public void testRenewFailureLeavesStateUntouched() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTLMS, "900000");
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll();

        fake.bookmarkRenewFailure = new SQLException("synthetic renew failure");
        fakeNowMs += 900000L / 3;
        task.poll();

        assertEquals(Collections.singletonList(100L), liveBookmarksOf(task));
        assertEquals(100L, task.tables.get(0).committedBookmark);
        assertTrue(task.tables.get(0).snapshotDone);

        // And the stream keeps working once the failure clears.
        fake.bookmarkRenewFailure = null;
        fake.enqueueHead("orders", 101L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 5001L));
        List<SourceRecord> out = task.poll();
        assertNotNull(out);
        assertEquals(1, out.size());
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
}
