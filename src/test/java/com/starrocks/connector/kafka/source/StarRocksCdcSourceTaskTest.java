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
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import static org.junit.Assert.fail;

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
    private static final List<String> ORDERS_KEYS = Collections.singletonList("id");

    private FakeCdcClient fake;
    private StarRocksCdcSourceTask task;

    @Before
    public void setUp() {
        fake = new FakeCdcClient();
        fake.setColumns("orders", ORDERS_COLS);
        fake.setKeyColumns("orders", ORDERS_KEYS);
        task = newTask(fake);
    }

    private long fakeNowMs;

    /**
     * What Kafka Connect has actually made durable, which is what fences releases. Tests drive it
     * with {@link #flushOffset} instead of acking records: an ack is not durability, and modelling
     * it as one is the mistake the fence was rewritten to stop making.
     */
    private final Map<Map<String, String>, Map<String, Object>> durableOffsets = new HashMap<>();

    /** Tables whose offset read throws instead of answering -- it does IO and can be closed. */
    private final Set<String> offsetReadFailures = new HashSet<>();

    /** Records the offset Connect would have flushed for {@code table} at {@code bookmarkId}. */
    private void flushOffset(String table, long bookmarkId) {
        durableOffsets.put(OffsetState.sourcePartition("db1", table),
                OffsetState.sourceOffset(bookmarkId, true));
    }

    private SourceTaskContext contextReading(Map<Map<String, String>, Map<String, Object>> store) {
        OffsetStorageReader reader = new OffsetStorageReader() {
            @Override
            public <T> Map<String, Object> offset(Map<String, T> partition) {
                Object name = partition.get(OffsetState.KEY_TABLE);
                if (name != null && offsetReadFailures.contains(name.toString())) {
                    throw new org.apache.kafka.connect.errors.ConnectException("Failed to fetch offsets.");
                }
                return store.get(partition);
            }

            @Override
            public <T> Map<Map<String, T>, Map<String, Object>> offsets(
                    Collection<Map<String, T>> partitions) {
                throw new UnsupportedOperationException("unused by the task");
            }
        };
        return new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return Collections.emptyMap();
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return reader;
            }
        };
    }

    private StarRocksCdcSourceTask newTask(final FakeCdcClient fake) {
        StarRocksCdcSourceTask t = newBareTask(fake);
        t.initialize(contextReading(durableOffsets));
        return t;
    }

    private StarRocksCdcSourceTask newBareTask(final FakeCdcClient fake) {
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
        m.put(StarRocksCdcSourceConfig.POLL_INTERVAL_MS, "1");
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

    private static long bookmarkOf(SourceRecord r) {
        return ((Number) r.sourceOffset().get(OffsetState.KEY_BOOKMARK_ID)).longValue();
    }

    /**
     * Connect commits the offset of the longest <em>acked prefix</em> of a source partition, not of
     * the poll batch. If every record of a window named head, acking the first alone would make the
     * whole window durable; a crash before the rest were acked dropped them silently -- measured at
     * 4999 lost rows in a 5000-row window. Only the last record may name head.
     */
    @Test
    public void testOnlyTheLastRecordOfAWindowCarriesHead() throws Exception {
        fake.enqueueHead("orders", 100L);
        task.start(baseProps());
        task.poll();

        fake.enqueueHead("orders", 101L);
        fake.enqueueChanges("orders",
                new FakeCdcClient.ChangeRow(new Object[]{1, 10L}, 0, 5001L),
                new FakeCdcClient.ChangeRow(new Object[]{2, 20L}, 0, 5002L),
                new FakeCdcClient.ChangeRow(new Object[]{3, 30L}, 0, 5003L));
        List<SourceRecord> out = task.poll();

        assertEquals(3, out.size());
        assertEquals("a partial ack must resume at the window's start", 100L, bookmarkOf(out.get(0)));
        assertEquals(100L, bookmarkOf(out.get(1)));
        assertEquals("only the last record proves the window complete", 101L, bookmarkOf(out.get(2)));
        for (SourceRecord r : out) {
            // Demotion rewrites the offset wholesale, so it must carry snapshot_done forward too:
            // written false, restoreOffset would read the table as mid-snapshot and redo it whole.
            assertEquals(Boolean.TRUE, r.sourceOffset().get(OffsetState.KEY_SNAPSHOT_DONE));
        }
    }

    /**
     * Acknowledgement order cannot matter, because acknowledgement no longer fences anything.
     * Connect commits the longest acked <em>prefix</em> and also calls {@code commitRecord} for
     * records it dropped, so any watermark derived from acks is an upper bound on durability --
     * releasing against it unpins the bookmark a restart resumes from.
     */
    @Test
    public void testReleaseFollowsTheFlushNotTheWindowOrder() throws Exception {
        fake.enqueueHead("orders", 100L);
        task.start(baseProps());
        task.poll();

        fake.enqueueHead("orders", 101L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 10L}, 0, 5001L));
        List<SourceRecord> first = task.poll();
        fake.enqueueHead("orders", 102L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{2, 20L}, 0, 5002L));
        List<SourceRecord> second = task.poll();
        assertEquals(Arrays.asList(100L, 101L, 102L), liveBookmarksOf(task));

        // The later window's callback lands first, and nothing is flushed.
        task.commit();
        pollToFlushReleases();
        task.commit();
        pollToFlushReleases();
        assertTrue("acks released " + fake.releasedBookmarks, fake.releasedBookmarks.isEmpty());
        assertEquals(Arrays.asList(100L, 101L, 102L), liveBookmarksOf(task));

        // Only the flushed position moves the fence, and it releases strictly below itself.
        flushOffset("orders", 102L);
        task.commit();
        pollToFlushReleases();
        assertEquals(Collections.singletonList(102L), liveBookmarksOf(task));
    }

    /**
     * The release SQL is issued from the poll thread, so an authorized release only reaches the
     * client on the next poll; this drives one idle poll to flush it.
     */
    private void pollToFlushReleases() throws Exception {
        task.poll();
    }

    private static boolean warnedPollOutpacesLeaseOf(StarRocksCdcSourceTask task) throws Exception {
        java.lang.reflect.Field tablesField = StarRocksCdcSourceTask.class.getDeclaredField("tables");
        tablesField.setAccessible(true);
        Object state = ((List<?>) tablesField.get(task)).get(0);
        java.lang.reflect.Field f = state.getClass().getDeclaredField("warnedPollOutpacesLease");
        f.setAccessible(true);
        return f.getBoolean(state);
    }

    private static List<Long> liveBookmarksOf(StarRocksCdcSourceTask task, String table) throws Exception {
        java.lang.reflect.Field f = StarRocksCdcSourceTask.class.getDeclaredField("tables");
        f.setAccessible(true);
        for (Object st : (List<?>) f.get(task)) {
            java.lang.reflect.Field nameField = st.getClass().getDeclaredField("table");
            nameField.setAccessible(true);
            if (table.equals(nameField.get(st))) {
                java.lang.reflect.Field live = st.getClass().getDeclaredField("liveBookmarks");
                live.setAccessible(true);
                return new ArrayList<>((Deque<Long>) live.get(st));
            }
        }
        throw new AssertionError("no TableState for " + table);
    }

    private static List<Long> liveBookmarksOf(StarRocksCdcSourceTask task) {
        return new ArrayList<>(task.tables.get(0).liveBookmarks);
    }

    /**
     * A release is fenced by what Connect made durable, not by what was acked. Acking every record
     * of both windows must release nothing while the offset store still names the older bookmark.
     */
    @Test
    public void testReleaseWaitsForTheDurableOffsetNotTheAck() throws Exception {
        fake.enqueueHead("orders", 100L);
        fake.enqueueSnapshotRows("orders", rows(new Object[]{1, 100L}));
        task.start(baseProps());
        List<SourceRecord> b1Records = task.poll();

        fake.enqueueHead("orders", 101L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 5001L));
        List<SourceRecord> b2Records = task.poll();
        assertEquals(Arrays.asList(100L, 101L), liveBookmarksOf(task));

        // Everything acked, nothing flushed: the durable position is still wherever it was.
        task.commit();
        pollToFlushReleases();
        assertTrue("acks alone released " + fake.releasedBookmarks, fake.releasedBookmarks.isEmpty());
        assertEquals(Arrays.asList(100L, 101L), liveBookmarksOf(task));

        // Connect flushes 101; only now is 100 unreachable by any restart.
        flushOffset("orders", 101L);
        task.commit();
        pollToFlushReleases();
        assertEquals(Collections.singletonList("db1.orders:100:kc:c1"), fake.releasedBookmarks);
        assertEquals(Collections.singletonList(101L), liveBookmarksOf(task));

        // 101 is the durable position itself and stays pinned however often commit runs.
        task.commit();
        pollToFlushReleases();
        task.commit();
        pollToFlushReleases();
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
    public void testCommitNeverReleasesTheDurableBookmark() throws Exception {
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
        flushOffset("orders", 101L);

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

        // (b) Zero-row windows: two polls move the head without emitting any record, so no newer
        // offset can ever become durable. Each head is handed straight back -- keeping it would
        // pin a bookmark the fence can never reach -- while the acked bookmark, which is now the
        // oldest live one, must survive: releasing "all but the newest" here would release it.
        FakeCdcClient zeroRowFake = new FakeCdcClient();
        zeroRowFake.setColumns("orders", ORDERS_COLS);
        zeroRowFake.setKeyColumns("orders", ORDERS_KEYS);
        Map<Map<String, String>, Map<String, Object>> zeroRowOffsets = new HashMap<>();
        StarRocksCdcSourceTask zeroRowTask = newBareTask(zeroRowFake);
        zeroRowTask.initialize(contextReading(zeroRowOffsets));

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

        assertEquals("an empty window's head must not be retained", Collections.singletonList(200L),
                liveBookmarksOf(zeroRowTask));
        assertEquals(Arrays.asList("db1.orders:201:kc:c1", "db1.orders:202:kc:c1"),
                zeroRowFake.releasedBookmarks);

        // Run several commit cycles so the one-cycle release lag is fully paid off: the acked
        // bookmark must still be there afterwards, because it is the oldest live one.
        for (int cycle = 0; cycle < 3; cycle++) {
            zeroRowTask.commit();
            zeroRowTask.poll();
        }

        // Further empty polls keep handing their head back, so the release list grows; what must
        // hold however many cycles run is that the acked bookmark is not among them and is still
        // live. Asserting the list verbatim here would only pin the fake's reuse of released ids.
        assertFalse("the durable bookmark must never be released",
                zeroRowFake.releasedBookmarks.contains("db1.orders:200:kc:c1"));
        assertEquals(Collections.singletonList(200L), liveBookmarksOf(zeroRowTask));
    }

    // ------------------------------------------------------------------
    // Invariant 6: a tombstone follows a real deletion only. StarRocks renders an update as
    // DELETE(before) + INSERT(after) at one row version, and a tombstone on that delete would
    // tell every compacted topic and KTable downstream to drop a row that still exists.
    // ------------------------------------------------------------------

    private Map<String, String> tombstoneProps() {
        Map<String, String> m = baseProps();
        m.put(StarRocksCdcSourceConfig.TOMBSTONES_ON_DELETE, "true");
        return m;
    }

    @Test
    public void testUpdateGetsNoTombstoneOnItsDeleteHalf() throws Exception {
        fake.enqueueHead("orders", 100L);
        task.start(tombstoneProps());
        task.poll();

        // One UPDATE of id=2: the before value and the after value share a row version.
        fake.enqueueHead("orders", 101L);
        fake.enqueueChanges("orders",
                new FakeCdcClient.ChangeRow(new Object[]{2, 20L}, 1, 5002L),
                new FakeCdcClient.ChangeRow(new Object[]{2, 200L}, 0, 5002L));

        List<SourceRecord> out = task.poll();

        assertEquals("an update is d + c and nothing else", 2, out.size());
        assertEquals("d", ((Struct) out.get(0).value()).getString("op"));
        assertEquals("c", ((Struct) out.get(1).value()).getString("op"));
    }

    /** A key deleted at one version and re-inserted at a later one really was deleted in between. */
    @Test
    public void testDeleteFollowedByAReinsertAtALaterVersionStillGetsATombstone() throws Exception {
        fake.enqueueHead("orders", 100L);
        task.start(tombstoneProps());
        task.poll();

        fake.enqueueHead("orders", 101L);
        fake.enqueueChanges("orders",
                new FakeCdcClient.ChangeRow(new Object[]{2, 20L}, 1, 5002L),
                new FakeCdcClient.ChangeRow(new Object[]{2, 200L}, 0, 5003L));

        List<SourceRecord> out = task.poll();

        assertEquals(3, out.size());
        assertEquals("d", ((Struct) out.get(0).value()).getString("op"));
        assertNull("the tombstone follows the deletion", out.get(1).value());
        assertEquals("c", ((Struct) out.get(2).value()).getString("op"));
    }

    /** A tombstone names its row by key, so a keyless one deletes nothing and is only noise. */
    @Test
    public void testKeylessTableGetsNoTombstone() throws Exception {
        fake.setKeyColumns("orders", Collections.<String>emptyList());
        fake.enqueueHead("orders", 100L);
        task.start(tombstoneProps());
        task.poll();

        fake.enqueueHead("orders", 101L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{2, 20L}, 1, 5002L));

        List<SourceRecord> out = task.poll();

        assertEquals(1, out.size());
        assertNull(out.get(0).key());
        assertNotNull("the delete itself still ships", out.get(0).value());
    }

    @Test
    public void testCommitReleasesNothingBeforeAnyFlush() throws Exception {
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

    /**
     * A crash mid-snapshot leaves {@code snapshot_done=false} durable. Resuming from the bookmark it
     * names would skip whatever rows never made it out -- a silent gap. The table must go back to
     * fresh and redo the snapshot whole, duplicates and all.
     */
    @Test
    public void testRestoringAHalfDeliveredSnapshotRedoesItWhole() throws Exception {
        task.start(baseProps());
        StarRocksCdcSourceTask.TableState t = task.tables.get(0);
        task.restoreOffset(t, OffsetState.sourceOffset(11952L, false));

        fake.enqueueHead("orders", 11955L);
        fake.enqueueSnapshotRows("orders", rows(new Object[]{1, 100L}));

        List<SourceRecord> out = task.poll();

        assertEquals("the snapshot must be taken again", 1, fake.snapshotCalls);
        assertNotNull(out);
        assertTrue("no CHANGES window may be opened from a position that was never durable",
                fake.streamedWindows.isEmpty());
        assertTrue("nothing may be released: the restored id was never held",
                fake.releasedBookmarks.isEmpty());
    }

    /**
     * The consumer side of the same rule: a quoted bookmark id must not restore as "snapshot done
     * at -1". That state skips the snapshot forever, opens CHANGES from -1, and puts -1 in
     * liveBookmarks, where commit()'s fence can never rise above it.
     */
    @Test
    public void testRestoringAnUnparsableBookmarkIdRedoesTheSnapshot() throws Exception {
        task.start(baseProps());
        StarRocksCdcSourceTask.TableState t = task.tables.get(0);
        Map<String, Object> corrupt = new HashMap<>();
        corrupt.put(OffsetState.KEY_BOOKMARK_ID, "11952");
        corrupt.put(OffsetState.KEY_SNAPSHOT_DONE, true);
        task.restoreOffset(t, corrupt);

        assertEquals(-1L, t.committedBookmark);
        assertFalse(t.snapshotDone);
        assertTrue("-1 is not a bookmark and must never be renewed or released",
                liveBookmarksOf(task).isEmpty());

        fake.enqueueHead("orders", 11955L);
        fake.enqueueSnapshotRows("orders", rows(new Object[]{1, 100L}));
        task.poll();

        assertEquals("the snapshot must be taken again", 1, fake.snapshotCalls);
        assertTrue("no CHANGES window may be opened from -1", fake.streamedWindows.isEmpty());
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
        task.poll();

        // Acked but not flushed: the resumed base is still the durable position and must stay.
        task.commit();
        pollToFlushReleases();
        assertTrue("released too early: " + fake.releasedBookmarks, fake.releasedBookmarks.isEmpty());

        flushOffset("orders", 11955L);
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
        task.poll();
        flushOffset("orders", 102L);
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
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTL_MS, "900000");
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

        // Any of the three can still become the offset Connect flushes, so any of them can be the
        // id a restart resumes from; renewing only the ends loses whichever one that turns out to be.
        assertEquals(1, renewCountOf(100L));
        assertEquals(1, renewCountOf(101L));
        assertEquals(1, renewCountOf(102L));
    }

    /** A ceiling can cap the lease well below what was asked for, and is readable nowhere else. */
    @Test
    public void testRenewIntervalFollowsServerReportedTtlNotConfiguredTtl() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTL_MS, "900000");
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

    /** The request always carries the configured TTL; sending the granted one ratchets it down. */
    @Test
    public void testRenewAlwaysRequestsTheConfiguredTtl() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTL_MS, "900000");
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
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTL_MS, "900000");
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
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTL_MS, "900000");
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
        props.put(StarRocksCdcSourceConfig.POLL_INTERVAL_MS, "1000");
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTL_MS, "900000");
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
        props.put(StarRocksCdcSourceConfig.POLL_INTERVAL_MS, "20000");
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTL_MS, "900000");
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
        props.put(StarRocksCdcSourceConfig.POLL_INTERVAL_MS, "40000");
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTL_MS, "900000");
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

    /**
     * A poll interval at or near the granted lease guarantees a lapse whatever the pacing says,
     * because a round only runs at the top of a poll. Simulation put the exact boundary at
     * poll &gt;= lease; warn from a third of it, while attempts still fit.
     */
    @Test
    public void testWarnsOncePerTableWhenPollOutpacesTheLease() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.POLL_INTERVAL_MS, "20000");
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTL_MS, "900000");
        fake.grantedTtlMs = 30000L; // 3 * 20000 >= 30000: the poll cannot service this lease
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll();

        fakeNowMs += 1;
        task.poll();
        assertTrue("the first grant must set the flag", warnedPollOutpacesLeaseOf(task));

        // A grant the poll interval can service leaves the flag alone -- it is one-shot per table.
        fake.grantedTtlMs = 900000L;
        fakeNowMs += 300000L;
        task.poll();
        assertTrue(warnedPollOutpacesLeaseOf(task));
    }

    /** A partial round stamps the clock: the failed id waits for the next scheduled round. */
    @Test
    public void testPartialRoundStampsTheClock() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTL_MS, "900000");
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
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTL_MS, "2");
        props.put(StarRocksCdcSourceConfig.POLL_INTERVAL_MS, "1"); // the smallest the validator allows
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

    /** Only -1 means "no expiry"; a 0 must not latch renewal off for the life of the task. */
    @Test
    public void testZeroGrantIsNotReadAsNoExpiry() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTL_MS, "900000");
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
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTL_MS, "900000");
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
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTL_MS, "0");
        props.put(StarRocksCdcSourceConfig.POLL_INTERVAL_MS, "1000");
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
     * A non-positive {@code source.bookmark.ttl.ms} drops the per-reference limit, not the cluster
     * ceiling that outlives it, so the connector has to ask -- and keep asking, since that ceiling
     * is a mutable config that can appear after the first answer.
     */
    @Test
    public void testNonPositiveTtlRenewsAndKeepsReProbingNoExpiry() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTL_MS, "0");
        fake.grantedTtlMs = 3600000L; // the cluster ceiling the connector cannot see
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll();

        fakeNowMs += 1;
        task.poll();
        assertEquals("ttl<=0 leaves the ceiling in force, so renewal must still run", 1, renewCountOf(100L));

        // A third of this lease is 20 minutes, but the interval ceiling is what binds: an operator
        // lowering bookmark_reference_max_ttl_ms must not go unnoticed for a third of a stale lease.
        fakeNowMs += 300000L - 1;
        task.poll();
        assertEquals("the interval ceiling paces this, not the 20-minute third", 1, renewCountOf(100L));
        fakeNowMs += 1;
        task.poll();
        assertEquals(2, renewCountOf(100L));

        // A -1 answer is only true of the ceiling as it stands, so re-probing continues.
        FakeCdcClient uncapped = new FakeCdcClient();
        uncapped.setColumns("orders", ORDERS_COLS);
        uncapped.setKeyColumns("orders", ORDERS_KEYS);
        uncapped.grantedTtlMs = -1L;
        StarRocksCdcSourceTask other = newTask(uncapped);
        uncapped.enqueueHead("orders", 200L);
        // Same ttl<=0 config: with no lease from either side there is no third to pace off, and
        // only the interval ceiling stands between this and a renewal on every poll.
        other.start(props);
        other.poll();
        fakeNowMs += 1;
        other.poll();  // the server answers "no expiry"
        int afterLearning = uncapped.renewedBookmarks.size();
        assertEquals(1, afterLearning);

        fakeNowMs += 300000L - 1;
        other.poll();
        assertEquals("the re-probe is paced, not run on every poll",
                afterLearning, uncapped.renewedBookmarks.size());
        fakeNowMs += 1;
        other.poll();
        assertEquals("a mutable ceiling means \"no expiry\" has to be re-probed",
                afterLearning + 1, uncapped.renewedBookmarks.size());
        other.stop();
    }

    /**
     * A transient FE failure must not kill the task. Connect retries only RetriableException;
     * anything else is logged as "will not recover until manually restarted", which would turn
     * every FE restart and leader failover into an operator page.
     */
    @Test
    public void testTransientSqlFailureIsRetriableNotFatal() throws Exception {
        fake.enqueueHead("orders", 100L);
        task.start(baseProps());
        task.poll();

        fake.bookmarkCreateFailure = new SQLException("Communications link failure");
        try {
            task.poll();
            fail("expected the poll to surface the failure");
        } catch (RetriableException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("orders"));
        }
    }

    // ------------------------------------------------------------------
    // Invariant 5: one table's failure never discards another table's records, whose positions
    // have already advanced in memory by the time the batch is built.
    // ------------------------------------------------------------------

    private Map<String, String> twoTableProps() {
        Map<String, String> m = baseProps();
        m.put(StarRocksCdcSourceConfig.TABLE_NAMES, "orders,items");
        m.put(StarRocksCdcSourceConfig.TASK_TABLES, "orders,items");
        return m;
    }

    /** Registers a second table on the shared fake so a two-table task can start. */
    private void addItemsTable() {
        fake.setColumns("items", ORDERS_COLS);
        fake.setKeyColumns("items", ORDERS_KEYS);
    }

    /** Both tables bootstrapped at their first bookmark, emitting nothing. */
    private void bootstrapBothWithoutSnapshot() throws Exception {
        fake.enqueueHead("orders", 10L);
        fake.enqueueHead("items", 20L);
        Map<String, String> props = twoTableProps();
        props.put(StarRocksCdcSourceConfig.SNAPSHOT_MODE, StarRocksCdcSourceConfig.SNAPSHOT_MODE_NO_SNAPSHOT);
        task.start(props);
        assertNull(task.poll());
    }

    /**
     * Connect discards the return value of a poll that throws. Failing the whole poll on the second
     * table therefore dropped the first table's records while its committedBookmark had already
     * advanced past them: the window reached neither Kafka nor a later re-read.
     */
    @Test
    public void testOneTableFailingKeepsAnotherTablesWindow() throws Exception {
        addItemsTable();
        bootstrapBothWithoutSnapshot();

        fake.enqueueHead("orders", 11L);
        fake.enqueueHead("items", 21L);
        fake.enqueueChanges("orders",
                new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 5001L),
                new FakeCdcClient.ChangeRow(new Object[]{2, 200L}, 0, 5002L));
        fake.changesFailureByTable.put("items", new SQLException("Communications link failure"));

        List<SourceRecord> records = task.poll();
        assertNotNull(records);
        assertEquals("orders' window survives items' failure", 2, records.size());
        assertEquals(11L, task.tables.get(0).committedBookmark);
        assertEquals("items holds its position for the retry", 20L, task.tables.get(1).committedBookmark);
    }

    /** Same rule for the snapshot phase, where the loss is the whole table's existing contents. */
    @Test
    public void testOneTableFailingKeepsAnotherTablesSnapshot() throws Exception {
        addItemsTable();
        fake.enqueueHead("orders", 10L);
        fake.enqueueHead("items", 20L);
        fake.enqueueSnapshotRows("orders",
                rows(new Object[]{1, 100L}, new Object[]{2, 200L}, new Object[]{3, 300L}));
        fake.snapshotFailureByTable.put("items", new SQLException("Communications link failure"));

        task.start(twoTableProps());
        List<SourceRecord> records = task.poll();
        assertNotNull(records);
        assertEquals("orders' snapshot survives items' failure", 3, records.size());
        assertTrue(task.tables.get(0).snapshotDone);
        assertFalse("items must redo its snapshot", task.tables.get(1).snapshotDone);
    }

    /**
     * The failing table's own rows are dropped: until demoteAllButLast runs they all carry head, so
     * one ack would declare durable a window that was never read to the end.
     */
    @Test
    public void testFailingTablesPartialWindowIsNotEmitted() throws Exception {
        addItemsTable();
        bootstrapBothWithoutSnapshot();

        fake.enqueueHead("orders", 11L);
        fake.enqueueHead("items", 21L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 5001L));
        fake.enqueueChanges("items", new FakeCdcClient.ChangeRow(new Object[]{9, 900L}, 0, 5009L));
        fake.changesFailureByTable.put("items", new SQLException("Communications link failure"));

        List<SourceRecord> records = task.poll();
        assertNotNull(records);
        assertEquals("only orders' record is emitted", 1, records.size());
        for (SourceRecord r : records) {
            assertFalse(r.topic(), r.topic().endsWith(".items"));
        }
        assertEquals("items holds its position for the retry", 20L, task.tables.get(1).committedBookmark);
    }

    /** With nothing to return, the failure must still reach Connect rather than read as an idle poll. */
    @Test
    public void testFailureOnEveryTableIsStillRetriable() throws Exception {
        addItemsTable();
        bootstrapBothWithoutSnapshot();

        fake.enqueueHead("orders", 11L);
        fake.enqueueHead("items", 21L);
        fake.changesFailureByTable.put("orders", new SQLException("Communications link failure"));
        fake.changesFailureByTable.put("items", new SQLException("Communications link failure"));

        try {
            task.poll();
            fail("expected the poll to surface the failure");
        } catch (RetriableException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("orders"));
        }
    }

    /**
     * commit() walks every table. A table with no durable offset yet -- fresh, or just
     * resnapshotted -- must not stop the walk: skipping the rest would leave their bookmarks
     * accumulating and renewed forever, and no single-table test can show it.
     */
    @Test
    public void testATableWithoutADurableOffsetDoesNotBlockTheOthers() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "orders,items");
        props.put(StarRocksCdcSourceConfig.TASK_TABLES, "orders,items");
        fake.setColumns("items", ORDERS_COLS);
        fake.setKeyColumns("items", ORDERS_KEYS);
        fake.enqueueHead("orders", 100L);
        fake.enqueueHead("items", 500L);
        task.start(props);
        task.poll();

        fake.enqueueHead("items", 501L);
        fake.enqueueChanges("items", new FakeCdcClient.ChangeRow(new Object[]{1, 10L}, 0, 5001L));
        task.poll();
        assertEquals(Arrays.asList(500L, 501L), liveBookmarksOf(task, "items"));

        // Only the second table has ever flushed; the first is still bare.
        flushOffset("items", 501L);
        task.commit();
        pollToFlushReleases();
        assertEquals("items must be released even though orders has no offset",
                Collections.singletonList(501L), liveBookmarksOf(task, "items"));
    }

    /**
     * The offset store is keyed by connector name, not by task generation, so a predecessor that is
     * still flushing can push the durable offset past what this task has read. Releasing against it
     * would drop the very bookmark this task resumed from, and the symptom would surface as a
     * non-trackable CHANGES read pointing nowhere near commit().
     */
    @Test
    public void testFenceNeverOutrunsThisTasksOwnPosition() throws Exception {
        fake.enqueueHead("orders", 100L);
        task.start(baseProps());
        task.restoreOffset(task.tables.get(0), OffsetState.sourceOffset(100L, true));
        assertEquals(Collections.singletonList(100L), liveBookmarksOf(task));

        // A zombie predecessor flushes a window this task never read.
        flushOffset("orders", 150L);
        task.commit();
        pollToFlushReleases();
        assertTrue("released its own resume base: " + fake.releasedBookmarks,
                fake.releasedBookmarks.isEmpty());
        assertEquals(Collections.singletonList(100L), liveBookmarksOf(task));
    }

    /**
     * A non-trackable failure carries the lease as it stood, whatever the cause. Stating the cause
     * was wrong both ways -- the FE messages naming a bookmark are dropped partitions and reshards,
     * while a real lapse usually arrives as a BE ancestor-chain error -- so the numbers go out and
     * the inference does not.
     */
    @Test
    public void testNonTrackableFailureReportsTheLeaseAsFact() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTL_MS, "900000");
        fake.grantedTtlMs = 900000L;
        fake.enqueueHead("orders", 100L);
        task.start(props);
        task.poll();
        fakeNowMs += 1;
        task.poll(); // learns the granted lease

        fakeNowMs += 1000L;
        fake.enqueueHead("orders", 101L);
        fake.nonTrackableMessage = "CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): ancestor chain cannot reach base version";
        fake.failNextChangesWithNonTrackable("orders");
        try {
            task.poll();
            fail("expected the non-trackable failure to surface");
        } catch (ConnectException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("server granted 900000 ms"));
            assertTrue(e.getMessage(), e.getMessage().contains("last renewed 1000 ms ago"));
            assertFalse("must not assert a cause: " + e.getMessage(),
                    e.getMessage().contains("lapsed") || e.getMessage().contains("not polling"));
        }
    }

    /** No lease learned means no numbers to report, and inventing them from the config would lie. */
    @Test
    public void testNonTrackableFailureSaysNothingWhenNoLeaseIsKnown() throws Exception {
        fake.bookmarkRenewFailure = new SQLException("no such function: bookmark_renew");
        fake.enqueueHead("orders", 100L);
        task.start(baseProps());
        task.poll();
        fakeNowMs += 1;
        task.poll();

        fake.enqueueHead("orders", 101L);
        fake.failNextChangesWithNonTrackable("orders");
        try {
            task.poll();
            fail("expected the non-trackable failure to surface");
        } catch (ConnectException e) {
            assertFalse(e.getMessage(), e.getMessage().contains("Lease at the time of failure"));
        }
    }

    /**
     * The offset read does IO and can fail or be closed under us. One unreadable table must not
     * cost every table after it its releases -- with a return instead of a continue, a single
     * failure silently stops reclaiming for the rest of the task.
     */
    @Test
    public void testAFailedOffsetReadDoesNotSkipTheRemainingTables() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "orders,items");
        props.put(StarRocksCdcSourceConfig.TASK_TABLES, "orders,items");
        fake.setColumns("items", ORDERS_COLS);
        fake.setKeyColumns("items", ORDERS_KEYS);
        fake.enqueueHead("orders", 100L);
        fake.enqueueHead("items", 500L);
        task.start(props);
        task.poll();

        fake.enqueueHead("items", 501L);
        fake.enqueueChanges("items", new FakeCdcClient.ChangeRow(new Object[]{1, 10L}, 0, 5001L));
        task.poll();
        flushOffset("items", 501L);

        // orders is read first and throws; items must still be reclaimed.
        offsetReadFailures.add("orders");
        try {
            task.commit();
        } finally {
            offsetReadFailures.clear();
        }
        pollToFlushReleases();
        assertEquals("a throwing read for one table must not skip the next",
                Collections.singletonList(501L), liveBookmarksOf(task, "items"));
    }

    /** Renewal must not decide a position is lost: that belongs to the CHANGES read alone. */
    @Test
    public void testRenewFailureLeavesStateUntouched() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StarRocksCdcSourceConfig.BOOKMARK_TTL_MS, "900000");
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
