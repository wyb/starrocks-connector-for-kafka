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
    // Invariant 1: old bookmarks are released only from commit(), keeping the newest two.
    // ------------------------------------------------------------------

    @Test
    public void testCommitReleasesAllButNewestTwo() throws Exception {
        fake.enqueueHead("orders", 100L);
        task.start(baseProps());
        task.poll();

        fake.enqueueHead("orders", 101L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{1, 100L}, 0, 5001L));
        task.poll();

        fake.enqueueHead("orders", 102L);
        fake.enqueueChanges("orders", new FakeCdcClient.ChangeRow(new Object[]{2, 200L}, 0, 5002L));
        task.poll();

        task.commit();
        assertEquals(Collections.singletonList("db1.orders:100:kc:c1"), fake.released);

        task.commit();
        assertEquals(1, fake.released.size());
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
