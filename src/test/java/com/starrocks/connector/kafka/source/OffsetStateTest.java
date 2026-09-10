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

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class OffsetStateTest {

    /**
     * Every snapshot row carries {@code snapshot_done=false}; a reader that accepted any non-null
     * value would resume a half-delivered snapshot instead of redoing it, which is the one gap the
     * marker exists to prevent.
     */
    @Test
    public void testSnapshotDoneFalseIsNotReadAsDone() {
        assertFalse(OffsetState.fromMap(OffsetState.sourceOffset(11955L, false)).snapshotDone);
        assertEquals(11955L, OffsetState.fromMap(OffsetState.sourceOffset(11955L, false)).bookmarkId);
    }

    @Test
    public void testRoundTrip() {
        Map<String, Object> m = OffsetState.sourceOffset(11955L, true);
        OffsetState s = OffsetState.fromMap(m);
        assertEquals(11955L, s.bookmarkId);
        assertTrue(s.snapshotDone);

        Map<String, String> partition = OffsetState.sourcePartition("db1", "t");
        assertEquals(2, partition.size());
        assertEquals("db1", partition.get(OffsetState.KEY_DB));
        assertEquals("t", partition.get(OffsetState.KEY_TABLE));
    }

    @Test
    public void testNullMeansFresh() {
        OffsetState s = OffsetState.fromMap(null);
        assertEquals(-1L, s.bookmarkId);
        assertFalse(s.snapshotDone);
    }

    @Test
    public void testIntegerBookmarkIdTolerated() {
        Map<String, Object> m = new HashMap<>();
        m.put(OffsetState.KEY_BOOKMARK_ID, Integer.valueOf(7));
        OffsetState s = OffsetState.fromMap(m);
        assertEquals(7L, s.bookmarkId);
        assertFalse(s.snapshotDone);
    }

    /**
     * Anything but a non-negative number is no position, and snapshot_done is then ignored. Pairing
     * the -1 sentinel with {@code snapshot_done=true} would restore "done at -1": the snapshot is
     * skipped forever and commit()'s fence never rises above zero. A null value reads the same as a
     * missing key, so the missing case rides on it.
     */
    @Test
    public void testAnythingButANonNegativeIdIgnoresSnapshotDone() {
        for (Object bad : new Object[]{"11955", Boolean.TRUE, null, -1L, -7L}) {
            Map<String, Object> m = new HashMap<>();
            m.put(OffsetState.KEY_BOOKMARK_ID, bad);
            m.put(OffsetState.KEY_SNAPSHOT_DONE, true);
            OffsetState s = OffsetState.fromMap(m);
            assertEquals(String.valueOf(bad), -1L, s.bookmarkId);
            assertFalse(String.valueOf(bad), s.snapshotDone);
        }
    }

    /** Only Boolean.TRUE is "done"; absent, a quoted "true" or a 1 all mean redo the snapshot. */
    @Test
    public void testOnlyBooleanTrueIsDone() {
        for (Object sd : new Object[]{null, "true", 1}) {
            Map<String, Object> m = new HashMap<>();
            m.put(OffsetState.KEY_BOOKMARK_ID, 5L);
            if (sd != null) {
                m.put(OffsetState.KEY_SNAPSHOT_DONE, sd);
            }
            OffsetState s = OffsetState.fromMap(m);
            assertEquals(String.valueOf(sd), 5L, s.bookmarkId);
            assertFalse(String.valueOf(sd), s.snapshotDone);
        }
    }
}
