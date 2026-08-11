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

    @Test
    public void testMissingSnapshotDoneDefaultsFalse() {
        Map<String, Object> m = new HashMap<>();
        m.put(OffsetState.KEY_BOOKMARK_ID, 5L);
        OffsetState s = OffsetState.fromMap(m);
        assertEquals(5L, s.bookmarkId);
        assertFalse(s.snapshotDone);
    }

    @Test
    public void testMissingBookmarkIdIgnoresSnapshotDone() {
        Map<String, Object> m = new HashMap<>();
        m.put(OffsetState.KEY_SNAPSHOT_DONE, true);
        OffsetState s = OffsetState.fromMap(m);
        assertEquals(-1L, s.bookmarkId);
        assertFalse(s.snapshotDone);
    }
}
