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

import java.util.HashMap;
import java.util.Map;

/**
 * Connect's offset for one captured table: the partition names {@code (db, table)}, the offset
 * carries the bookmark to resume from and whether the snapshot finished. Keying it per table is
 * what lets a rebalance move a table to another task and still resume it where it left off.
 */
public final class OffsetState {
    public static final String KEY_DB = "db";
    public static final String KEY_TABLE = "table";
    public static final String KEY_BOOKMARK_ID = "bookmark_id";
    public static final String KEY_SNAPSHOT_DONE = "snapshot_done";

    public final long bookmarkId;
    public final boolean snapshotDone;

    public OffsetState(long bookmarkId, boolean snapshotDone) {
        this.bookmarkId = bookmarkId;
        this.snapshotDone = snapshotDone;
    }

    public static OffsetState fresh() {
        return new OffsetState(-1L, false);
    }

    public static Map<String, String> sourcePartition(String db, String table) {
        Map<String, String> partition = new HashMap<>();
        partition.put(KEY_DB, db);
        partition.put(KEY_TABLE, table);
        return partition;
    }

    public static Map<String, Object> sourceOffset(long bookmarkId, boolean snapshotDone) {
        Map<String, Object> offset = new HashMap<>();
        offset.put(KEY_BOOKMARK_ID, bookmarkId);
        offset.put(KEY_SNAPSHOT_DONE, snapshotDone);
        return offset;
    }

    /**
     * Restores what {@code offsetStorageReader} handed back. Read as {@link Number}, not cast to
     * {@code Long}: the offset store round-trips through JSON, so a bookmark id small enough to fit
     * an int comes back as an {@code Integer}.
     *
     * <p>Anything that is not a position leaves the table fresh. Pairing the {@code -1} sentinel
     * with {@code snapshot_done=true} would restore "done at -1": the snapshot is skipped forever
     * and {@code commit()}'s fence never rises above zero, so the table can never release again.
     */
    public static OffsetState fromMap(Map<String, Object> raw) {
        if (raw == null) {
            return fresh();
        }
        // Missing, unparsable and negative all land on the -1 sentinel, and all three mean the
        // same thing: no position.
        Object bmId = raw.get(KEY_BOOKMARK_ID);
        long bookmarkId = bmId instanceof Number ? ((Number) bmId).longValue() : -1L;
        if (bookmarkId < 0) {
            return fresh();
        }
        return new OffsetState(bookmarkId, Boolean.TRUE.equals(raw.get(KEY_SNAPSHOT_DONE)));
    }
}
