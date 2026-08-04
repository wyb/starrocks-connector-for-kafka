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

    /**
     * Connect 的 sourcePartition：{"db": db, "table": table}
     */
    public static Map<String, String> sourcePartition(String db, String table) {
        Map<String, String> partition = new HashMap<>();
        partition.put(KEY_DB, db);
        partition.put(KEY_TABLE, table);
        return partition;
    }

    /**
     * Connect 的 sourceOffset：{"bookmark_id": Long, "snapshot_done": Boolean}
     */
    public static Map<String, Object> sourceOffset(long bookmarkId, boolean snapshotDone) {
        Map<String, Object> offset = new HashMap<>();
        offset.put(KEY_BOOKMARK_ID, bookmarkId);
        offset.put(KEY_SNAPSHOT_DONE, snapshotDone);
        return offset;
    }

    /**
     * 从 offsetStorageReader 读回的 raw map 恢复。
     * raw == null 或缺 bookmark_id → fresh()。
     * bookmark_id 可能反序列化成 Integer/Long，统一 ((Number) v).longValue()。
     * snapshot_done 缺失或非 Boolean.TRUE → false。
     */
    public static OffsetState fromMap(Map<String, Object> raw) {
        if (raw == null) {
            return fresh();
        }

        long bookmarkId = -1L;
        if (raw.containsKey(KEY_BOOKMARK_ID)) {
            Object bmId = raw.get(KEY_BOOKMARK_ID);
            if (bmId instanceof Number) {
                bookmarkId = ((Number) bmId).longValue();
            }
        }

        boolean snapshotDone = false;
        if (raw.containsKey(KEY_SNAPSHOT_DONE)) {
            Object sd = raw.get(KEY_SNAPSHOT_DONE);
            if (Boolean.TRUE.equals(sd)) {
                snapshotDone = true;
            }
        }

        return new OffsetState(bookmarkId, snapshotDone);
    }
}
