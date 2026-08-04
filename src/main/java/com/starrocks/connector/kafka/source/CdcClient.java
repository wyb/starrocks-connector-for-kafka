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

import java.sql.SQLException;
import java.util.List;

/**
 * StarRocks-facing operations needed by the CDC source task and connector: bookmark lifecycle,
 * table metadata discovery, and streaming reads of a snapshot or a CHANGES range.
 *
 * <p>Implementations own their JDBC connection lifecycle; callers must {@link #close()} the
 * client when done with it.
 */
public interface CdcClient extends AutoCloseable {

    long bookmarkCreate(String db, String table, String holder, long ttlMs) throws SQLException;

    void bookmarkRelease(String db, String table, long bookmarkId, String holder) throws SQLException;

    List<ColumnMeta> fetchColumns(String db, String table) throws SQLException;

    List<String> fetchPrimaryKeys(String db, String table) throws SQLException;

    String fetchTableModel(String db, String table) throws SQLException;

    boolean cdcPropertyEnabled(String db, String table) throws SQLException;

    void streamSnapshot(String db, String table, List<String> cols, long bookmarkId, RowConsumer consumer)
            throws SQLException;

    void streamChanges(String db, String table, List<String> cols, long base, long head, ChangeRowConsumer consumer)
            throws SQLException, NonTrackableException;

    @Override
    void close();

    interface RowConsumer {
        void accept(Object[] row);
    }

    interface ChangeRowConsumer {
        void accept(Object[] row, int changeType, long rowVersion);
    }
}
