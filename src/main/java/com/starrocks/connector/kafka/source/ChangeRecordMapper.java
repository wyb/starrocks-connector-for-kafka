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

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns a raw row plus column metadata into a Debezium envelope {@link SourceRecord}. Pure: never
 * touches JDBC, and every schema is built once in the constructor.
 *
 * <p>The envelope comes from Debezium's own {@link Envelope}, which owns the field order and the
 * {@code op} codes. <b>Do not reassemble it by hand</b>: that order is part of the Avro schema
 * identity registries compare against. Only the row schema and the {@code source} block are ours.
 *
 * <p>{@code transaction} stays null; the CHANGES stream carries no transaction metadata.
 */
public final class ChangeRecordMapper {

    /** StarRocks emits 1 for a row's *before* value: a real deletion, or the first half of an update. */
    static final int CHANGE_TYPE_DELETE = 1;

    /** Snapshot rows carry no version; see {@link #toSnapshotRecord} for why not a bookmark id. */
    private static final long SNAPSHOT_ROW_VERSION = 0L;

    private static final String FIELD_DB = "db";
    private static final String FIELD_TABLE = "table";
    private static final String FIELD_ROW_VERSION = "row_version";
    private static final String FIELD_BOOKMARK = "bookmark";
    private static final String FIELD_BASE = "base";
    private static final String FIELD_HEAD = "head";

    private final String db;
    private final String table;
    private final String topic;
    private final List<ColumnMeta> cols;
    private final List<String> keyCols;
    private final Map<String, Integer> colIndexByName;

    private final Schema rowSchema;
    private final Schema keySchema; // null when keyCols is empty
    private final Schema bookmarkSchema;
    private final Schema sourceSchema;
    private final Envelope envelope;

    public ChangeRecordMapper(String db, String table, String topic,
                              List<ColumnMeta> cols, List<String> keyCols) {
        this.db = db;
        this.table = table;
        this.topic = topic;
        this.cols = cols;
        this.keyCols = keyCols;

        this.colIndexByName = new HashMap<>();
        for (int i = 0; i < cols.size(); i++) {
            colIndexByName.put(cols.get(i).name, i);
        }

        this.rowSchema = buildRowSchema();
        this.keySchema = keyCols.isEmpty() ? null : buildKeySchema();
        this.bookmarkSchema = SchemaBuilder.struct()
                .name(topic + ".Bookmark")
                .field(FIELD_BASE, Schema.INT64_SCHEMA)
                .field(FIELD_HEAD, Schema.INT64_SCHEMA)
                .build();
        this.sourceSchema = SchemaBuilder.struct()
                .name(topic + ".Source")
                .field(FIELD_DB, Schema.STRING_SCHEMA)
                .field(FIELD_TABLE, Schema.STRING_SCHEMA)
                .field(FIELD_ROW_VERSION, Schema.INT64_SCHEMA)
                .field(FIELD_BOOKMARK, bookmarkSchema)
                .build();
        this.envelope = Envelope.defineSchema()
                .withName(Envelope.schemaName(topic))
                .withRecord(rowSchema)
                .withSource(sourceSchema)
                .build();
    }

    /** 0=INSERT/UPSERT -&gt; op "c"; 1=DELETE -&gt; op "d" (before populated). */
    public SourceRecord toChangeRecord(Object[] row, int changeType, long rowVersion,
                                       long baseBookmark, long headBookmark, boolean snapshotDoneFlag) {
        Struct rowStruct = toRowStruct(row);
        Struct source = buildSource(rowVersion, baseBookmark, headBookmark);
        Instant ts = Instant.ofEpochMilli(System.currentTimeMillis());

        // delete() puts its record argument in before; create() puts it in after.
        Struct value = changeType == CHANGE_TYPE_DELETE
                ? envelope.delete(rowStruct, source, ts)
                : envelope.create(rowStruct, source, ts);

        Map<String, String> sourcePartition = OffsetState.sourcePartition(db, table);
        Map<String, Object> sourceOffset = OffsetState.sourceOffset(headBookmark, snapshotDoneFlag);
        Struct key = toKeyStruct(row);

        return new SourceRecord(sourcePartition, sourceOffset, topic, keySchema, key, envelope.schema(), value);
    }

    /**
     * op "r", with {@code row_version} = {@link #SNAPSHOT_ROW_VERSION} rather than the bookmark id.
     *
     * <p>A bookmark id comes from FE's global id generator and dwarfs a partition's visible version,
     * so using it here would make every snapshot row look newer than every change that follows. A
     * pinned read spans partitions each at their own version, so there is no one version to report;
     * {@code source.bookmark} still identifies the snapshot.
     */
    public SourceRecord toSnapshotRecord(Object[] row, long bookmarkId) {
        Struct after = toRowStruct(row);
        Struct source = buildSource(SNAPSHOT_ROW_VERSION, bookmarkId, bookmarkId);
        Struct value = envelope.read(after, source, Instant.ofEpochMilli(System.currentTimeMillis()));

        Map<String, String> sourcePartition = OffsetState.sourcePartition(db, table);
        Map<String, Object> sourceOffset = OffsetState.sourceOffset(bookmarkId, false);
        Struct key = toKeyStruct(row);

        return new SourceRecord(sourcePartition, sourceOffset, topic, keySchema, key, envelope.schema(), value);
    }

    public SourceRecord tombstoneFor(SourceRecord deleteRecord) {
        return new SourceRecord(
                deleteRecord.sourcePartition(),
                deleteRecord.sourceOffset(),
                deleteRecord.topic(),
                deleteRecord.keySchema(),
                deleteRecord.key(),
                null,
                null);
    }

    private Struct buildSource(long rowVersion, long baseBookmark, long headBookmark) {
        Struct bookmark = new Struct(bookmarkSchema)
                .put(FIELD_BASE, baseBookmark)
                .put(FIELD_HEAD, headBookmark);
        return new Struct(sourceSchema)
                .put(FIELD_DB, db)
                .put(FIELD_TABLE, table)
                .put(FIELD_ROW_VERSION, rowVersion)
                .put(FIELD_BOOKMARK, bookmark);
    }

    private Struct toRowStruct(Object[] row) {
        Struct struct = new Struct(rowSchema);
        for (int i = 0; i < cols.size(); i++) {
            putValue(struct, cols.get(i), row[i]);
        }
        return struct;
    }

    private Struct toKeyStruct(Object[] row) {
        if (keySchema == null) {
            return null;
        }
        Struct key = new Struct(keySchema);
        for (String keyCol : keyCols) {
            int idx = indexOf(keyCol);
            putValue(key, cols.get(idx), row[idx]);
        }
        return key;
    }

    /**
     * The key list and the column list come from two queries against
     * {@code information_schema.columns} and can still disagree if the table is altered between
     * them. Unboxing a miss would leave {@code start()} throwing a bare NPE that names neither
     * column nor table.
     */
    private int indexOf(String keyCol) {
        Integer idx = colIndexByName.get(keyCol);
        if (idx == null) {
            throw new ConnectException("key column '" + keyCol + "' of " + db + "." + table
                    + " is not among the captured columns " + colIndexByName.keySet());
        }
        return idx;
    }

    /**
     * The row is already canonical ({@link ValueReader}): decimals at the declared scale, temporals
     * as text. Only a nested column still needs assembling into Connect's own containers.
     */
    private static void putValue(Struct struct, ColumnMeta col, Object value) {
        if (value == null) {
            return;
        }
        if (col.type.isNested()) {
            struct.put(col.name, col.type.toConnectValue(struct.schema().field(col.name).schema(), value));
            return;
        }
        struct.put(col.name, value);
    }

    private Schema buildRowSchema() {
        SchemaBuilder builder = SchemaBuilder.struct().name(topic + ".Value").optional();
        for (ColumnMeta col : cols) {
            builder.field(col.name, schemaFor(col));
        }
        return builder.build();
    }

    private Schema buildKeySchema() {
        SchemaBuilder builder = SchemaBuilder.struct().name(topic + ".Key");
        for (String keyCol : keyCols) {
            ColumnMeta col = cols.get(indexOf(keyCol));
            builder.field(col.name, schemaFor(col));
        }
        return builder.build();
    }

    /**
     * Every column's schema comes from its {@link ColumnType}: the same tree that drives how the
     * value is read, so the two cannot drift apart. Only the column itself takes the declared
     * nullability; nested elements are always optional. The name seeds nested struct names.
     */
    private Schema schemaFor(ColumnMeta col) {
        return col.type.toConnectSchema(topic + "." + col.name, col.nullable);
    }
}
