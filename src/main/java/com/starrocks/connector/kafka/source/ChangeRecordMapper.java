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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Maps raw CDC rows to Debezium SourceRecords without JDBC access.
 * Debezium's Envelope preserves the outer field order expected by Avro schema registries. The
 * row and source schemas and field bindings are built once per table. Input row containers
 * transfer to the returned record and must not be modified or reused by the caller.
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
    /** Positions in {@code cols} of the columns flagged as key, in declaration order. */
    private final List<Integer> keyIndexes;

    private final Schema rowSchema;
    private final Schema keySchema; // null when the table has no key column
    private final Schema bookmarkSchema;
    private final Schema sourceSchema;
    private final Envelope envelope;
    private final Map<String, String> sourcePartition;
    private final FieldBinding[] rowBindings;
    private final FieldBinding[] keyBindings;
    private final Field bookmarkBaseField;
    private final Field bookmarkHeadField;
    private final Field sourceDbField;
    private final Field sourceTableField;
    private final Field sourceRowVersionField;
    private final Field sourceBookmarkField;

    public ChangeRecordMapper(String db, String table, String topic, List<ColumnMeta> cols) {
        this.db = db;
        this.table = table;
        this.topic = topic;
        this.cols = Collections.unmodifiableList(new ArrayList<>(cols));
        this.sourcePartition = Collections.unmodifiableMap(OffsetState.sourcePartition(db, table));

        this.keyIndexes = new ArrayList<>();
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).key) {
                keyIndexes.add(i);
            }
        }

        this.rowSchema = buildRowSchema();
        this.keySchema = keyIndexes.isEmpty() ? null : buildKeySchema();
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
        this.bookmarkBaseField = bookmarkSchema.field(FIELD_BASE);
        this.bookmarkHeadField = bookmarkSchema.field(FIELD_HEAD);
        this.sourceDbField = sourceSchema.field(FIELD_DB);
        this.sourceTableField = sourceSchema.field(FIELD_TABLE);
        this.sourceRowVersionField = sourceSchema.field(FIELD_ROW_VERSION);
        this.sourceBookmarkField = sourceSchema.field(FIELD_BOOKMARK);
        this.rowBindings = new FieldBinding[cols.size()];
        for (int i = 0; i < cols.size(); i++) {
            rowBindings[i] = new FieldBinding(i, rowSchema.field(cols.get(i).name), cols.get(i).type);
        }
        this.keyBindings = new FieldBinding[keyIndexes.size()];
        for (int i = 0; i < keyIndexes.size(); i++) {
            int columnIndex = keyIndexes.get(i);
            ColumnMeta col = cols.get(columnIndex);
            keyBindings[i] = new FieldBinding(columnIndex, keySchema.field(col.name), col.type);
        }
        this.envelope = Envelope.defineSchema()
                .withName(Envelope.schemaName(topic))
                .withRecord(rowSchema)
                .withSource(sourceSchema)
                .build();
    }

    /**
     * 0=INSERT/UPSERT -&gt; op "c"; 1=DELETE -&gt; op "d" (before populated). The offset names
     * {@code head} with the snapshot done: a change record only ever follows one.
     */
    public SourceRecord toChangeRecord(Object[] row, int changeType, long rowVersion, long baseBookmark, long headBookmark) {
        Struct rowStruct = toRowStruct(row);
        Struct source = buildSource(rowVersion, baseBookmark, headBookmark);
        Instant ts = Instant.ofEpochMilli(System.currentTimeMillis());

        // delete() puts its record argument in before; create() puts it in after.
        Struct value = changeType == CHANGE_TYPE_DELETE
                ? envelope.delete(rowStruct, source, ts)
                : envelope.create(rowStruct, source, ts);

        Map<String, Object> sourceOffset = OffsetState.sourceOffset(headBookmark, true);
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

        Map<String, Object> sourceOffset = OffsetState.sourceOffset(bookmarkId, false);
        Struct key = toKeyStruct(row);

        return new SourceRecord(sourcePartition, sourceOffset, topic, keySchema, key, envelope.schema(), value);
    }

    public SourceRecord tombstoneFor(SourceRecord deleteRecord) {
        return new SourceRecord(deleteRecord.sourcePartition(), deleteRecord.sourceOffset(), deleteRecord.topic(),
                deleteRecord.kafkaPartition(), deleteRecord.keySchema(), deleteRecord.key(), null, null,
                deleteRecord.timestamp(), deleteRecord.headers());
    }

    private Struct buildSource(long rowVersion, long baseBookmark, long headBookmark) {
        Struct bookmark = new Struct(bookmarkSchema)
                .put(bookmarkBaseField, baseBookmark)
                .put(bookmarkHeadField, headBookmark);
        return new Struct(sourceSchema)
                .put(sourceDbField, db)
                .put(sourceTableField, table)
                .put(sourceRowVersionField, rowVersion)
                .put(sourceBookmarkField, bookmark);
    }

    private Struct toRowStruct(Object[] row) {
        Struct struct = new Struct(rowSchema);
        for (FieldBinding binding : rowBindings) {
            binding.put(struct, row);
        }
        return struct;
    }

    private Struct toKeyStruct(Object[] row) {
        if (keySchema == null) {
            return null;
        }
        Struct key = new Struct(keySchema);
        for (FieldBinding binding : keyBindings) {
            binding.put(key, row);
        }
        return key;
    }

    /** Fields and nested converters belong to the exact row/key schema they were bound against. */
    private static final class FieldBinding {
        private final int columnIndex;
        private final Field field;
        private final ColumnType.ConnectValueConverter converter;

        FieldBinding(int columnIndex, Field field, ColumnType type) {
            this.columnIndex = columnIndex;
            this.field = field;
            this.converter = type.bindConnectValue(field.schema());
        }

        void put(Struct target, Object[] row) {
            Object value = row[columnIndex];
            if (value != null) {
                target.put(field, converter.convert(value));
            }
        }
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
        for (int idx : keyIndexes) {
            ColumnMeta col = cols.get(idx);
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
