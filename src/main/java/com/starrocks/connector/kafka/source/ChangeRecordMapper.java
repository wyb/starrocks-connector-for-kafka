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

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pure function mapper that turns a raw row (as {@code Object[]} + column metadata) into a
 * Debezium-style envelope {@link SourceRecord}: {@code {op, before, after, source, ts_ms}}.
 *
 * <p>Never touches JDBC/ResultSet directly so it stays trivially testable. All schemas
 * (row/key/envelope) are built once in the constructor and reused for every record produced
 * by this instance.
 */
public final class ChangeRecordMapper {

    // changeType contract for toChangeRecord(): 0=INSERT/UPSERT, 1=DELETE. Not exposed as public
    // constants since the method signature (not this encoding detail) is what Task 5 depends on.
    private static final int CHANGE_TYPE_DELETE = 1;

    private static final String OP_CREATE = "c";
    private static final String OP_DELETE = "d";
    private static final String OP_READ = "r";

    private static final String FIELD_OP = "op";
    private static final String FIELD_BEFORE = "before";
    private static final String FIELD_AFTER = "after";
    private static final String FIELD_SOURCE = "source";
    private static final String FIELD_TS_MS = "ts_ms";

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
    private final List<String> pkCols;
    private final Map<String, Integer> colIndexByName;

    private final Schema rowSchema;
    private final Schema keySchema; // null when pkCols is empty
    private final Schema bookmarkSchema;
    private final Schema sourceSchema;
    private final Schema envelopeSchema;

    public ChangeRecordMapper(String db, String table, String topic,
                               List<ColumnMeta> cols, List<String> pkCols) {
        this.db = db;
        this.table = table;
        this.topic = topic;
        this.cols = cols;
        this.pkCols = pkCols;

        this.colIndexByName = new LinkedHashMap<>();
        for (int i = 0; i < cols.size(); i++) {
            colIndexByName.put(cols.get(i).name, i);
        }

        this.rowSchema = buildRowSchema();
        this.keySchema = pkCols.isEmpty() ? null : buildKeySchema();
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
        this.envelopeSchema = SchemaBuilder.struct()
                .name(topic + ".Envelope")
                .field(FIELD_OP, Schema.STRING_SCHEMA)
                .field(FIELD_BEFORE, rowSchema)
                .field(FIELD_AFTER, rowSchema)
                .field(FIELD_SOURCE, sourceSchema)
                .field(FIELD_TS_MS, Schema.INT64_SCHEMA)
                .build();
    }

    /**
     * changeType: 0=INSERT/UPSERT -&gt; op "c" (after populated); 1=DELETE -&gt; op "d" (before populated).
     * sourceOffset = OffsetState.sourceOffset(headBookmark, snapshotDoneFlag).
     */
    public SourceRecord toChangeRecord(Object[] row, int changeType, long rowVersion,
                                        long baseBookmark, long headBookmark, boolean snapshotDoneFlag) {
        Struct rowStruct = toRowStruct(row);
        boolean isDelete = changeType == CHANGE_TYPE_DELETE;
        String op = isDelete ? OP_DELETE : OP_CREATE;
        Struct before = isDelete ? rowStruct : null;
        Struct after = isDelete ? null : rowStruct;

        Struct envelope = buildEnvelope(op, before, after, rowVersion, baseBookmark, headBookmark);

        Map<String, String> sourcePartition = OffsetState.sourcePartition(db, table);
        Map<String, Object> sourceOffset = OffsetState.sourceOffset(headBookmark, snapshotDoneFlag);
        Struct key = toKeyStruct(row);

        return new SourceRecord(sourcePartition, sourceOffset, topic, keySchema, key, envelopeSchema, envelope);
    }

    /**
     * op "r", after populated; sourceOffset = OffsetState.sourceOffset(bookmarkId, false).
     */
    public SourceRecord toSnapshotRecord(Object[] row, long bookmarkId) {
        Struct after = toRowStruct(row);
        Struct envelope = buildEnvelope(OP_READ, null, after, bookmarkId, bookmarkId, bookmarkId);

        Map<String, String> sourcePartition = OffsetState.sourcePartition(db, table);
        Map<String, Object> sourceOffset = OffsetState.sourceOffset(bookmarkId, false);
        Struct key = toKeyStruct(row);

        return new SourceRecord(sourcePartition, sourceOffset, topic, keySchema, key, envelopeSchema, envelope);
    }

    /**
     * Tombstone sharing key/topic/partition/offset with {@code deleteRecord}: value and valueSchema are both null.
     */
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

    private Struct buildEnvelope(String op, Struct before, Struct after,
                                  long rowVersion, long baseBookmark, long headBookmark) {
        Struct bookmark = new Struct(bookmarkSchema)
                .put(FIELD_BASE, baseBookmark)
                .put(FIELD_HEAD, headBookmark);
        Struct source = new Struct(sourceSchema)
                .put(FIELD_DB, db)
                .put(FIELD_TABLE, table)
                .put(FIELD_ROW_VERSION, rowVersion)
                .put(FIELD_BOOKMARK, bookmark);

        Struct envelope = new Struct(envelopeSchema)
                .put(FIELD_OP, op)
                .put(FIELD_SOURCE, source)
                .put(FIELD_TS_MS, System.currentTimeMillis());
        if (before != null) {
            envelope.put(FIELD_BEFORE, before);
        }
        if (after != null) {
            envelope.put(FIELD_AFTER, after);
        }
        return envelope;
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
        for (String pkCol : pkCols) {
            int idx = colIndexByName.get(pkCol);
            putValue(key, cols.get(idx), row[idx]);
        }
        return key;
    }

    private static void putValue(Struct struct, ColumnMeta col, Object value) {
        if (value == null) {
            return;
        }
        if (isDecimalType(col.jdbcType) && value instanceof BigDecimal) {
            struct.put(col.name, ((BigDecimal) value).setScale(col.scale));
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
        for (String pkCol : pkCols) {
            int idx = colIndexByName.get(pkCol);
            ColumnMeta col = cols.get(idx);
            builder.field(col.name, schemaFor(col));
        }
        return builder.build();
    }

    private static boolean isDecimalType(int jdbcType) {
        return jdbcType == Types.DECIMAL || jdbcType == Types.NUMERIC;
    }

    private static Schema schemaFor(ColumnMeta col) {
        switch (col.jdbcType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return col.nullable ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA;
            case Types.TINYINT:
                return col.nullable ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA;
            case Types.SMALLINT:
                return col.nullable ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA;
            case Types.INTEGER:
                return col.nullable ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA;
            case Types.BIGINT:
                return col.nullable ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
            case Types.REAL:
            case Types.FLOAT:
                return col.nullable ? Schema.OPTIONAL_FLOAT32_SCHEMA : Schema.FLOAT32_SCHEMA;
            case Types.DOUBLE:
                return col.nullable ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA;
            case Types.DECIMAL:
            case Types.NUMERIC:
                SchemaBuilder decimalBuilder = Decimal.builder(col.scale);
                return col.nullable ? decimalBuilder.optional().build() : decimalBuilder.build();
            case Types.DATE:
                return col.nullable ? Date.builder().optional().build() : Date.SCHEMA;
            case Types.TIMESTAMP:
                return col.nullable ? Timestamp.builder().optional().build() : Timestamp.SCHEMA;
            default:
                return col.nullable ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
        }
    }
}
