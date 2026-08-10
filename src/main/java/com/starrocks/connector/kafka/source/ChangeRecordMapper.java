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
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;

import java.math.BigDecimal;
import java.sql.Types;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pure function mapper that turns a raw row (as {@code Object[]} + column metadata) into a
 * Debezium envelope {@link SourceRecord}.
 *
 * <p>The envelope is built by Debezium's own {@link Envelope} rather than by hand. That library
 * owns the envelope's field set and field order -- {@code before, after, source, op, ts_ms,
 * transaction} -- and the {@code op} code each factory method stamps ({@code r}/{@code c}/{@code
 * d}). Do not go back to assembling it with {@link SchemaBuilder}/{@link Struct}: the canonical
 * order is part of the Avro schema identity that downstream consumers and schema registries
 * compare against, and a hand-rolled envelope drifts from it silently. The only pieces this class
 * still defines are the two schemas Debezium cannot know about: the row schema (which must stay
 * optional, since {@code before}/{@code after} are null on {@code c}/{@code d} respectively) and
 * the StarRocks-specific {@code source} block.
 *
 * <p>{@code transaction} is left null: StarRocks' CHANGES stream carries no transaction metadata.
 *
 * <p>Never touches JDBC/ResultSet directly so it stays trivially testable. All schemas
 * (row/key/source) and the envelope itself are built once in the constructor and reused for every
 * record produced by this instance.
 */
public final class ChangeRecordMapper {

    // changeType contract for toChangeRecord(): 0=INSERT/UPSERT, 1=DELETE. Not exposed as public
    // constants since the method signature (not this encoding detail) is what Task 5 depends on.
    private static final int CHANGE_TYPE_DELETE = 1;

    /**
     * {@code source.row_version} reported for a snapshot ({@code op="r"}) record. Snapshot rows
     * carry no version: see {@link #toSnapshotRecord} for why a bookmark id must not be used here.
     */
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
    private final List<String> pkCols;
    private final Map<String, Integer> colIndexByName;

    private final Schema rowSchema;
    private final Schema keySchema; // null when pkCols is empty
    private final Schema bookmarkSchema;
    private final Schema sourceSchema;
    private final Envelope envelope;

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
        // withRecord() defines before and after from the one row schema; build() appends op, ts_ms
        // and transaction, giving the canonical order before, after, source, op, ts_ms, transaction.
        this.envelope = Envelope.defineSchema()
                .withName(Envelope.schemaName(topic))
                .withRecord(rowSchema)
                .withSource(sourceSchema)
                .build();
    }

    /**
     * changeType: 0=INSERT/UPSERT -&gt; op "c" (after populated); 1=DELETE -&gt; op "d" (before populated).
     * sourceOffset = OffsetState.sourceOffset(headBookmark, snapshotDoneFlag).
     */
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
     * op "r", after populated; sourceOffset = OffsetState.sourceOffset(bookmarkId, false).
     *
     * <p>{@code source.row_version} is {@link #SNAPSHOT_ROW_VERSION}, not the bookmark id. A
     * change record's {@code row_version} is the partition's visible version, which starts at 1
     * and increments per publish; a bookmark id comes from the FE's global id generator and is
     * typically orders of magnitude larger. Putting a bookmark id in that field would make every
     * snapshot row look newer than every change that follows it, so a consumer deduplicating on
     * "apply only if row_version increased" would discard all of them. A snapshot row genuinely
     * has no single version to report -- the pinned read spans partitions that each sit at their
     * own version -- so it reports none. {@code source.bookmark.base}/{@code head} still carry
     * the pinning bookmark id: those fields are in the bookmark id space and are the right place
     * to identify which snapshot a row came from.
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
            // BINARY/VARBINARY must not fall through to the STRING default. Doing so was silently
            // lossy rather than loud: the read side matched by also defaulting to getString(), so
            // nothing failed -- the bytes were just decoded with the connection charset, and every
            // sequence that is not valid text became U+FFFD with no way back to the original.
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return col.nullable ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA;
            default:
                return col.nullable ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
        }
    }
}
