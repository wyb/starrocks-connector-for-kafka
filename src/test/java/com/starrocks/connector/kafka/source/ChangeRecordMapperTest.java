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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class ChangeRecordMapperTest {

    private ChangeRecordMapper mapper() {
        List<ColumnMeta> cols = Arrays.asList(
                new ColumnMeta("k", Types.INTEGER, 10, 0, false),
                new ColumnMeta("v", Types.BIGINT, 19, 0, true));
        return new ChangeRecordMapper("db1", "orders", "sr.db1.orders",
                cols, Collections.singletonList("k"));
    }

    /**
     * The primary-key list and the column list come from different queries and can disagree, on
     * casing for instance. Unboxing the miss threw a bare NPE out of the task's start(), naming
     * neither column nor table.
     */
    @Test
    public void testPrimaryKeyColumnMissingFromColumnListIsNamed() {
        List<ColumnMeta> cols = Arrays.asList(
                new ColumnMeta("k", Types.INTEGER, 10, 0, false),
                new ColumnMeta("v", Types.BIGINT, 19, 0, true));
        try {
            new ChangeRecordMapper("db1", "orders", "sr.db1.orders", cols, Collections.singletonList("K"));
            fail("expected a ConnectException naming the unmatched primary key column");
        } catch (ConnectException expected) {
            String message = expected.getMessage();
            assertTrue("should name the column, was: " + message, message.contains("'K'"));
            assertTrue("should name the table, was: " + message, message.contains("db1.orders"));
        }
    }

    @Test
    public void testInsertRowBecomesCreateEnvelope() {
        SourceRecord r = mapper().toChangeRecord(new Object[]{1, 100L}, 0, 11955L, 11952L, 11955L, true);
        Struct value = (Struct) r.value();
        assertEquals("c", value.getString("op"));
        assertNull(value.get("before"));
        assertEquals(1, ((Struct) value.get("after")).getInt32("k").intValue());
        Struct src = (Struct) value.get("source");
        assertEquals("db1", src.getString("db"));
        assertEquals("orders", src.getString("table"));
        assertEquals(11955L, src.getInt64("row_version").longValue());
        assertEquals(11952L, ((Struct) src.get("bookmark")).getInt64("base").longValue());
        assertEquals(11955L, ((Struct) src.get("bookmark")).getInt64("head").longValue());
        assertTrue(value.getInt64("ts_ms") > 0L);
        assertEquals(1, ((Struct) r.key()).getInt32("k").intValue());
        assertEquals("sr.db1.orders", r.topic());
        assertEquals(Boolean.TRUE, r.sourceOffset().get("snapshot_done"));
        assertEquals(11955L, r.sourceOffset().get("bookmark_id"));
        assertEquals("db1", r.sourcePartition().get("db"));
        assertEquals("orders", r.sourcePartition().get("table"));
    }

    @Test
    public void testDeleteRowBecomesDeleteEnvelopeWithBeforeImage() {
        SourceRecord r = mapper().toChangeRecord(new Object[]{2, 200L}, 1, 11960L, 11958L, 11960L, false);
        Struct value = (Struct) r.value();
        assertEquals("d", value.getString("op"));
        assertEquals(2, ((Struct) value.get("before")).getInt32("k").intValue());
        assertNull(value.get("after"));
        assertEquals(Boolean.FALSE, r.sourceOffset().get("snapshot_done"));
    }

    @Test
    public void testSnapshotRowBecomesReadEnvelope() {
        SourceRecord r = mapper().toSnapshotRecord(new Object[]{3, null}, 7L);
        Struct value = (Struct) r.value();
        assertEquals("r", value.getString("op"));
        Struct after = (Struct) value.get("after");
        assertEquals(3, after.getInt32("k").intValue());
        assertNull(after.get("v"));
        assertEquals(7L, r.sourceOffset().get("bookmark_id"));
        assertEquals(Boolean.FALSE, r.sourceOffset().get("snapshot_done"));
        Struct src = (Struct) value.get("source");
        // Not the bookmark id: row_version lives in the partition-version space while bookmark
        // ids come from the FE global id generator and are far larger, so reusing one here would
        // make every snapshot row outrank the changes that follow it.
        assertEquals(0L, src.getInt64("row_version").longValue());
        Struct bookmark = (Struct) src.get("bookmark");
        assertEquals(7L, bookmark.getInt64("base").longValue());
        assertEquals(7L, bookmark.getInt64("head").longValue());
    }

    /**
     * Pins the defect directly: a snapshot row must never report a row_version that can outrank a
     * later change record's. Change versions start at 1 and climb one publish at a time; bookmark
     * ids come from the FE global id generator -- a real report had bookmarks 11952/11955 against
     * partition version 3.
     */
    @Test
    public void testSnapshotRowVersionCannotOutrankLaterChanges() {
        ChangeRecordMapper m = mapper();
        SourceRecord snapshot = m.toSnapshotRecord(new Object[]{1, 10L}, 11952L);
        SourceRecord change = m.toChangeRecord(new Object[]{1, 20L}, 0, 4L, 11952L, 11955L, true);
        long snapshotVersion = ((Struct) ((Struct) snapshot.value()).get("source")).getInt64("row_version");
        long changeVersion = ((Struct) ((Struct) change.value()).get("source")).getInt64("row_version");
        assertTrue("snapshot row_version " + snapshotVersion + " must not outrank change row_version "
                + changeVersion, snapshotVersion < changeVersion);
    }

    /**
     * The envelope is Debezium's, so its field order is Debezium's canonical order -- not the
     * op-first order this connector once hand-rolled. Downstream Avro/Protobuf schema identity is
     * computed from that order, so a reordering is a compatibility break, not a cosmetic change.
     */
    @Test
    public void testEnvelopeFieldOrderMatchesDebezium() {
        SourceRecord r = mapper().toChangeRecord(new Object[]{1, 100L}, 0, 11955L, 11952L, 11955L, true);
        List<String> names = new ArrayList<>();
        for (Field f : ((Struct) r.value()).schema().fields()) {
            names.add(f.name());
        }
        assertEquals(Arrays.asList("before", "after", "source", "op", "ts_ms", "transaction"), names);
        assertEquals("sr.db1.orders.Envelope", ((Struct) r.value()).schema().name());
    }

    /**
     * StarRocks' CHANGES stream carries no transaction metadata, so the field Debezium's builder
     * always appends stays present in the schema (consumers can rely on it) and null in the value.
     */
    @Test
    public void testTransactionFieldPresentAndNull() {
        ChangeRecordMapper m = mapper();
        for (SourceRecord r : Arrays.asList(
                m.toChangeRecord(new Object[]{1, 100L}, 0, 11955L, 11952L, 11955L, true),
                m.toChangeRecord(new Object[]{2, 200L}, 1, 11960L, 11958L, 11960L, false),
                m.toSnapshotRecord(new Object[]{3, 300L}, 7L))) {
            Struct value = (Struct) r.value();
            Field transaction = value.schema().field("transaction");
            assertNotNull("envelope schema must carry a transaction field", transaction);
            assertTrue("transaction must be optional so it can stay unset", transaction.schema().isOptional());
            assertNull(value.get("transaction"));
        }
    }

    @Test
    public void testNoPkTableHasNullKey() {
        List<ColumnMeta> cols = Arrays.asList(
                new ColumnMeta("k", Types.INTEGER, 10, 0, false),
                new ColumnMeta("v", Types.BIGINT, 19, 0, true));
        ChangeRecordMapper mapper = new ChangeRecordMapper("db1", "orders", "sr.db1.orders",
                cols, Collections.emptyList());

        SourceRecord r = mapper.toChangeRecord(new Object[]{1, 100L}, 0, 1L, 1L, 1L, false);
        assertNull(r.key());
        assertNull(r.keySchema());
    }

    @Test
    public void testDecimalDateTimestampMapping() {
        List<ColumnMeta> cols = Arrays.asList(
                new ColumnMeta("d", Types.DECIMAL, 10, 2, true),
                new ColumnMeta("dt", Types.DATE, 0, 0, true),
                new ColumnMeta("ts", Types.TIMESTAMP, 0, 0, true),
                new ColumnMeta("j", Types.OTHER, 0, 0, true));
        ChangeRecordMapper mapper = new ChangeRecordMapper("db1", "misc", "sr.db1.misc",
                cols, Collections.emptyList());

        SourceRecord r = mapper.toSnapshotRecord(
                new Object[]{new BigDecimal("1.50"), new java.util.Date(0), new java.util.Date(0), "{}"}, 1L);

        Struct value = (Struct) r.value();
        Struct after = (Struct) value.get("after");
        Schema rowSchema = after.schema();

        Schema decimalSchema = rowSchema.field("d").schema();
        assertEquals(Decimal.LOGICAL_NAME, decimalSchema.name());
        assertEquals("2", decimalSchema.parameters().get(Decimal.SCALE_FIELD));
        assertTrue(decimalSchema.isOptional());

        Schema dateSchema = rowSchema.field("dt").schema();
        assertEquals(Date.LOGICAL_NAME, dateSchema.name());
        assertTrue(dateSchema.isOptional());

        Schema tsSchema = rowSchema.field("ts").schema();
        assertEquals(Timestamp.LOGICAL_NAME, tsSchema.name());
        assertTrue(tsSchema.isOptional());

        Schema jSchema = rowSchema.field("j").schema();
        assertEquals(Schema.Type.STRING, jSchema.type());
        assertTrue(jSchema.isOptional());
    }

    /**
     * BINARY/VARBINARY must reach Kafka as BYTES, carrying the exact bytes read.
     *
     * <p>These used to fall through to the STRING default on both sides at once -- schema and read
     * -- so nothing ever threw and the corruption was invisible: bytes went through
     * {@code getString()}, got decoded with the connection charset, and anything that was not valid
     * text came out as U+FFFD. The round trip below is what makes that regression loud.
     */
    @Test
    public void testBinaryColumnsCarryRawBytesNotText() {
        List<ColumnMeta> cols = Arrays.asList(
                new ColumnMeta("b", Types.BINARY, 4, 0, false),
                new ColumnMeta("vb", Types.VARBINARY, 16, 0, true));
        ChangeRecordMapper mapper = new ChangeRecordMapper("db1", "blobs", "sr.db1.blobs",
                cols, Collections.emptyList());

        // 0xFF 0xFE is not valid UTF-8; decoding it as text is exactly the lossy path being guarded.
        byte[] fixed = new byte[]{(byte) 0xFF, (byte) 0xFE, 0x00, 0x41};
        byte[] variable = new byte[]{(byte) 0xC3, 0x28};

        SourceRecord r = mapper.toSnapshotRecord(new Object[]{fixed, variable}, 1L);
        Struct after = (Struct) ((Struct) r.value()).get("after");
        Schema rowSchema = after.schema();

        assertEquals(Schema.Type.BYTES, rowSchema.field("b").schema().type());
        assertFalse(rowSchema.field("b").schema().isOptional());
        assertEquals(Schema.Type.BYTES, rowSchema.field("vb").schema().type());
        assertTrue(rowSchema.field("vb").schema().isOptional());

        assertArrayEquals(fixed, (byte[]) after.get("b"));
        assertArrayEquals(variable, (byte[]) after.get("vb"));
    }

    /**
     * JSON and the complex types are still carried as text, but a consumer should be able to tell
     * them from an ordinary string without knowing the source table, so their schemas get a logical
     * name. JSON uses Debezium's own, which downstream SMTs and sinks already recognise; ARRAY, MAP
     * and STRUCT get StarRocks-specific names, deliberately not io.debezium.data.Json -- that would
     * promise every value parses as JSON, which has not been verified for NULLs, embedded quotes or
     * nesting.
     */
    @Test
    public void testStructuredTextColumnsCarryALogicalTypeName() {
        List<ColumnMeta> cols = Arrays.asList(
                new ColumnMeta("j", Types.OTHER, 0, 0, true, "json", "json"),
                new ColumnMeta("a", Types.OTHER, 0, 0, true, "array", "array<int>"),
                new ColumnMeta("m", Types.OTHER, 0, 0, true, "map", "map<varchar(10),int>"),
                new ColumnMeta("s", Types.OTHER, 0, 0, false, "struct", "struct<x int>"),
                new ColumnMeta("v", Types.VARCHAR, 20, 0, true, "varchar", "varchar(20)"));
        ChangeRecordMapper mapper = new ChangeRecordMapper("db1", "cx", "sr.db1.cx",
                cols, Collections.emptyList());

        SourceRecord r = mapper.toSnapshotRecord(
                new Object[]{"{\"k\":1}", "[1,2]", "{\"a\":1}", "{\"x\":7}", "plain"}, 1L);
        Schema rowSchema = ((Struct) ((Struct) r.value()).get("after")).schema();

        assertEquals("io.debezium.data.Json", rowSchema.field("j").schema().name());
        assertEquals("com.starrocks.data.Array", rowSchema.field("a").schema().name());
        assertEquals("com.starrocks.data.Map", rowSchema.field("m").schema().name());
        assertEquals("com.starrocks.data.Struct", rowSchema.field("s").schema().name());
        // A plain string must stay unnamed -- naming everything would make the marker meaningless.
        assertNull(rowSchema.field("v").schema().name());

        // Still STRING underneath, and nullability still comes from the column, not the name.
        assertEquals(Schema.Type.STRING, rowSchema.field("a").schema().type());
        assertTrue(rowSchema.field("a").schema().isOptional());
        assertFalse(rowSchema.field("s").schema().isOptional());
    }

    /** Columns described without the server's view (no srDataType) must still work, unnamed. */
    @Test
    public void testColumnsWithoutStarRocksTypeFallBackToPlainString() {
        List<ColumnMeta> cols = Collections.singletonList(new ColumnMeta("t", Types.OTHER, 0, 0, true));
        ChangeRecordMapper mapper = new ChangeRecordMapper("db1", "cx", "sr.db1.cx",
                cols, Collections.emptyList());

        SourceRecord r = mapper.toSnapshotRecord(new Object[]{"x"}, 1L);
        Schema rowSchema = ((Struct) ((Struct) r.value()).get("after")).schema();
        assertEquals(Schema.Type.STRING, rowSchema.field("t").schema().type());
        assertNull(rowSchema.field("t").schema().name());
    }

    @Test
    public void testTombstoneSharesKeyAndOffset() {
        ChangeRecordMapper m = mapper();
        SourceRecord deleteRecord = m.toChangeRecord(new Object[]{2, 200L}, 1, 11960L, 11958L, 11960L, false);
        SourceRecord tombstone = m.tombstoneFor(deleteRecord);

        assertNull(tombstone.value());
        assertNull(tombstone.valueSchema());
        assertEquals(deleteRecord.key(), tombstone.key());
        assertEquals(deleteRecord.keySchema(), tombstone.keySchema());
        assertEquals(deleteRecord.topic(), tombstone.topic());
        assertEquals(deleteRecord.sourcePartition(), tombstone.sourcePartition());
        assertEquals(deleteRecord.sourceOffset(), tombstone.sourceOffset());
    }
}
