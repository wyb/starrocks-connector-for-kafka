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
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Types;
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
        assertEquals(7L, src.getInt64("row_version").longValue());
        Struct bookmark = (Struct) src.get("bookmark");
        assertEquals(7L, bookmark.getInt64("base").longValue());
        assertEquals(7L, bookmark.getInt64("head").longValue());
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
