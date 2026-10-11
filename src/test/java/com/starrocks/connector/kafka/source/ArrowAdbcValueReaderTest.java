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

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ArrowAdbcValueReaderTest {
    @Test
    public void scalarVectorsKeepCanonicalTypesAndTemporalPrecision() {
        try (RootAllocator allocator = new RootAllocator();
                BitVector flag = new BitVector("flag", allocator);
                DateDayVector date = new DateDayVector("date", allocator);
                TimeStampMicroVector datetime = new TimeStampMicroVector("datetime", allocator);
                DecimalVector decimal = new DecimalVector("decimal", allocator, 18, 2);
                VarBinaryVector binary = new VarBinaryVector("binary", allocator)) {
            flag.allocateNew();
            flag.setSafe(0, 1);
            date.allocateNew();
            date.setSafe(0, 20670);
            datetime.allocateNew();
            datetime.setSafe(0, 1785933296123456L);
            decimal.allocateNew();
            decimal.setSafe(0, new BigDecimal("1.50"));
            binary.allocateNew();
            binary.setSafe(0, new byte[] {1, 2, (byte) 0xff});

            ArrowAdbcValueReader reader = new ArrowAdbcValueReader();
            assertEquals(true, reader.readValue(flag, 0, ColumnType.scalar(ColumnType.Kind.BOOLEAN)));
            assertEquals("2026-08-05", reader.readValue(date, 0, ColumnType.scalar(ColumnType.Kind.DATE)));
            assertEquals("2026-08-05 12:34:56.123456",
                    reader.readValue(datetime, 0, ColumnType.scalar(ColumnType.Kind.DATETIME)));
            assertEquals(new BigDecimal("1.50"), reader.readValue(decimal, 0, ColumnType.decimal(2)));
            assertArrayEquals(new byte[] {1, 2, (byte) 0xff},
                    (byte[]) reader.readValue(binary, 0, ColumnType.scalar(ColumnType.Kind.BYTES)));
            try {
                reader.readValue(binary, 0, ColumnType.opaque(null));
                fail("non-text OPAQUE vector should not be stringified");
            } catch (DataException expected) {
                assertTrue(expected.getMessage().contains("opaque"));
            }
        }
    }

    @Test
    public void textValuesMatchArrowObjectsIncludingUtf8EmptyAndNull() {
        try (RootAllocator allocator = new RootAllocator();
                VarCharVector vector = new VarCharVector("s", allocator)) {
            vector.allocateNew();
            vector.setSafe(0, "héllo 星".getBytes(StandardCharsets.UTF_8));
            vector.setSafe(1, "{\"k\":\"值\"}".getBytes(StandardCharsets.UTF_8));
            vector.setSafe(2, new byte[0]);
            vector.setNull(3);
            vector.setValueCount(4);

            ArrowAdbcValueReader reader = new ArrowAdbcValueReader();
            assertEquals(vector.getObject(0).toString(), reader.readValue(vector, 0,
                    ColumnType.scalar(ColumnType.Kind.STRING)));
            assertEquals(vector.getObject(1).toString(), reader.readValue(vector, 1,
                    ColumnType.scalar(ColumnType.Kind.JSON)));
            assertEquals("", reader.readValue(vector, 2,
                    ColumnType.scalar(ColumnType.Kind.OPAQUE)));
            assertNull(reader.readValue(vector, 3,
                    ColumnType.scalar(ColumnType.Kind.STRING)));
        }
    }

    private static Field mapField(Field value) {
        Field key = new Field(MapVector.KEY_NAME, FieldType.notNullable(new ArrowType.Int(32, true)), null);
        Field entries = new Field(MapVector.DATA_VECTOR_NAME, FieldType.notNullable(new ArrowType.Struct()),
                Arrays.asList(key, value));
        return new Field("m", FieldType.nullable(new ArrowType.Map(false)), Collections.singletonList(entries));
    }

    @Test
    public void directMapMatchesNestedReaderForValuesEmptyAndNull() {
        Field value = new Field(MapVector.VALUE_NAME, FieldType.nullable(new ArrowType.Int(64, true)), null);
        ColumnType type = ColumnType.map(ColumnType.scalar(ColumnType.Kind.INT),
                ColumnType.scalar(ColumnType.Kind.BIGINT));
        ArrowValueReader reader = new ArrowValueReader();
        ArrowAdbcValueReader adbc = new ArrowAdbcValueReader();
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(Collections.singletonList(mapField(value))),
                        allocator)) {
            MapVector vector = (MapVector) root.getVector("m");
            StructVector entries = (StructVector) vector.getDataVector();
            IntVector keys = (IntVector) entries.getChild(MapVector.KEY_NAME);
            BigIntVector values = (BigIntVector) entries.getChild(MapVector.VALUE_NAME);
            int start = vector.startNewValue(0);
            keys.setSafe(start, 1);
            values.setSafe(start, 100);
            entries.setIndexDefined(start);
            keys.setSafe(start + 1, 2);
            entries.setIndexDefined(start + 1);
            vector.endValue(0, 2);
            vector.startNewValue(1);
            vector.endValue(1, 0);
            vector.setNull(2);
            root.setRowCount(3);

            Map<String, Object> expected = new LinkedHashMap<>();
            expected.put("1", 100L);
            expected.put("2", null);
            assertEquals(expected, adbc.readMap(vector, 0, type));
            assertEquals(reader.readNested(type, vector.getObject(0)),
                    adbc.readMap(vector, 0, type));
            assertEquals(Collections.emptyMap(), adbc.readMap(vector, 1, type));
            assertTrue(vector.isNull(2));
        }
    }

    @Test
    public void directMapPreservesNestedDateValues() {
        Field day = new Field(ListVector.DATA_VECTOR_NAME,
                FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null);
        Field value = new Field(MapVector.VALUE_NAME, FieldType.nullable(new ArrowType.List()),
                Collections.singletonList(day));
        ColumnType type = ColumnType.map(ColumnType.scalar(ColumnType.Kind.INT),
                ColumnType.array(ColumnType.scalar(ColumnType.Kind.DATE)));
        ArrowValueReader reader = new ArrowValueReader();
        ArrowAdbcValueReader adbc = new ArrowAdbcValueReader();
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(Collections.singletonList(mapField(value))),
                        allocator)) {
            MapVector vector = (MapVector) root.getVector("m");
            StructVector entries = (StructVector) vector.getDataVector();
            IntVector keys = (IntVector) entries.getChild(MapVector.KEY_NAME);
            ListVector values = (ListVector) entries.getChild(MapVector.VALUE_NAME);
            int start = vector.startNewValue(0);
            keys.setSafe(start, 1);
            int firstDay = values.startNewValue(start);
            DateDayVector days = (DateDayVector) values.getDataVector();
            days.setSafe(firstDay, 20670);
            days.setSafe(firstDay + 1, 20671);
            values.endValue(start, 2);
            entries.setIndexDefined(start);
            keys.setSafe(start + 1, 2);
            values.setNull(start + 1);
            entries.setIndexDefined(start + 1);
            vector.endValue(0, 2);
            root.setRowCount(1);

            Map<String, Object> expected = new LinkedHashMap<>();
            expected.put("1", Arrays.asList("2026-08-05", "2026-08-06"));
            expected.put("2", null);
            assertEquals(expected, adbc.readMap(vector, 0, type));
            assertEquals(reader.readNested(type, vector.getObject(0)),
                    adbc.readMap(vector, 0, type));
        }
    }
}
