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

import io.debezium.data.Json;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Every column gets one {@link ColumnType}; these pin how it is derived from what the server said. */
public class ColumnMetaTest {

    private static ColumnType.Kind kindOf(int jdbcType) {
        return new ColumnMeta("c", jdbcType, 0, 0, true).type.kind;
    }

    @Test
    public void testScalarKindsFollowTheJdbcType() {
        assertEquals(ColumnType.Kind.BOOLEAN, kindOf(Types.BOOLEAN));
        assertEquals(ColumnType.Kind.BOOLEAN, kindOf(Types.BIT));
        assertEquals(ColumnType.Kind.TINYINT, kindOf(Types.TINYINT));
        assertEquals(ColumnType.Kind.SMALLINT, kindOf(Types.SMALLINT));
        assertEquals(ColumnType.Kind.INT, kindOf(Types.INTEGER));
        assertEquals(ColumnType.Kind.BIGINT, kindOf(Types.BIGINT));
        assertEquals(ColumnType.Kind.FLOAT, kindOf(Types.REAL));
        assertEquals(ColumnType.Kind.DOUBLE, kindOf(Types.FLOAT));
        assertEquals(ColumnType.Kind.DOUBLE, kindOf(Types.DOUBLE));
        assertEquals(ColumnType.Kind.DATE, kindOf(Types.DATE));
        assertEquals(ColumnType.Kind.DATETIME, kindOf(Types.TIMESTAMP));
        assertEquals(ColumnType.Kind.BYTES, kindOf(Types.BINARY));
        assertEquals(ColumnType.Kind.BYTES, kindOf(Types.VARBINARY));
        assertEquals(ColumnType.Kind.STRING, kindOf(Types.CHAR));
        assertEquals(ColumnType.Kind.STRING, kindOf(Types.VARCHAR));
        assertEquals(ColumnType.Kind.OPAQUE, kindOf(Types.OTHER));
    }

    @Test
    public void testDecimalKeepsTheDeclaredScale() {
        ColumnType t = new ColumnMeta("d", Types.DECIMAL, 18, 2, true).type;
        assertEquals(ColumnType.Kind.DECIMAL, t.kind);
        assertEquals("2", t.toConnectSchema("t.d", true).parameters().get(Decimal.SCALE_FIELD));
    }

    /** DATA_TYPE "bigint unsigned" on the DECIMAL getters is LARGEINT: Decimal at scale 0, always. */
    @Test
    public void testLargeIntIsItsOwnKind() {
        ColumnType t = new ColumnMeta("big", Types.DECIMAL, 39, 0, true, "bigint unsigned", "bigint(20) unsigned").type;
        assertEquals(ColumnType.Kind.LARGEINT, t.kind);
        assertEquals("0", t.toConnectSchema("t.big", true).parameters().get(Decimal.SCALE_FIELD));
    }

    @Test
    public void testJsonIsItsOwnKind() {
        ColumnType t = new ColumnMeta("j", Types.OTHER, 0, 0, true, "json", "json").type;
        assertEquals(ColumnType.Kind.JSON, t.kind);
        assertEquals(Json.LOGICAL_NAME, t.toConnectSchema("t.j", true).name());
    }

    @Test
    public void testParsedComplexColumnIsTheNestedTree() {
        ColumnMeta c = new ColumnMeta("a", Types.OTHER, 0, 0, true, "array", "array<int(11)>");
        assertEquals(ColumnType.Kind.ARRAY, c.type.kind);
        assertTrue(c.type.isNested());
    }

    /** No parse, no guess: the column stays text and says which complex type it was. */
    @Test
    public void testUnparsedComplexColumnIsOpaqueTextNamedForItsType() {
        String[][] cases = {{"array", "com.starrocks.data.Array"}, {"map", "com.starrocks.data.Map"},
                            {"struct", "com.starrocks.data.Struct"}};
        for (String[] c : cases) {
            ColumnMeta col = new ColumnMeta("x", Types.OTHER, 0, 0, false, c[0], "struct<x int>");
            assertEquals(c[0], ColumnType.Kind.OPAQUE, col.type.kind);
            assertFalse(col.type.isNested());
            Schema s = col.type.toConnectSchema("t.x", col.nullable);
            assertEquals(Schema.Type.STRING, s.type());
            assertEquals(c[1], s.name());
            assertFalse(s.isOptional());
        }
    }

    /** A type this connector never met is text with no name at all. */
    @Test
    public void testUnrecognizedTypeIsPlainText() {
        ColumnType t = new ColumnMeta("u", Types.OTHER, 0, 0, true, "variant", "variant").type;
        assertEquals(ColumnType.Kind.OPAQUE, t.kind);
        Schema s = t.toConnectSchema("t.u", true);
        assertEquals(Schema.Type.STRING, s.type());
        assertNull(s.name());
        assertTrue(s.isOptional());
    }
}
