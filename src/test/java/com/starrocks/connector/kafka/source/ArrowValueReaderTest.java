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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Feeds the reader the object shapes the Arrow Flight JDBC driver produces (verified against
 * flight-sql-jdbc-driver 18.0.0) and checks the neutral value, then the assembled Connect value.
 */
public class ArrowValueReaderTest {

    /** Stands in for org.apache.arrow.vector.util.Text: a CharSequence that is not a String. */
    private static CharSequence text(String s) {
        return new StringBuilder(s);
    }

    private static ColumnType type(String columnType) {
        return ColumnTypeParser.parse(columnType).get();
    }

    private static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    /** DATE arrives as Integer days and DATETIME as Long micros; both are INT-shaped by class. */
    @Test
    public void testDeclaredTypeDisambiguatesDaysAndMicrosFromInts() {
        assertEquals(Arrays.asList("2026-08-05"), new ArrowValueReader().nested(type("array<date>"), Arrays.asList(20670)));
        assertEquals(Arrays.asList(20670), new ArrowValueReader().nested(type("array<int(11)>"), Arrays.asList(20670)));
        assertEquals(Arrays.asList("2026-08-05 12:34:56.123456"),
                new ArrowValueReader().nested(type("array<datetime>"), Arrays.asList(1785933296123456L)));
        assertEquals(Arrays.asList(1785933296123456L),
                new ArrowValueReader().nested(type("array<bigint(20)>"), Arrays.asList(1785933296123456L)));
    }

    @Test
    public void testTextBecomesStringAndBytesStayBytes() {
        assertEquals(Arrays.asList("a", "b"),
                new ArrowValueReader().nested(type("array<varchar(10)>"), Arrays.asList(text("a"), text("b"))));
        List<?> bytes = (List<?>) new ArrowValueReader().nested(type("array<varbinary(4)>"),
                Arrays.asList(new byte[] {1, 2, (byte) 0xff}));
        assertArrayEquals(new byte[] {1, 2, (byte) 0xff}, (byte[]) bytes.get(0));
    }

    @Test
    public void testDecimalsTakeTheDeclaredScale() {
        assertEquals(Arrays.asList(new BigDecimal("1.50")),
                new ArrowValueReader().nested(type("array<DECIMAL64(18,2)>"), Arrays.asList(new BigDecimal("1.5"))));
        assertEquals(Arrays.asList(new BigDecimal("99999999999999999999999999999999999999")),
                new ArrowValueReader().nested(type("array<largeint(40)>"),
                        Arrays.asList(new BigDecimal("99999999999999999999999999999999999999"))));
    }

    /** The nested form: MapVector.getObject is ListVector's, a List of {key, value} entry maps. */
    private static List<Object> entries(Object... kv) {
        List<Object> out = new java.util.ArrayList<>();
        for (int i = 0; i < kv.length; i += 2) {
            Map<String, Object> e = new LinkedHashMap<>();
            e.put("key", kv[i]);
            e.put("value", kv[i + 1]);
            out.add(e);
        }
        return out;
    }

    /**
     * A map nested in a list or struct arrives as a List of entry maps, not a Map -- the
     * accessor's Map conversion only happens for a top-level column. Both shapes must read alike.
     */
    @Test
    public void testNestedMapArrivesAsEntryListAndReadsLikeTheTopLevelMap() {
        assertEquals(Arrays.asList(map("1", 10, "2", null)),
                new ArrowValueReader().nested(type("array<map<int(11),int(11)>>"), Arrays.asList(entries(1, 10, 2, null))));
        Map<Object, Object> row = new LinkedHashMap<>();
        row.put("tags", entries(text("k"), 20670));
        assertEquals(map("tags", map("k", "2026-08-05")),
                new ArrowValueReader().nested(type("struct<`tags` map<varchar(10),date>>"), row));
        assertEquals(Arrays.asList(map()),
                new ArrowValueReader().nested(type("array<map<int(11),int(11)>>"), Arrays.asList(entries())));
    }

    /** Map keys become strings spelled per their declared type, values are read like elements. */
    @Test
    public void testMapKeysAreSpelledPerDeclaredType() {
        Map<Object, Object> intKeyed = new LinkedHashMap<>();
        intKeyed.put(1, 10);
        intKeyed.put(2, null);
        assertEquals(map("1", 10, "2", null), new ArrowValueReader().nested(type("map<int(11),int(11)>"), intKeyed));

        Map<Object, Object> dateKeyed = new LinkedHashMap<>();
        dateKeyed.put(20670, text("v"));
        assertEquals(map("2026-08-05", "v"), new ArrowValueReader().nested(type("map<date,varchar(10)>"), dateKeyed));
    }

    /** Struct fields come back in declared order and by name, even if the driver's map is sparse. */
    @Test
    public void testStructFollowsTheDeclaredFieldsNotTheMap() {
        Map<Object, Object> in = new LinkedHashMap<>();
        in.put("y", text("seven"));
        in.put("x", 7);
        Object out = new ArrowValueReader().nested(type("struct<`x` int(11), `y` varchar(10), `z` date>"), in);
        assertEquals(map("x", 7, "y", "seven", "z", null), out);
        assertEquals(Arrays.asList("x", "y", "z"), new java.util.ArrayList<>(((Map<?, ?>) out).keySet()));
    }

    @Test
    public void testNullsPassThroughAtEveryLevel() {
        assertNull(new ArrowValueReader().nested(type("array<int(11)>"), null));
        assertEquals(Arrays.asList(1, null), new ArrowValueReader().nested(type("array<int(11)>"), Arrays.asList(1, null)));
    }

    /** End to end: neutral value into the Connect value the schema wants, structs included. */
    @Test
    public void testAssembledValueMatchesTheConnectSchema() {
        ColumnType t = type("array<struct<`x` int(11), `tags` map<varchar(10),date>>>");
        Schema s = t.toConnectSchema("t.c", true);
        Map<Object, Object> row = new LinkedHashMap<>();
        row.put("x", 7);
        row.put("tags", entries(text("k"), 20670)); // nested, so the entry-list shape

        Object neutral = new ArrowValueReader().nested(t, Arrays.asList(row, null));
        List<?> value = (List<?>) t.toConnectValue(s, neutral);

        Struct first = (Struct) value.get(0);
        first.validate();
        assertEquals(7, first.get("x"));
        assertEquals(map("k", "2026-08-05"), first.get("tags"));
        assertNull(value.get(1));
    }

    /** A shape the declared type cannot explain is a schema mismatch, reported, not coerced. */
    @Test
    public void testUnexpectedShapeIsReportedWithTheType() {
        try {
            new ArrowValueReader().nested(type("array<int(11)>"), map("not", "a list"));
            fail("expected DataException");
        } catch (DataException e) {
            assertEquals(true, e.getMessage().contains("array<int>"));
            assertEquals(true, e.getMessage().contains("LinkedHashMap"));
        }
    }
}
