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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The text shapes BE's {@code put_mysql_row_buffer} produces for nested values (array_column.cpp,
 * map_column.cpp, struct_column.cpp, mysql_row_buffer.cpp, json_column.cpp), and what each must
 * become.
 */
public class MysqlTextReaderTest {

    private static Object read(String type, String text) {
        return MysqlTextReader.read(ColumnTypeParser.parse(type).get(), text, MysqlTextReader.BinaryEncoding.HEX);
    }

    private static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    public void testNumbersAndBooleansAreUnquoted() {
        assertEquals(Arrays.asList(1, -2, null), read("array<int(11)>", "[1,-2,null]"));
        assertEquals(Arrays.asList((byte) 7), read("array<tinyint(4)>", "[7]"));
        assertEquals(Arrays.asList(5L), read("array<bigint(20)>", "[5]"));
        // BE prints a BOOLEAN as 1/0 through push_number; only the declared type says it is a boolean.
        assertEquals(Arrays.asList(true, false), read("array<boolean>", "[1,0]"));
        assertEquals(Arrays.asList(new BigDecimal("1.50")), read("array<DECIMAL64(18,2)>", "[1.5]"));
        assertEquals(Arrays.asList(new BigDecimal("99999999999999999999999999999999999999")),
                read("array<largeint(40)>", "[99999999999999999999999999999999999999]"));
    }

    /** fmt spells non-finite values inf/-inf/nan, which Double.parseDouble rejects. */
    @Test
    public void testFloatsIncludingNonFinite() {
        List<?> d = (List<?>) read("array<double>", "[1.5,inf,-inf,nan]");
        assertEquals(1.5d, d.get(0));
        assertEquals(Double.POSITIVE_INFINITY, d.get(1));
        assertEquals(Double.NEGATIVE_INFINITY, d.get(2));
        assertTrue(Double.isNaN((Double) d.get(3)));
        assertEquals(Arrays.asList(2.5f), read("array<float>", "[2.5]"));
    }

    /** Strings escape only the quote and the backslash; a raw newline inside is legal text. */
    @Test
    public void testStringsWithBeEscapingAndRawControlChars() {
        assertEquals(Arrays.asList("a\"b", "c\\d", "line1\nline2", ""),
                read("array<varchar(20)>", "[\"a\\\"b\",\"c\\\\d\",\"line1\nline2\",\"\"]"));
    }

    /** DATE and DATETIME are already in TemporalText's format and pass through untouched. */
    @Test
    public void testTemporalsPassThrough() {
        assertEquals(Arrays.asList("2026-08-05"), read("array<date>", "[\"2026-08-05\"]"));
        assertEquals(Arrays.asList("2026-08-05 12:34:56.123456", "2026-08-05 12:34:56"),
                read("array<datetime>", "[\"2026-08-05 12:34:56.123456\",\"2026-08-05 12:34:56\"]"));
    }

    /** json_column.cpp pushes a nested JSON value with a single-quote escape char. */
    @Test
    public void testNestedJsonIsSingleQuoted() {
        assertEquals(Arrays.asList("{\"a\": 1}", "it\\'s"),
                read("array<json>", "['{\"a\": 1}','it\\\\\\'s']"));
    }

    @Test
    public void testBinaryDecodesPerSessionEncoding() {
        byte[] hex = (byte[]) ((List<?>) read("array<varbinary(4)>", "[\"0102ff\"]")).get(0);
        assertArrayEquals(new byte[] {1, 2, (byte) 0xff}, hex);
        byte[] b64 = (byte[]) ((List<?>) MysqlTextReader.read(ColumnTypeParser.parse("array<varbinary(4)>").get(),
                "[\"AQL/\"]", MysqlTextReader.BinaryEncoding.BASE64)).get(0);
        assertArrayEquals(new byte[] {1, 2, (byte) 0xff}, b64);
    }

    /** map_column.cpp prints a numeric key bare -- {1:2} -- which no JSON parser accepts. */
    @Test
    public void testMapKeysFollowTheDeclaredKeyType() {
        assertEquals(map("1", 10, "2", null), read("map<int(11),int(11)>", "{1:10,2:null}"));
        assertEquals(map("mk", 11), read("map<varchar(10),int(11)>", "{\"mk\":11}"));
        assertEquals(map("2026-08-05", "v"), read("map<date,varchar(10)>", "{\"2026-08-05\":\"v\"}"));
        assertEquals(map(), read("map<int(11),int(11)>", "{}"));
    }

    /** struct_column.cpp prints quoted field names; fields come back in declared order. */
    @Test
    public void testStructByDeclaredFieldsWithMissingAsNull() {
        Object out = read("struct<`x` int(11), `y` varchar(10), `z` date>", "{\"x\":7,\"y\":\"seven\"}");
        assertEquals(map("x", 7, "y", "seven", "z", null), out);
        assertEquals(Arrays.asList("x", "y", "z"), new java.util.ArrayList<>(((Map<?, ?>) out).keySet()));
    }

    @Test
    public void testThreeLevelsAndNullsAtEveryLevel() {
        Object out = read("array<map<varchar(10),struct<`x` int(11), `ys` array<datetime>>>>",
                "[{\"k\":{\"x\":1,\"ys\":[\"2026-08-05 12:34:56\",null]}},null,{}]");
        assertEquals(Arrays.asList(
                map("k", map("x", 1, "ys", Arrays.asList("2026-08-05 12:34:56", null))),
                null,
                map()), out);
        assertNull(read("array<int(11)>", "null"));
        assertEquals(Arrays.asList(), read("array<int(11)>", "[]"));
    }

    /** A quoted "null" is the string null, not SQL NULL. */
    @Test
    public void testQuotedNullIsAString() {
        assertEquals(Arrays.asList("null"), read("array<varchar(10)>", "[\"null\"]"));
    }

    @Test
    public void testMalformedTextIsReportedWithOffsetAndInput() {
        String[][] cases = {
            {"array<int(11)>", "[1,2"},
            {"struct<`x` int(11)>", "{1:2"},
            {"array<varchar(10)>", "[\"open"},
            {"struct<`x` int(11)>", "{\"nope\":1}"},
            {"array<int(11)>", "[1] x"},
            {"array<int(11)>", "[x]"},
            {"array<boolean>", "[2]"},
        };
        for (String[] c : cases) {
            try {
                read(c[0], c[1]);
                fail("expected DataException for " + c[1]);
            } catch (DataException e) {
                assertTrue(e.getMessage(), e.getMessage().contains(c[1]) || e.getMessage().contains("nope"));
            }
        }
    }
}
