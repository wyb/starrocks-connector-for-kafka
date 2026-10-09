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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Every spelling FE's {@code Type.toSql()} produces inside a nested type, and every string it can
 * produce that must be refused rather than guessed at.
 */
public class ColumnTypeParserTest {

    private static ColumnType parse(String s) {
        Optional<ColumnType> t = ColumnTypeParser.parse(s);
        assertTrue("expected to parse: " + s, t.isPresent());
        return t.get();
    }

    private static Schema schema(String s) {
        return parse(s).toConnectSchema("t.c", true);
    }

    @Test
    public void testScalarElementsAsFeSpellsThem() {
        assertEquals("array<boolean>", parse("array<boolean>").toString());
        assertEquals("array<tinyint>", parse("array<tinyint(4)>").toString());
        assertEquals("array<smallint>", parse("array<smallint(6)>").toString());
        assertEquals("array<int>", parse("array<int(11)>").toString());
        assertEquals("array<bigint>", parse("array<bigint(20)>").toString());
        assertEquals("array<largeint>", parse("array<largeint(40)>").toString());
        assertEquals("array<float>", parse("array<float>").toString());
        assertEquals("array<double>", parse("array<double>").toString());
        assertEquals("array<string>", parse("array<varchar(10)>").toString());
        assertEquals("array<string>", parse("array<char(3)>").toString());
        assertEquals("array<bytes>", parse("array<varbinary(16)>").toString());
        assertEquals("array<date>", parse("array<date>").toString());
        assertEquals("array<datetime>", parse("array<datetime>").toString());
        assertEquals("array<json>", parse("array<json>").toString());
    }

    /**
     * Four spellings of one decimal: DATA_TYPE says "decimal", COLUMN_TYPE says "decimal(18, 2)",
     * an array item says "DECIMAL64(18,2)" (ArrayType.toSql goes through toString), and DECIMALV2
     * says "DECIMAL(27,9)". Only the scale matters to the schema.
     */
    @Test
    public void testDecimalSpellingsAllYieldTheScale() {
        assertEquals("array<decimal(2)>", parse("array<DECIMAL64(18,2)>").toString());
        assertEquals("array<decimal(9)>", parse("array<DECIMAL(27,9)>").toString());
        assertEquals("map<string,decimal(2)>", parse("map<varchar(10),decimal(18, 2)>").toString());
        assertEquals("struct<d:decimal(0)>", parse("struct<`d` decimal(38, 0)>").toString());
        assertEquals("2", schema("array<DECIMAL128(38,2)>").valueSchema().parameters().get(Decimal.SCALE_FIELD));
    }

    /** A bare "decimal" is a wildcard; no column carries one, so it is a refusal, not scale 0. */
    @Test
    public void testDecimalWithoutPrecisionIsRefused() {
        assertFalse(ColumnTypeParser.parse("array<decimal>").isPresent());
    }

    /** JsonConverter renders a map as an object only when the key schema is STRING. */
    @Test
    public void testMapKeysTravelAsStringButKeepTheDeclaredType() {
        ColumnType t = parse("map<int(11),int(11)>");
        assertEquals(ColumnType.Kind.INT, t.key.kind);
        Schema s = t.toConnectSchema("t.m", false);
        assertEquals(Schema.Type.MAP, s.type());
        assertEquals(Schema.Type.STRING, s.keySchema().type());
        assertEquals(Schema.Type.INT32, s.valueSchema().type());
        assertTrue(s.valueSchema().isOptional());
        assertFalse(s.isOptional());
    }

    /** Fields keep declared order and names; a doubled backtick is one literal backtick. */
    @Test
    public void testStructFieldsKeepOrderAndUnescapeNames() {
        ColumnType t = parse("struct<`x` int(11), `y` varchar(10), `a``b` date>");
        assertEquals("struct<x:int,y:string,a`b:date>", t.toString());
        Schema s = t.toConnectSchema("t.s", true);
        assertEquals("t.s", s.name());
        assertEquals("x", s.fields().get(0).name());
        assertEquals("a`b", s.fields().get(2).name());
        assertEquals(ColumnType.DATE_LOGICAL_NAME, s.field("a`b").schema().name());
    }

    @Test
    public void testThreeLevelsOfNesting() {
        ColumnType t = parse("array<map<varchar(10),struct<`x` int(11), `ys` array<datetime>>>>");
        assertEquals("array<map<string,struct<x:int,ys:array<datetime>>>>", t.toString());
        Schema s = t.toConnectSchema("t.deep", true);
        Schema inner = s.valueSchema().valueSchema();
        assertEquals(Schema.Type.STRUCT, inner.type());
        assertEquals("t.deep.element.value", inner.name());
        assertEquals(ColumnType.DATETIME_LOGICAL_NAME,
                inner.field("ys").schema().valueSchema().name());
    }

    /** The reader checks a driver's field names against this set, so it must mirror the declaration. */
    @Test
    public void testStructFieldNamesMirrorTheDeclaration() {
        assertEquals(new HashSet<>(Arrays.asList("x", "y", "a`b")),
                parse("struct<`x` int(11), `y` varchar(10), `a``b` date>").fieldNames);
        assertTrue(parse("array<int(11)>").fieldNames.isEmpty());
    }

    /** Names must be unique per nested struct or the Avro converter rejects the record schema. */
    @Test
    public void testNestedStructNamesDeriveFromThePath() {
        Schema s = schema("struct<`a` struct<`b` int(11)>, `c` array<struct<`d` int(11)>>>");
        assertEquals("t.c.a", s.field("a").schema().name());
        assertEquals("t.c.c.element", s.field("c").schema().valueSchema().name());
    }

    @Test
    public void testJsonElementsKeepDebeziumsName() {
        assertEquals(Json.LOGICAL_NAME, schema("array<json>").valueSchema().name());
    }

    /** FE appends a field comment unescaped, so any comment makes the string unparseable. */
    @Test
    public void testStructFieldCommentIsRefused() {
        assertFalse(ColumnTypeParser.parse("struct<`a` int(11) COMMENT 'x'>").isPresent());
        assertFalse(ColumnTypeParser.parse("struct<`a` int(11) COMMENT 'a > b, c'>").isPresent());
    }

    /** Past 15 levels FE prints "..." for the rest; better text than a wrong schema. */
    @Test
    public void testTruncationMarkerIsRefused() {
        assertFalse(ColumnTypeParser.parse("array<...>").isPresent());
        assertFalse(ColumnTypeParser.parse("struct<`a` ...>").isPresent());
    }

    @Test
    public void testTypesNoRecordCanCarryAreRefused() {
        for (String s : new String[] {"array<hll>", "map<varchar(10),bitmap>", "struct<`p` percentile>",
                                      "array<variant>", "array<time>", "array<TIME>", "array<function>"}) {
            assertFalse("expected refusal: " + s, ColumnTypeParser.parse(s).isPresent());
        }
    }

    @Test
    public void testMalformedInputIsRefusedNotThrown() {
        for (String s : new String[] {null, "", "  ", "array<int(11)", "array<int(11)>>", "map<int(11)>",
                                      "struct<x int(11)>", "struct<`x` int(11)", "array<>", "int(11) unsigned",
                                      "array<int(11)> trailing"}) {
            assertFalse("expected refusal: " + s, ColumnTypeParser.parse(s).isPresent());
        }
    }

    /** Top-level scalars are not what the parser is for, but the same grammar covers them. */
    @Test
    public void testBareScalarParses() {
        assertEquals("datetime", parse("datetime").toString());
        assertEquals(Schema.Type.INT64, schema("bigint(20)").type());
    }
}
