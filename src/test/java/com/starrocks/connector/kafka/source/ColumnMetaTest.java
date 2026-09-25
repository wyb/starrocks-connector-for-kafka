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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Every column gets one {@link ColumnType}; these pin how it is derived from what the server said. */
public class ColumnMetaTest {

    private static ColumnType.Kind kindOf(String dataType) {
        return kindOf(dataType, dataType);
    }

    private static ColumnType.Kind kindOf(String dataType, String columnType) {
        return new ColumnMeta("c", dataType, columnType, 0, true).type.kind;
    }

    /** The closed set FE's {@code Type.toMysqlDataTypeString} emits; if StarRocks adds a type, this notices. */
    @Test
    public void testScalarKindsFollowTheServerType() {
        assertEquals(ColumnType.Kind.TINYINT, kindOf("tinyint"));
        assertEquals(ColumnType.Kind.SMALLINT, kindOf("smallint"));
        assertEquals(ColumnType.Kind.INT, kindOf("int"));
        assertEquals(ColumnType.Kind.BIGINT, kindOf("bigint"));
        assertEquals(ColumnType.Kind.LARGEINT, kindOf("bigint unsigned", "bigint(20) unsigned"));
        assertEquals(ColumnType.Kind.FLOAT, kindOf("float"));
        assertEquals(ColumnType.Kind.DOUBLE, kindOf("double"));
        assertEquals(ColumnType.Kind.DECIMAL, kindOf("decimal", "decimal(18, 2)"));
        assertEquals(ColumnType.Kind.STRING, kindOf("char", "char(10)"));
        assertEquals(ColumnType.Kind.STRING, kindOf("varchar", "varchar(10)"));
        assertEquals(ColumnType.Kind.DATE, kindOf("date"));
        assertEquals(ColumnType.Kind.DATETIME, kindOf("datetime"));
        assertEquals(ColumnType.Kind.BYTES, kindOf("binary", "binary(4)"));
        assertEquals(ColumnType.Kind.BYTES, kindOf("varbinary", "varbinary(16)"));
        assertEquals(ColumnType.Kind.JSON, kindOf("json"));
        for (String sketch : new String[] {"hll", "bitmap", "percentile"}) {
            assertEquals(sketch, ColumnType.Kind.OPAQUE, kindOf(sketch));
        }
    }

    /** FE renders BOOLEAN's DATA_TYPE as "tinyint" and only its COLUMN_TYPE as "tinyint(1)". */
    @Test
    public void testBooleanIsDistinguishedFromTinyint() {
        assertEquals(ColumnType.Kind.BOOLEAN, kindOf("tinyint", "tinyint(1)"));
        assertEquals(ColumnType.Kind.TINYINT, kindOf("tinyint", "tinyint(4)"));
        assertEquals(ColumnType.Kind.TINYINT, kindOf("tinyint", null));
    }

    /** DATA_TYPE casing and padding are the server's choice, not a contract. */
    @Test
    public void testServerTypeMatchingIgnoresCaseAndPadding() {
        assertEquals(ColumnType.Kind.INT, kindOf("  INT "));
        assertEquals(ColumnType.Kind.DATETIME, kindOf("DateTime"));
        assertEquals(ColumnType.Kind.LARGEINT, kindOf("BIGINT UNSIGNED"));
        assertEquals(ColumnType.Kind.BOOLEAN, kindOf("TinyInt", " TINYINT(1) "));
        assertEquals("bigint unsigned", new ColumnMeta("c", " BIGINT UNSIGNED ", null, 0, true).srDataType);
    }

    /** The startup log and the duplicate-column error print columns this way. */
    @Test
    public void testToStringShowsWhatTheServerSaidAndWhatItBecame() {
        assertEquals("v(varchar->string,sql=varchar(20),null=true)",
                new ColumnMeta("v", "VarChar", "varchar(20)", 0, true).toString());
        assertEquals("d(decimal->decimal(2),sql=decimal(18, 2),null=false)",
                new ColumnMeta("d", "decimal", "decimal(18, 2)", 2, false).toString());
    }

    @Test
    public void testDecimalKeepsTheDeclaredScale() {
        ColumnType t = new ColumnMeta("d", "decimal", "decimal(18, 2)", 2, true).type;
        assertEquals(ColumnType.Kind.DECIMAL, t.kind);
        assertEquals("2", t.toConnectSchema("t.d", true).parameters().get(Decimal.SCALE_FIELD));
    }

    /** LARGEINT is integral: Decimal at scale 0 whatever NUMERIC_SCALE says (FE reports NULL for it). */
    @Test
    public void testLargeIntIsItsOwnKindAtScaleZero() {
        for (int numericScale : new int[] {0, 9}) {
            ColumnType t = new ColumnMeta("big", "bigint unsigned", "bigint(20) unsigned", numericScale, true).type;
            assertEquals(ColumnType.Kind.LARGEINT, t.kind);
            assertEquals("0", t.toConnectSchema("t.big", true).parameters().get(Decimal.SCALE_FIELD));
        }
    }

    @Test
    public void testJsonIsItsOwnKind() {
        ColumnType t = new ColumnMeta("j", "json", "json", 0, true).type;
        assertEquals(ColumnType.Kind.JSON, t.kind);
        assertEquals(Json.LOGICAL_NAME, t.toConnectSchema("t.j", true).name());
    }

    @Test
    public void testParsedComplexColumnIsTheNestedTree() {
        ColumnMeta c = new ColumnMeta("a", "array", "array<int(11)>", 0, true);
        assertEquals(ColumnType.Kind.ARRAY, c.type.kind);
        assertTrue(c.type.isNested());
    }

    /** No parse, no guess: the column stays text and says which complex type it was. */
    @Test
    public void testUnparsedComplexColumnIsOpaqueTextNamedForItsType() {
        String[][] cases = {{"array", "com.starrocks.data.Array"}, {"map", "com.starrocks.data.Map"},
                            {"struct", "com.starrocks.data.Struct"}};
        for (String[] c : cases) {
            ColumnMeta col = new ColumnMeta("x", c[0], "struct<x int>", 0, false);
            assertEquals(c[0], ColumnType.Kind.OPAQUE, col.type.kind);
            assertFalse(col.type.isNested());
            Schema s = col.type.toConnectSchema("t.x", col.nullable);
            assertEquals(Schema.Type.STRING, s.type());
            assertEquals(c[1], s.name());
            assertFalse(s.isOptional());
        }
    }

    /** A type this connector never met is text with no name at all; so is a column the server was never asked about. */
    @Test
    public void testUnrecognizedTypeIsPlainText() {
        for (ColumnMeta c : new ColumnMeta[] {new ColumnMeta("u", "variant", "variant", 0, true),
                                              new ColumnMeta("n", null, null, 0, true)}) {
            assertEquals(ColumnType.Kind.OPAQUE, c.type.kind);
            Schema s = c.type.toConnectSchema("t." + c.name, true);
            assertEquals(Schema.Type.STRING, s.type());
            assertNull(s.name());
            assertTrue(s.isOptional());
        }
    }

    /** Real spellings StarRocks has and this connector does not map. */
    @Test
    public void testTypesStarRocksHasButThisConnectorDoesNotMapAreUnrecognized() {
        assertTrue(ColumnMeta.isUnrecognized("variant"));
        assertTrue(ColumnMeta.isUnrecognized("time"));
        assertTrue(ColumnMeta.isUnrecognized("unknown_type"));
    }

    /** Every type with a case in the mapping, including the ones carried as text on purpose. */
    @Test
    public void testMappedTypesAreRecognized() {
        for (String t : new String[] {"tinyint", "smallint", "int", "bigint", "bigint unsigned",
                                      "float", "double", "decimal", "char", "varchar", "date",
                                      "datetime", "binary", "varbinary", "array", "map", "struct",
                                      "json", "hll", "bitmap", "percentile"}) {
            assertFalse("expected " + t + " to be recognized", ColumnMeta.isUnrecognized(t));
        }
        assertFalse(ColumnMeta.isUnrecognized("  ARRAY "));
    }

    /** Null means the server was never asked, which is not the same as a type nobody knows. */
    @Test
    public void testNullServerTypeIsNotReportedAsUnrecognized() {
        assertFalse(ColumnMeta.isUnrecognized(null));
    }

    /** A SELECT of a sketch yields nothing usable, so the table is refused rather than streamed. */
    @Test
    public void testAggregateSketchTypesAreNonExportable() {
        assertTrue(ColumnMeta.isNonExportable("hll"));
        assertTrue(ColumnMeta.isNonExportable("bitmap"));
        assertTrue(ColumnMeta.isNonExportable("percentile"));
    }

    /** Complex types are exportable, natively or as text; a guard catching ARRAY would reject valid tables. */
    @Test
    public void testOrdinaryAndComplexTypesAreExportable() {
        for (String t : new String[] {"int", "varchar", "datetime", "json", "array", "map", "struct",
                                      "binary", "unknown", ""}) {
            assertFalse("expected " + t + " to be exportable", ColumnMeta.isNonExportable(t));
        }
    }

    /** A column described without consulting the server has no StarRocks name; it must not throw. */
    @Test
    public void testNullServerTypeIsExportable() {
        assertFalse(ColumnMeta.isNonExportable(null));
    }

    /** DATA_TYPE casing is the server's choice here too. */
    @Test
    public void testNonExportableMatchingIgnoresCaseAndPadding() {
        assertTrue(ColumnMeta.isNonExportable("  HLL "));
        assertTrue(ColumnMeta.isNonExportable("BitMap"));
    }
}
