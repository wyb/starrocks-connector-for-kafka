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

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Every column gets one {@link ColumnType}; these pin how it is derived from what the server said. */
public class ColumnMetaTest {

    private static ColumnType typeOf(String dataType, String columnType) {
        return new ColumnMeta("c", dataType, columnType, 0, true).type;
    }

    private static ColumnType.Kind kindOf(String dataType) {
        return typeOf(dataType, dataType).kind;
    }

    /** The closed set FE's {@code Type.toMysqlDataTypeString} emits; if StarRocks adds a type, this notices. */
    @Test
    public void testKindsFollowTheServerType() {
        assertEquals(ColumnType.Kind.TINYINT, kindOf("tinyint"));
        assertEquals(ColumnType.Kind.SMALLINT, kindOf("smallint"));
        assertEquals(ColumnType.Kind.INT, kindOf("int"));
        assertEquals(ColumnType.Kind.BIGINT, kindOf("bigint"));
        assertEquals(ColumnType.Kind.LARGEINT, typeOf("bigint unsigned", "bigint(20) unsigned").kind);
        assertEquals(ColumnType.Kind.FLOAT, kindOf("float"));
        assertEquals(ColumnType.Kind.DOUBLE, kindOf("double"));
        assertEquals(ColumnType.Kind.DECIMAL, typeOf("decimal", "decimal(18, 2)").kind);
        assertEquals(ColumnType.Kind.STRING, typeOf("char", "char(10)").kind);
        assertEquals(ColumnType.Kind.STRING, typeOf("varchar", "varchar(10)").kind);
        assertEquals(ColumnType.Kind.DATE, kindOf("date"));
        assertEquals(ColumnType.Kind.DATETIME, kindOf("datetime"));
        assertEquals(ColumnType.Kind.BYTES, typeOf("binary", "binary(4)").kind);
        assertEquals(ColumnType.Kind.BYTES, typeOf("varbinary", "varbinary(16)").kind);
        assertEquals(ColumnType.Kind.JSON, kindOf("json"));
        assertEquals(ColumnType.Kind.ARRAY, typeOf("array", "array<int(11)>").kind);
        assertEquals(ColumnType.Kind.MAP, typeOf("map", "map<varchar(10),int(11)>").kind);
        assertEquals(ColumnType.Kind.STRUCT, typeOf("struct", "struct<`x` int(11)>").kind);
        assertTrue(typeOf("array", "array<int(11)>").isNested());
        for (String sketch : new String[] {"hll", "bitmap", "percentile"}) {
            assertEquals(sketch, ColumnType.Kind.OPAQUE, kindOf(sketch));
        }
    }

    /** FE renders BOOLEAN's DATA_TYPE as "tinyint" and only its COLUMN_TYPE as "tinyint(1)". */
    @Test
    public void testBooleanIsDistinguishedFromTinyint() {
        assertEquals(ColumnType.Kind.BOOLEAN, typeOf("tinyint", "tinyint(1)").kind);
        assertEquals(ColumnType.Kind.TINYINT, typeOf("tinyint", "tinyint(4)").kind);
        assertEquals(ColumnType.Kind.TINYINT, typeOf("tinyint", null).kind);
    }

    /** DATA_TYPE casing and padding are the server's choice, not a contract. */
    @Test
    public void testServerTypeMatchingIgnoresCaseAndPadding() {
        assertEquals(ColumnType.Kind.INT, kindOf("  INT "));
        assertEquals(ColumnType.Kind.DATETIME, kindOf("DateTime"));
        assertEquals(ColumnType.Kind.LARGEINT, kindOf("BIGINT UNSIGNED"));
        assertEquals(ColumnType.Kind.BOOLEAN, typeOf("TinyInt", " TINYINT(1) ").kind);
        assertEquals("bigint unsigned", new ColumnMeta("c", " BIGINT UNSIGNED ", null, 0, true).srDataType);
        assertFalse(ColumnMeta.isUnrecognized("  ARRAY "));
        assertTrue(ColumnMeta.isNonExportable("  HLL "));
        assertTrue(ColumnMeta.isNonExportable("BitMap"));
    }

    /** The startup log and the duplicate-column error print columns this way. */
    @Test
    public void testToStringShowsWhatTheServerSaidAndWhatItBecame() {
        assertEquals("v(varchar->string,sql=varchar(20),null=true)",
                new ColumnMeta("v", "VarChar", "varchar(20)", 0, true).toString());
        assertEquals("d(decimal->decimal(2),sql=decimal(18, 2),null=false)",
                new ColumnMeta("d", "decimal", "decimal(18, 2)", 2, false).toString());
        assertEquals("id(int->int,sql=int(11),null=false,key)",
                new ColumnMeta("id", "int", "int(11)", 0, false, true).toString());
    }

    /** The five-argument constructor is the non-key column; only the reader sets the flag. */
    @Test
    public void testKeyFlagDefaultsToFalse() {
        assertFalse(new ColumnMeta("v", "int", "int(11)", 0, true).key);
        assertTrue(new ColumnMeta("id", "int", "int(11)", 0, false, true).key);
    }

    /** NUMERIC_SCALE reaches DECIMAL only; LARGEINT is integral and stays at scale 0 whatever FE reports. */
    @Test
    public void testDecimalTakesNumericScaleAndLargeIntIgnoresIt() {
        ColumnType d = new ColumnMeta("d", "decimal", "decimal(18, 2)", 2, true).type;
        assertEquals("2", d.toConnectSchema("t.d", true).parameters().get(Decimal.SCALE_FIELD));
        for (int numericScale : new int[] {0, 9}) {
            ColumnType t = new ColumnMeta("big", "bigint unsigned", "bigint(20) unsigned", numericScale, true).type;
            assertEquals(ColumnType.Kind.LARGEINT, t.kind);
            assertEquals("0", t.toConnectSchema("t.big", true).parameters().get(Decimal.SCALE_FIELD));
        }
    }

    /**
     * What cannot be mapped is text: a complex column that did not parse keeps its type's logical
     * name; a type never met, or a column the server was not asked about, has none.
     */
    @Test
    public void testWhatCannotBeMappedIsOpaqueText() {
        String[][] named = {{"array", "com.starrocks.data.Array"}, {"map", "com.starrocks.data.Map"},
                            {"struct", "com.starrocks.data.Struct"}};
        for (String[] c : named) {
            ColumnMeta col = new ColumnMeta("x", c[0], "struct<x int>", 0, false);
            assertEquals(c[0], ColumnType.Kind.OPAQUE, col.type.kind);
            assertFalse(col.type.isNested());
            Schema s = col.type.toConnectSchema("t.x", col.nullable);
            assertEquals(Schema.Type.STRING, s.type());
            assertEquals(c[1], s.name());
            assertFalse(s.isOptional());
        }
        for (ColumnMeta col : new ColumnMeta[] {new ColumnMeta("u", "variant", "variant", 0, true),
                                                new ColumnMeta("n", null, null, 0, true)}) {
            assertEquals(ColumnType.Kind.OPAQUE, col.type.kind);
            Schema s = col.type.toConnectSchema("t." + col.name, true);
            assertEquals(Schema.Type.STRING, s.type());
            assertNull(s.name());
            assertTrue(s.isOptional());
        }
    }

    /** The complement of the known set; null means the server was not asked, not a type nobody knows. */
    @Test
    public void testUnrecognizedIsTheComplementOfTheKnownSet() {
        for (String t : new String[] {"tinyint", "smallint", "int", "bigint", "bigint unsigned",
                                      "float", "double", "decimal", "char", "varchar", "date",
                                      "datetime", "binary", "varbinary", "array", "map", "struct",
                                      "json", "hll", "bitmap", "percentile"}) {
            assertFalse("expected " + t + " to be recognized", ColumnMeta.isUnrecognized(t));
        }
        for (String t : new String[] {"variant", "time", "unknown_type"}) {
            assertTrue("expected " + t + " to be unrecognized", ColumnMeta.isUnrecognized(t));
        }
        assertFalse(ColumnMeta.isUnrecognized(null));
    }

    /** Only the aggregate sketches are refused; complex and unknown types are exportable, natively or as text. */
    @Test
    public void testOnlyAggregateSketchesAreNonExportable() {
        for (String t : new String[] {"hll", "bitmap", "percentile"}) {
            assertTrue("expected " + t + " to be non-exportable", ColumnMeta.isNonExportable(t));
        }
        for (String t : new String[] {"int", "varchar", "datetime", "json", "array", "map", "struct",
                                      "binary", "unknown", "", null}) {
            assertFalse("expected " + t + " to be exportable", ColumnMeta.isNonExportable(t));
        }
    }
}
