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

import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** The {@code DATA_TYPE} to {@link Types} mapping, and which types the connector refuses outright. */
public class ColumnMetadataReaderTest {

    /** A transcription of the closed set FE can emit; if StarRocks adds a type, this notices. */
    @Test
    public void testEveryStarRocksDataTypeMapsToAJdbcType() {
        assertEquals(Types.TINYINT, ColumnMetadataReader.toJdbcType("tinyint"));
        assertEquals(Types.SMALLINT, ColumnMetadataReader.toJdbcType("smallint"));
        assertEquals(Types.INTEGER, ColumnMetadataReader.toJdbcType("int"));
        assertEquals(Types.BIGINT, ColumnMetadataReader.toJdbcType("bigint"));
        assertEquals(Types.REAL, ColumnMetadataReader.toJdbcType("float"));
        assertEquals(Types.DOUBLE, ColumnMetadataReader.toJdbcType("double"));
        assertEquals(Types.DECIMAL, ColumnMetadataReader.toJdbcType("decimal"));
        assertEquals(Types.CHAR, ColumnMetadataReader.toJdbcType("char"));
        assertEquals(Types.VARCHAR, ColumnMetadataReader.toJdbcType("varchar"));
        assertEquals(Types.DATE, ColumnMetadataReader.toJdbcType("date"));
        assertEquals(Types.TIMESTAMP, ColumnMetadataReader.toJdbcType("datetime"));
        assertEquals(Types.BINARY, ColumnMetadataReader.toJdbcType("binary"));
        assertEquals(Types.VARBINARY, ColumnMetadataReader.toJdbcType("varbinary"));
    }

    /** LARGEINT is 128-bit and spelled "bigint unsigned"; BIGINT would silently truncate it. */
    @Test
    public void testLargeIntIsNotMappedToBigint() {
        assertEquals(Types.OTHER, ColumnMetadataReader.toJdbcType("bigint unsigned"));
    }

    /** Opaque and unknown types are read as text rather than guessed at. */
    @Test
    public void testOpaqueAndUnknownTypesFallBackToOther() {
        for (String t : new String[] {"hll", "bitmap", "percentile", "json", "unknown", "", null}) {
            assertEquals("expected OTHER for " + t, Types.OTHER, ColumnMetadataReader.toJdbcType(t));
        }
    }

    /** DATA_TYPE casing is the server's choice, not a contract. */
    @Test
    public void testDataTypeMatchingIgnoresCaseAndPadding() {
        assertEquals(Types.INTEGER, ColumnMetadataReader.toJdbcType("  INT "));
        assertEquals(Types.TIMESTAMP, ColumnMetadataReader.toJdbcType("DateTime"));
        assertEquals(Types.OTHER, ColumnMetadataReader.toJdbcType("BIGINT UNSIGNED"));
    }

    /** OTHER is rendered as text, which is the lossy path these two spellings exist to avoid. */
    @Test
    public void testBinarySpellingsDoNotFallBackToText() {
        assertEquals(Types.BINARY, ColumnMetadataReader.toJdbcType("binary"));
        assertEquals(Types.VARBINARY, ColumnMetadataReader.toJdbcType("varbinary"));
    }

    /** FE spells these exactly "array", "map" and "struct"; all share OTHER, hence srDataType. */
    @Test
    public void testComplexTypesAreCarriedAsTextForNow() {
        assertEquals(Types.OTHER, ColumnMetadataReader.toJdbcType("array"));
        assertEquals(Types.OTHER, ColumnMetadataReader.toJdbcType("map"));
        assertEquals(Types.OTHER, ColumnMetadataReader.toJdbcType("struct"));
        assertEquals(Types.OTHER, ColumnMetadataReader.toJdbcType("json"));
    }

    /**
     * Real spellings StarRocks has and this connector does not map. A check for the bare word
     * "unknown" -- which this once was -- matches none of them, so a type StarRocks added went by
     * in silence.
     */
    @Test
    public void testTypesStarRocksHasButThisConnectorDoesNotMapAreUnrecognized() {
        assertTrue(ColumnMetadataReader.isUnrecognized("variant"));
        assertTrue(ColumnMetadataReader.isUnrecognized("time"));
        assertTrue(ColumnMetadataReader.isUnrecognized("unknown_type"));
    }

    /** Every type with a case in toJdbcType, including the ones carried as text on purpose. */
    @Test
    public void testMappedTypesAreRecognized() {
        for (String t : new String[] {"tinyint", "smallint", "int", "bigint", "bigint unsigned",
                                      "float", "double", "decimal", "char", "varchar", "date",
                                      "datetime", "binary", "varbinary", "array", "map", "struct",
                                      "json", "hll", "bitmap", "percentile"}) {
            assertFalse("expected " + t + " to be recognized", ColumnMetadataReader.isUnrecognized(t));
        }
        assertFalse(ColumnMetadataReader.isUnrecognized("  ARRAY "));
    }

    /** Null means the server was never asked, which is not the same as a type nobody knows. */
    @Test
    public void testNullStarRocksTypeIsNotReportedAsUnrecognized() {
        assertFalse(ColumnMetadataReader.isUnrecognized(null));
    }

    /** A SELECT of a sketch yields nothing usable, so the table is refused rather than streamed. */
    @Test
    public void testAggregateSketchTypesAreNonExportable() {
        assertTrue(ColumnMetadataReader.isNonExportable("hll"));
        assertTrue(ColumnMetadataReader.isNonExportable("bitmap"));
        assertTrue(ColumnMetadataReader.isNonExportable("percentile"));
    }

    /** Complex types are exportable, just as text; a guard catching ARRAY would reject valid tables. */
    @Test
    public void testOrdinaryAndComplexTypesAreExportable() {
        for (String t : new String[] {"int", "varchar", "datetime", "json", "array", "map", "struct",
                                      "binary", "unknown", ""}) {
            assertFalse("expected " + t + " to be exportable", ColumnMetadataReader.isNonExportable(t));
        }
    }

    /** A column described without consulting the server has no StarRocks name; it must not throw. */
    @Test
    public void testNullStarRocksTypeIsExportable() {
        assertFalse(ColumnMetadataReader.isNonExportable(null));
    }

    /** DATA_TYPE casing is the server's choice here too. */
    @Test
    public void testNonExportableMatchingIgnoresCaseAndPadding() {
        assertTrue(ColumnMetadataReader.isNonExportable("  HLL "));
        assertTrue(ColumnMetadataReader.isNonExportable("BitMap"));
    }
}
