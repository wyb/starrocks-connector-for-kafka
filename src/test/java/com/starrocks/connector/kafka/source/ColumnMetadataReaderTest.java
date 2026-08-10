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

/**
 * The {@code DATA_TYPE} spelling to {@link Types} mapping used on the Arrow Flight transport, where
 * the driver's own result-set description is unusable and {@code information_schema.columns} is the
 * only trustworthy source.
 *
 * <p>These cases used to live in a class named after URL parsing, which is where they were least
 * likely to be found or maintained.
 */
public class ColumnMetadataReaderTest {

    /**
     * Every spelling the BE's {@code SchemaColumnsScanner::to_mysql_data_type_string} can put in
     * {@code information_schema.columns.DATA_TYPE}. That switch is the closed set this mapping
     * answers to, so the cases here are a transcription of it -- if StarRocks adds a type there,
     * one of these will be the thing that notices.
     */
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

    /**
     * LARGEINT is 128-bit and arrives spelled "bigint unsigned". Mapping it to BIGINT would
     * silently truncate every value that does not fit a long, so it goes to OTHER and is carried
     * as text instead.
     */
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

    /**
     * BINARY and VARBINARY must not land on OTHER: OTHER is rendered as a string by
     * {@code ChangeRecordMapper}, and putting binary through a string is exactly the lossy path
     * these two spellings exist to avoid.
     */
    @Test
    public void testBinarySpellingsDoNotFallBackToText() {
        assertEquals(Types.BINARY, ColumnMetadataReader.toJdbcType("binary"));
        assertEquals(Types.VARBINARY, ColumnMetadataReader.toJdbcType("varbinary"));
    }

    /**
     * Complex types share {@link Types#OTHER} with everything else carried as text, so the JDBC
     * type alone cannot tell them apart -- which is why they are listed explicitly in the switch
     * and why the StarRocks name is kept on {@link ColumnMeta}. FE renders these three via
     * {@code toMysqlDataTypeString()} as exactly "array", "map" and "struct".
     */
    @Test
    public void testComplexTypesAreCarriedAsTextForNow() {
        assertEquals(Types.OTHER, ColumnMetadataReader.toJdbcType("array"));
        assertEquals(Types.OTHER, ColumnMetadataReader.toJdbcType("map"));
        assertEquals(Types.OTHER, ColumnMetadataReader.toJdbcType("struct"));
        assertEquals(Types.OTHER, ColumnMetadataReader.toJdbcType("json"));
    }

    /**
     * The three aggregate sketches. A SELECT of one yields nothing a consumer can use, so the
     * connector refuses the table outright rather than streaming a non-value.
     */
    @Test
    public void testAggregateSketchTypesAreNonExportable() {
        assertTrue(ColumnMetadataReader.isNonExportable("hll"));
        assertTrue(ColumnMetadataReader.isNonExportable("bitmap"));
        assertTrue(ColumnMetadataReader.isNonExportable("percentile"));
    }

    /**
     * Everything else must pass, complex types included: they are exportable, just as text. A
     * guard that also caught ARRAY would reject perfectly capturable tables.
     */
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
