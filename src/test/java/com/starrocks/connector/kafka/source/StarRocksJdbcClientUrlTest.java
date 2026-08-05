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

import java.lang.reflect.Method;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * URL fan-out is the whole transport switch: {@code starrocks.jdbc.url} carries no separate
 * transport key, so whichever scheme it names decides which driver runs and whether the
 * MySQL-only streaming knobs apply. These cases pin that behaviour, since the rest of the client
 * has no unit tests (its connection behaviour is covered by the integration smoke test instead).
 */
public class StarRocksJdbcClientUrlTest {

    @SuppressWarnings("unchecked")
    private List<String> parseUrls(String url) throws Exception {
        Method m = StarRocksJdbcClient.class.getDeclaredMethod("parseUrls", String.class);
        m.setAccessible(true);
        return (List<String>) m.invoke(null, url);
    }

    /** jdbc:mysql is rewritten to jdbc:mariadb, because that is the driver actually bundled. */
    @Test
    public void testMysqlHostsFanOutAndAreRewrittenToMariadb() throws Exception {
        assertEquals(Arrays.asList("jdbc:mariadb://fe1:9030", "jdbc:mariadb://fe2:9030"),
                parseUrls("jdbc:mysql://fe1:9030,fe2:9030"));
    }

    /** The Arrow scheme must survive untouched -- rewriting it would hand it to the wrong driver. */
    @Test
    public void testArrowFlightSchemeIsNotRewritten() throws Exception {
        assertEquals(Collections.singletonList("jdbc:arrow-flight-sql://fe1:9408"),
                parseUrls("jdbc:arrow-flight-sql://fe1:9408"));
    }

    /**
     * A plaintext Flight server needs {@code useEncryption=false}, so the query string has to be
     * replicated onto every host the list fans out to -- not left attached to the last one.
     */
    @Test
    public void testArrowFlightQueryStringIsCopiedToEveryHost() throws Exception {
        assertEquals(
                Arrays.asList(
                        "jdbc:arrow-flight-sql://fe1:9408?useEncryption=false&useSSL=false",
                        "jdbc:arrow-flight-sql://fe2:9408?useEncryption=false&useSSL=false"),
                parseUrls("jdbc:arrow-flight-sql://fe1:9408,fe2:9408?useEncryption=false&useSSL=false"));
    }

    /**
     * Every spelling the BE's {@code SchemaColumnsScanner::to_mysql_data_type_string} can put in
     * {@code information_schema.columns.DATA_TYPE}. That switch is the closed set this mapping
     * answers to, so the cases here are a transcription of it -- if StarRocks adds a type there,
     * one of these will be the thing that notices.
     */
    @Test
    public void testEveryStarRocksDataTypeMapsToAJdbcType() {
        assertEquals(Types.TINYINT, StarRocksJdbcClient.toJdbcType("tinyint"));
        assertEquals(Types.SMALLINT, StarRocksJdbcClient.toJdbcType("smallint"));
        assertEquals(Types.INTEGER, StarRocksJdbcClient.toJdbcType("int"));
        assertEquals(Types.BIGINT, StarRocksJdbcClient.toJdbcType("bigint"));
        assertEquals(Types.REAL, StarRocksJdbcClient.toJdbcType("float"));
        assertEquals(Types.DOUBLE, StarRocksJdbcClient.toJdbcType("double"));
        assertEquals(Types.DECIMAL, StarRocksJdbcClient.toJdbcType("decimal"));
        assertEquals(Types.CHAR, StarRocksJdbcClient.toJdbcType("char"));
        assertEquals(Types.VARCHAR, StarRocksJdbcClient.toJdbcType("varchar"));
        assertEquals(Types.DATE, StarRocksJdbcClient.toJdbcType("date"));
        assertEquals(Types.TIMESTAMP, StarRocksJdbcClient.toJdbcType("datetime"));
        assertEquals(Types.BINARY, StarRocksJdbcClient.toJdbcType("binary"));
        assertEquals(Types.VARBINARY, StarRocksJdbcClient.toJdbcType("varbinary"));
    }

    /**
     * LARGEINT is 128-bit and arrives spelled "bigint unsigned". Mapping it to BIGINT would
     * silently truncate every value that does not fit a long, so it goes to OTHER and is carried
     * as text instead.
     */
    @Test
    public void testLargeIntIsNotMappedToBigint() {
        assertEquals(Types.OTHER, StarRocksJdbcClient.toJdbcType("bigint unsigned"));
    }

    /** Opaque and unknown types are read as text rather than guessed at. */
    @Test
    public void testOpaqueAndUnknownTypesFallBackToOther() {
        for (String t : new String[] {"hll", "bitmap", "percentile", "json", "unknown", "", null}) {
            assertEquals("expected OTHER for " + t, Types.OTHER, StarRocksJdbcClient.toJdbcType(t));
        }
    }

    /** DATA_TYPE casing is the server's choice, not a contract. */
    @Test
    public void testDataTypeMatchingIgnoresCaseAndPadding() {
        assertEquals(Types.INTEGER, StarRocksJdbcClient.toJdbcType("  INT "));
        assertEquals(Types.TIMESTAMP, StarRocksJdbcClient.toJdbcType("DateTime"));
        assertEquals(Types.OTHER, StarRocksJdbcClient.toJdbcType("BIGINT UNSIGNED"));
    }
}
