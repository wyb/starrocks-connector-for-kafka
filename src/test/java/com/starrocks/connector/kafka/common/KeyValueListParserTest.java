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

package com.starrocks.connector.kafka.common;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The sink and the CDC source both route data through this parser, so its failure behaviour is
 * shared contract rather than an implementation detail: every malformed value must raise, never
 * return a partial or empty map that a caller could mistake for "no mapping configured".
 */
public class KeyValueListParserTest {

    private static final String KEY = "starrocks.topic2table.map";

    private Map<String, String> parse(String raw) {
        return KeyValueListParser.parse(KEY, raw);
    }

    private void assertRejected(String raw, String expectedFragment) {
        try {
            parse(raw);
            fail("expected ConfigException for " + raw);
        } catch (ConfigException e) {
            assertTrue("message should mention " + expectedFragment + " but was: " + e.getMessage(),
                    e.getMessage().contains(expectedFragment));
        }
    }

    @Test
    public void testNullAndBlankYieldEmptyMap() {
        assertTrue(parse(null).isEmpty());
        assertTrue(parse("").isEmpty());
        assertTrue(parse("   ").isEmpty());
    }

    @Test
    public void testPairsAreParsedInOrder() {
        Map<String, String> result = parse("t1:tbl1,t2:tbl2,t3:tbl3");
        assertEquals(3, result.size());
        assertEquals("tbl1", result.get("t1"));
        assertEquals("tbl3", result.get("t3"));
        // Order matters only for error messages and test determinism, but a HashMap here would make
        // both arbitrary, so the insertion order is pinned.
        assertEquals(java.util.Arrays.asList("t1", "t2", "t3"), new ArrayList<>(result.keySet()));
    }

    @Test
    public void testWhitespaceAroundEntriesAndSidesIsTrimmed() {
        Map<String, String> result = parse("  t1 : tbl1 ,\t t2:tbl2  ");
        assertEquals("tbl1", result.get("t1"));
        assertEquals("tbl2", result.get("t2"));
    }

    @Test
    public void testEmptyEntriesBetweenCommasAreSkipped() {
        assertEquals(2, parse("t1:tbl1,,t2:tbl2,").size());
    }

    /** Split is on the FIRST colon, so a value may contain one. */
    @Test
    public void testValueMayContainAColon() {
        assertEquals("db:tbl", parse("t1:db:tbl").get("t1"));
    }

    @Test
    public void testEntryWithoutSeparatorIsRejected() {
        assertRejected("t1:tbl1,broken", "missing a ':' separator");
    }

    @Test
    public void testEmptyKeyOrValueIsRejected() {
        assertRejected(":tbl1", "non-empty");
        assertRejected("t1:", "non-empty");
        assertRejected("t1: ", "non-empty");
    }

    /**
     * A repeated key is always a mistake, and silently keeping one of the two mappings would leave
     * which one takes effect up to iteration order.
     */
    @Test
    public void testDuplicateKeyIsRejected() {
        assertRejected("t1:tbl1,t1:tbl2", "mapped more than once");
    }

    /** The offending value has to reach the operator; a bare "invalid config" is not actionable. */
    @Test
    public void testExceptionNamesTheConfigKeyAndTheOffendingEntry() {
        try {
            parse("t1:tbl1,oops");
            fail("expected ConfigException");
        } catch (ConfigException e) {
            assertTrue(e.getMessage().contains(KEY));
            assertTrue(e.getMessage().contains("oops"));
        }
    }
}
