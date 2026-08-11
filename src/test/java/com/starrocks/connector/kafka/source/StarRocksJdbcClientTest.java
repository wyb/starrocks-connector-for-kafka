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

import java.sql.SQLException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Reading the CDC property out of {@code information_schema.tables_config.PROPERTIES}. Everything
 * else on {@link StarRocksJdbcClient} needs a server and is covered by the integration smoke test.
 *
 * <p>FE writes that column as {@code new Gson().toJson(table.getProperties())} over a
 * {@code Map<String, String>}, so values are JSON strings, not JSON booleans. The samples below
 * keep that shape.
 */
public class StarRocksJdbcClientTest {

    private static final String REAL_PK_TABLE_PROPERTIES =
            "{\"compression\":\"LZ4\",\"datacache.enable\":\"true\","
                    + "\"enable_change_data_capture\":\"%s\",\"enable_persistent_index\":\"true\","
                    + "\"replication_num\":\"1\",\"storage_volume\":\"builtin_storage_volume\"}";

    private static boolean enabledFor(String value) throws SQLException {
        return StarRocksJdbcClient.changeDataCaptureEnabled(
                "db1", "t1", String.format(REAL_PK_TABLE_PROPERTIES, value));
    }

    @Test
    public void testPropertyIsReadFromTheJsonMap() throws Exception {
        assertTrue(enabledFor("true"));
        assertFalse(enabledFor("false"));
    }

    /** The property's value is FE's Boolean.toString(); its casing is not a contract. */
    @Test
    public void testValueCasingIsIgnored() throws Exception {
        assertTrue(enabledFor("TRUE"));
        assertTrue(enabledFor("True"));
    }

    /** FE omits the key entirely unless the table is a cloud-native primary-key table. */
    @Test
    public void testMissingKeyIsNotEnabled() throws Exception {
        assertFalse(StarRocksJdbcClient.changeDataCaptureEnabled(
                "db1", "t1", "{\"compression\":\"LZ4\",\"replication_num\":\"1\"}"));
        assertFalse(StarRocksJdbcClient.changeDataCaptureEnabled("db1", "t1", "{}"));
    }

    /**
     * A value that cannot be read must not answer "false": preflight would then tell an operator to
     * run an ALTER enabling a property that is already on. This is the failure the SHOW CREATE TABLE
     * substring match could produce silently, and the reason for moving to a parsed format.
     */
    @Test
    public void testUnreadablePropertiesThrowRatherThanReportDisabled() {
        for (String broken : new String[] {null, "", "   ", "not json", "{\"a\":", "[]", "\"a\"", "42"}) {
            try {
                StarRocksJdbcClient.changeDataCaptureEnabled("db1", "t1", broken);
                fail("expected a SQLException for PROPERTIES=" + broken);
            } catch (SQLException expected) {
                assertTrue("message should name the table, was: " + expected.getMessage(),
                        expected.getMessage().contains("db1.t1"));
            }
        }
    }
}
