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

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class StarRocksCdcSourceConfigTest {
    private Map<String, String> base() {
        Map<String, String> m = new HashMap<>();
        m.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:mysql://fe1:9030,fe2:9030");
        m.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db1");
        m.put(StarRocksCdcSourceConfig.USERNAME, "root");
        m.put(StarRocksCdcSourceConfig.PASSWORD, "");
        m.put(StarRocksCdcSourceConfig.TABLE_NAMES, "orders, users");
        return m;
    }

    @Test
    public void testDefaultsApplied() {
        StarRocksCdcSourceConfig c = new StarRocksCdcSourceConfig(base());
        assertEquals("sr", c.topicPrefix());
        assertEquals("initial", c.snapshotMode());
        assertEquals(5000L, c.pollIntervalMs());
        assertEquals(604800000L, c.bookmarkTtlMs());
        assertEquals("fail", c.nonTrackablePolicy());
        assertFalse(c.netChanges());
        assertFalse(c.tombstonesOnDelete());
    }

    @Test
    public void testTableNamesTrimmed() {
        assertEquals(Arrays.asList("orders", "users"), new StarRocksCdcSourceConfig(base()).tableNames());
    }

    @Test
    public void testTopicForUsesMapThenPrefix() {
        Map<String, String> m = base();
        m.put(StarRocksCdcSourceConfig.TABLE2TOPIC_MAP, "orders:my_topic");
        StarRocksCdcSourceConfig c = new StarRocksCdcSourceConfig(m);
        assertEquals("my_topic", c.topicFor("orders"));
        assertEquals("sr.db1.users", c.topicFor("users"));
    }

    @Test(expected = ConfigException.class)
    public void testMissingRequiredRejected() {
        Map<String, String> m = base();
        m.remove(StarRocksCdcSourceConfig.TABLE_NAMES);
        new StarRocksCdcSourceConfig(m);
    }

    @Test(expected = ConfigException.class)
    public void testBadSnapshotModeRejected() {
        Map<String, String> m = base();
        m.put(StarRocksCdcSourceConfig.SNAPSHOT_MODE, "bogus");
        new StarRocksCdcSourceConfig(m);
    }

    @Test(expected = ConfigException.class)
    public void testBadTable2TopicEntryRejected() {
        Map<String, String> m = base();
        m.put(StarRocksCdcSourceConfig.TABLE2TOPIC_MAP, "orders");
        new StarRocksCdcSourceConfig(m);
    }
}
