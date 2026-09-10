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

    /** 0 hot-loops the FE leader and a negative reaches Thread.sleep; neither may be configurable. */
    @Test
    public void testPollIntervalMustBePositive() {
        for (String bad : new String[] {"0", "-1"}) {
            Map<String, String> m = base();
            m.put(StarRocksCdcSourceConfig.POLL_INTERVAL_MS, bad);
            try {
                new StarRocksCdcSourceConfig(m);
                fail("source.poll.interval.ms=" + bad + " must be rejected at startup");
            } catch (ConfigException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains(
                        StarRocksCdcSourceConfig.POLL_INTERVAL_MS));
            }
        }
    }

    @Test
    public void testDefaultsApplied() {
        StarRocksCdcSourceConfig c = new StarRocksCdcSourceConfig(base());
        assertEquals("sr", c.topicPrefix());
        assertEquals("initial", c.snapshotMode());
        assertEquals(5000L, c.pollIntervalMs());
        assertEquals(604800000L, c.bookmarkTtlMs());
        assertEquals("fail", c.nonTrackablePolicy());
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

    /** Passes ConfigDef's required check, then names nothing: a healthy, idle, silent connector. */
    @Test
    public void testTableListThatNamesNothingRejected() {
        for (String value : new String[] {",", " ", " , , "}) {
            Map<String, String> m = base();
            m.put(StarRocksCdcSourceConfig.TABLE_NAMES, value);
            try {
                new StarRocksCdcSourceConfig(m);
                fail("expected a ConfigException for " + StarRocksCdcSourceConfig.TABLE_NAMES + "='" + value + "'");
            } catch (ConfigException expected) {
                assertTrue("message should name the offending config, was: " + expected.getMessage(),
                        expected.getMessage().contains(StarRocksCdcSourceConfig.TABLE_NAMES));
            }
        }
    }

    /**
     * Two TableStates for one table share a holder, so bookmark_create hands both the same window
     * and every row ships twice; split across tasks, one task's commit() releases the bookmark the
     * other is still using as its base.
     */
    @Test
    public void testDuplicateTableNameRejected() {
        for (String value : new String[] {"orders,orders", "orders, orders ,items", "items,orders,orders"}) {
            Map<String, String> m = base();
            m.put(StarRocksCdcSourceConfig.TABLE_NAMES, value);
            try {
                new StarRocksCdcSourceConfig(m);
                fail("expected a ConfigException for " + StarRocksCdcSourceConfig.TABLE_NAMES + "='" + value + "'");
            } catch (ConfigException expected) {
                assertTrue("message should name the repeated table, was: " + expected.getMessage(),
                        expected.getMessage().contains("orders"));
            }
        }
    }

    /** Only exact repeats are a typo; a case-different name may be a different table server-side. */
    @Test
    public void testCaseDifferingTableNamesAreNotTreatedAsDuplicates() {
        Map<String, String> m = base();
        m.put(StarRocksCdcSourceConfig.TABLE_NAMES, "orders,Orders");
        assertEquals(Arrays.asList("orders", "Orders"), new StarRocksCdcSourceConfig(m).tableNames());
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

    /**
     * no_snapshot + resnapshot is silently lossy, not self-healing: the resnapshot path pins a
     * fresh bookmark at the current version and, with snapshots off, reads nothing -- dropping every
     * change between the unusable base and that new bookmark. It must be rejected up front.
     */
    @Test
    public void testNoSnapshotWithResnapshotPolicyRejected() {
        Map<String, String> m = base();
        m.put(StarRocksCdcSourceConfig.SNAPSHOT_MODE, "no_snapshot");
        m.put(StarRocksCdcSourceConfig.NONTRACKABLE_POLICY, "resnapshot");

        try {
            new StarRocksCdcSourceConfig(m);
            fail("expected ConfigException for no_snapshot + resnapshot");
        } catch (ConfigException e) {
            // Literals, not the constants the message is built from: this is the text an operator
            // reads, and asserting it against its own source would pass however it drifted.
            assertTrue("message was: " + e.getMessage(),
                    e.getMessage().contains("source.nontrackable.policy=resnapshot cannot be combined with"
                            + " source.snapshot.mode=no_snapshot"));
            assertTrue("the message must name both ways out; was: " + e.getMessage(),
                    e.getMessage().contains("Use source.snapshot.mode=initial, or"
                            + " source.nontrackable.policy=fail."));
        }
    }

    @Test
    public void testNoSnapshotWithFailPolicyAccepted() {
        Map<String, String> m = base();
        m.put(StarRocksCdcSourceConfig.SNAPSHOT_MODE, "no_snapshot");
        StarRocksCdcSourceConfig c = new StarRocksCdcSourceConfig(m);
        assertEquals("no_snapshot", c.snapshotMode());
        assertEquals("fail", c.nonTrackablePolicy());
    }
}
