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

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link StarRocksCdcSourceConnector}: the fail-fast preflight checks run from {@code
 * start()} (unique-key rejection, PK-without-CDC rejection, metadata pseudo-column collisions, and
 * the bookmark-meta-function probe) and the round-robin table sharding performed by {@code
 * taskConfigs()}.
 */
public class StarRocksCdcSourceConnectorTest {

    private Map<String, String> base() {
        Map<String, String> m = new HashMap<>();
        m.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:mysql://fe1:9030,fe2:9030");
        m.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db1");
        m.put(StarRocksCdcSourceConfig.USERNAME, "root");
        m.put(StarRocksCdcSourceConfig.PASSWORD, "");
        m.put(StarRocksCdcSourceConfig.TABLE_NAMES, "orders, users");
        return m;
    }

    private StarRocksCdcSourceConnector newConnector(final FakeCdcClient fake) {
        return new StarRocksCdcSourceConnector() {
            @Override
            protected CdcClient createClient(StarRocksCdcSourceConfig cfg) {
                return fake;
            }
        };
    }

    @Test
    public void testTaskConfigsRoundRobin() {
        FakeCdcClient fake = new FakeCdcClient();
        for (String t : Arrays.asList("t1", "t2", "t3", "t4")) {
            fake.modelByTable.put(t, "DUP_KEYS");
        }
        Map<String, String> props = base();
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t1,t2,t3,t4");
        StarRocksCdcSourceConnector connector = newConnector(fake);
        connector.start(props);

        List<Map<String, String>> configs2 = connector.taskConfigs(2);
        assertEquals(2, configs2.size());
        assertEquals("t1,t3", configs2.get(0).get(StarRocksCdcSourceConfig.TASK_TABLES));
        assertEquals("t2,t4", configs2.get(1).get(StarRocksCdcSourceConfig.TASK_TABLES));
        for (Map<String, String> taskProps : configs2) {
            // Every original key must survive into each task's config.
            assertEquals("root", taskProps.get(StarRocksCdcSourceConfig.USERNAME));
            assertEquals("db1", taskProps.get(StarRocksCdcSourceConfig.DATABASE_NAME));
            assertEquals("t1,t2,t3,t4", taskProps.get(StarRocksCdcSourceConfig.TABLE_NAMES));
        }

        List<Map<String, String>> configs8 = connector.taskConfigs(8);
        assertEquals(4, configs8.size());
        for (Map<String, String> taskProps : configs8) {
            assertEquals(1, taskProps.get(StarRocksCdcSourceConfig.TASK_TABLES).split(",").length);
        }
    }

    @Test
    public void testPreflightRejectsUniqueKeyTable() {
        // Real runtime value: FE's KeysType.UNIQUE_KEYS enum constant rendered via toString()
        // (see InformationSchemaDataSource), which some docs mislabel as "UNQ_KEYS".
        FakeCdcClient fakeRealValue = new FakeCdcClient();
        fakeRealValue.modelByTable.put("t1", "UNIQUE_KEYS");
        Map<String, String> props = base();
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t1");
        StarRocksCdcSourceConnector connectorRealValue = newConnector(fakeRealValue);

        try {
            connectorRealValue.start(props);
            fail("expected ConnectException for a UNIQUE KEY table (real FE value UNIQUE_KEYS)");
        } catch (ConnectException e) {
            assertTrue("message was: " + e.getMessage(), e.getMessage().contains("db1.t1"));
        }

        // Docs-described spelling; must also be rejected in case any deployment emits it.
        FakeCdcClient fakeDocsValue = new FakeCdcClient();
        fakeDocsValue.modelByTable.put("t1", "UNQ_KEYS");
        StarRocksCdcSourceConnector connectorDocsValue = newConnector(fakeDocsValue);

        try {
            connectorDocsValue.start(props);
            fail("expected ConnectException for a UNIQUE KEY table (docs value UNQ_KEYS)");
        } catch (ConnectException e) {
            assertTrue("message was: " + e.getMessage(), e.getMessage().contains("db1.t1"));
        }
    }

    @Test
    public void testPreflightRejectsPkTableWithoutCdc() {
        FakeCdcClient fake = new FakeCdcClient();
        fake.modelByTable.put("t1", "PRIMARY_KEYS");
        fake.cdcEnabledByTable.put("t1", false);
        Map<String, String> props = base();
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t1");
        StarRocksCdcSourceConnector connector = newConnector(fake);

        try {
            connector.start(props);
            fail("expected ConnectException for a PK table without CDC enabled");
        } catch (ConnectException e) {
            assertTrue("message was: " + e.getMessage(),
                    e.getMessage().contains("ALTER TABLE db1.t1 SET (\"enable_change_data_capture\" = \"true\")"));
        }
    }

    @Test
    public void testPreflightRejectsMetadataColumnCollision() {
        FakeCdcClient fake = new FakeCdcClient();
        fake.modelByTable.put("t1", "DUP_KEYS");
        fake.colsByTable.put("t1", Arrays.asList(
                new ColumnMeta("k", Types.INTEGER, 10, 0, false),
                new ColumnMeta("__CHANGE_TYPE__", Types.INTEGER, 10, 0, false)));
        Map<String, String> props = base();
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t1");
        StarRocksCdcSourceConnector connector = newConnector(fake);

        try {
            connector.start(props);
            fail("expected ConnectException for a metadata pseudo-column collision");
        } catch (ConnectException e) {
            assertTrue("message was: " + e.getMessage(), e.getMessage().contains("__CHANGE_TYPE__"));
        }
    }

    /**
     * HLL, BITMAP and PERCENTILE hold aggregate sketches. Selecting one yields nothing a consumer
     * can interpret, so the table is refused at startup -- otherwise the connector runs happily and
     * streams that non-value to Kafka indefinitely, which from the outside is indistinguishable
     * from working.
     */
    @Test
    public void testPreflightRejectsAggregateSketchColumns() {
        for (String sketch : new String[] {"hll", "bitmap", "percentile"}) {
            FakeCdcClient fake = new FakeCdcClient();
            fake.modelByTable.put("t1", "DUP_KEYS");
            fake.colsByTable.put("t1", Arrays.asList(
                    new ColumnMeta("k", Types.INTEGER, 10, 0, false, "int", "int(11)"),
                    new ColumnMeta("sketch", Types.OTHER, 0, 0, true, sketch, sketch)));
            Map<String, String> props = base();
            props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t1");

            try {
                newConnector(fake).start(props);
                fail("expected ConnectException for a " + sketch + " column");
            } catch (ConnectException e) {
                assertTrue("message should name the column, was: " + e.getMessage(),
                        e.getMessage().contains("sketch"));
                assertTrue("message should name the type, was: " + e.getMessage(),
                        e.getMessage().contains(sketch));
            }
        }
    }

    /**
     * Complex types are exportable -- as text, for now -- so the same guard must not catch them.
     * A check that also rejected ARRAY would turn away perfectly capturable tables.
     */
    @Test
    public void testPreflightAcceptsComplexTypeColumns() {
        FakeCdcClient fake = new FakeCdcClient();
        fake.modelByTable.put("t1", "DUP_KEYS");
        fake.colsByTable.put("t1", Arrays.asList(
                new ColumnMeta("k", Types.INTEGER, 10, 0, false, "int", "int(11)"),
                new ColumnMeta("a", Types.OTHER, 0, 0, true, "array", "array<int>"),
                new ColumnMeta("m", Types.OTHER, 0, 0, true, "map", "map<varchar(10),int>"),
                new ColumnMeta("s", Types.OTHER, 0, 0, true, "struct", "struct<x int>"),
                new ColumnMeta("j", Types.OTHER, 0, 0, true, "json", "json"),
                // An unrecognised type warns but must not block: the value is still preserved.
                new ColumnMeta("u", Types.OTHER, 0, 0, true, "unknown", "somenewtype<int>")));
        Map<String, String> props = base();
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t1");

        newConnector(fake).start(props);
    }

    @Test
    public void testPreflightAcceptsDupAndPkWithCdc() {
        FakeCdcClient fake = new FakeCdcClient();
        fake.modelByTable.put("t1", "DUP_KEYS");
        fake.modelByTable.put("t2", "PRIMARY_KEYS");
        fake.cdcEnabledByTable.put("t2", true);
        Map<String, String> props = base();
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t1,t2");
        StarRocksCdcSourceConnector connector = newConnector(fake);

        connector.start(props);

        assertSame(StarRocksCdcSourceTask.class, connector.taskClass());
        assertSame(StarRocksCdcSourceConfig.CONFIG_DEF, connector.config());
    }

    /**
     * The bookmark-function probe is the check that catches the likeliest first-run blocker:
     * {@code Config.enable_bookmark_meta_functions} defaults to false in StarRocks, and every
     * bookmark call then fails. Preflight must surface the remedy rather than let tasks start and
     * bury it inside a per-table poll failure.
     */
    @Test
    public void testPreflightRejectsDisabledBookmarkFunctions() {
        FakeCdcClient fake = new FakeCdcClient();
        fake.modelByTable.put("t1", "DUP_KEYS");
        fake.bookmarkCreateFailure = new SQLException(
                "Bookmark meta functions are disabled. Set enable_bookmark_meta_functions=true");
        Map<String, String> props = base();
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t1");
        StarRocksCdcSourceConnector connector = newConnector(fake);

        try {
            connector.start(props);
            fail("expected ConnectException when bookmark meta functions are disabled");
        } catch (ConnectException e) {
            assertTrue("message was: " + e.getMessage(), e.getMessage().contains(
                    "ADMIN SET FRONTEND CONFIG (\"enable_bookmark_meta_functions\" = \"true\")"));
            assertTrue("message was: " + e.getMessage(), e.getMessage().contains("db1.t1"));
        }
    }

    @Test
    public void testPreflightProbesAndReleasesOneBookmark() {
        FakeCdcClient fake = new FakeCdcClient();
        fake.modelByTable.put("t1", "DUP_KEYS");
        fake.modelByTable.put("t2", "DUP_KEYS");
        Map<String, String> props = base();
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t1,t2");
        props.put("name", "c1");

        newConnector(fake).start(props);

        // One throwaway bookmark on the first table only, released again straight away.
        assertEquals(Collections.singletonList("db1.t1:kc:c1:preflight"), fake.createdBookmarks);
        assertEquals(Collections.singletonList("db1.t1:1:kc:c1:preflight"), fake.releasedBookmarks);
    }

    /**
     * The probe must never borrow the tasks' holder id. {@code bookmark_create} is idempotent per
     * holder: on a restart where the first captured table is idle, a probe running under the task
     * holder would be handed back the table's still-held committed bookmark, and the probe's
     * release would then drop the task's own reference to it and delete the very bookmark the
     * durable offset names -- a task that dies (or silently re-reads the whole table) on every
     * restart. A distinct holder makes create acquire a second, independent reference and makes
     * the release drop only that one.
     */
    @Test
    public void testPreflightProbeUsesDistinctHolder() {
        FakeCdcClient fake = new FakeCdcClient();
        fake.modelByTable.put("t1", "DUP_KEYS");
        Map<String, String> props = base();
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t1");
        props.put("name", "c1");

        newConnector(fake).start(props);

        // The holder StarRocksCdcSourceTask#start would compute for this same connector name.
        String taskHolder = new StarRocksCdcSourceConfig(props).holderId("c1");
        assertEquals("kc:c1", taskHolder);

        assertEquals(1, fake.createHolders.size());
        assertEquals(1, fake.releaseHolders.size());
        String probeHolder = fake.createHolders.get(0);
        assertEquals("the probe must release under the very holder it created with",
                probeHolder, fake.releaseHolders.get(0));
        assertNotEquals("the preflight probe must not share the task's holder id",
                taskHolder, probeHolder);
        assertTrue("the probe holder should still be derived from the task holder, was: " + probeHolder,
                probeHolder.startsWith(taskHolder));
    }
}
