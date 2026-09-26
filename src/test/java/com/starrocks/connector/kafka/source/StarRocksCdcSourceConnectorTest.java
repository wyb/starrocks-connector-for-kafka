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
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.junit.Test;

import java.sql.SQLException;
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
 * Tests for {@link StarRocksCdcSourceConnector}: the fail-fast preflight in {@code start()} (table
 * model, CDC property, columns, bookmark-function probe), the round-robin sharding in {@code
 * taskConfigs()}, {@code exactlyOnceSupport} and the offset vetting in {@code alterOffsets}.
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
            protected CdcClient createClient(StarRocksCdcSourceConfig config) {
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

    /**
     * A view leaves {@code table_model} unset: InformationSchemaDataSource sets it only after
     * casting to OlapTable, and the BE fills the unset thrift field with "", so "no model" arrives
     * as an empty string as often as a null.
     */
    @Test
    public void testPreflightRejectsATableWithNoModel() {
        for (String model : new String[] {null, "", "   "}) {
            FakeCdcClient fake = new FakeCdcClient();
            if (model != null) {
                fake.modelByTable.put("t1", model);
            }
            Map<String, String> props = base();
            props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t1");
            try {
                newConnector(fake).start(props);
                fail("expected ConnectException for table model " + (model == null ? "null" : "'" + model + "'"));
            } catch (ConnectException e) {
                assertTrue("message was: " + e.getMessage(), e.getMessage().contains("db1.t1"));
                assertTrue("the message must say what has no table model; was: " + e.getMessage(),
                        e.getMessage().contains("Views"));
            }
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

    /** A model this connector does not know is refused by name, not captured on a guess. */
    @Test
    public void testPreflightRejectsATableModelItDoesNotKnow() {
        FakeCdcClient fake = new FakeCdcClient();
        fake.modelByTable.put("t1", "XYZ_KEYS");
        Map<String, String> props = base();
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t1");
        try {
            newConnector(fake).start(props);
            fail("expected ConnectException for an unknown table model");
        } catch (ConnectException e) {
            assertTrue("message was: " + e.getMessage(), e.getMessage().contains("db1.t1"));
            assertTrue("the message must name the spelling; was: " + e.getMessage(),
                    e.getMessage().contains("XYZ_KEYS"));
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

    /**
     * StarRocks resolves the collision case-insensitively and renames its own pseudo-column, so a
     * lowercase clash leaves the projection's bare {@code __CHANGE_TYPE__} reading the user's
     * column: deletes would arrive as creates and unrelated rows as deletes, with no error.
     */
    @Test
    public void testPreflightRejectsMetadataColumnCollisionInAnyCase() {
        for (String name : new String[] {
                "__CHANGE_TYPE__", "__change_type__", "__Change_Type__", "__ROW_VERSION__", "__row_version__"}) {
            FakeCdcClient fake = new FakeCdcClient();
            fake.modelByTable.put("t1", "DUP_KEYS");
            fake.colsByTable.put("t1", Arrays.asList(
                    new ColumnMeta("k", "int", "int(11)", 0, false),
                    new ColumnMeta(name, "int", "int(11)", 0, false)));
            Map<String, String> props = base();
            props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t1");
            StarRocksCdcSourceConnector connector = newConnector(fake);
            try {
                connector.start(props);
                fail("expected a collision refusal for column " + name);
            } catch (ConnectException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("collides"));
                assertTrue("the refusal must name the column: " + expected.getMessage(),
                        expected.getMessage().contains(name));
            }
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
                    new ColumnMeta("k", "int", "int(11)", 0, false),
                    new ColumnMeta("sketch", sketch, sketch, 0, true)));
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
     * Complex types are exportable -- natively when the declaration parses, as text otherwise --
     * so the sketch guard must not catch them; a check that also rejected ARRAY would turn away
     * perfectly capturable tables.
     */
    @Test
    public void testPreflightAcceptsComplexTypeColumns() {
        ColumnMeta unparsable = new ColumnMeta("deep", "array", "array<...>", 0, true);
        assertEquals("the case under test is the one the parser refuses",
                ColumnType.Kind.OPAQUE, unparsable.type.kind);
        FakeCdcClient fake = new FakeCdcClient();
        fake.modelByTable.put("t1", "DUP_KEYS");
        fake.colsByTable.put("t1", Arrays.asList(
                new ColumnMeta("k", "int", "int(11)", 0, false),
                new ColumnMeta("a", "array", "array<int>", 0, true),
                new ColumnMeta("m", "map", "map<varchar(10),int>", 0, true),
                new ColumnMeta("s", "struct", "struct<x int>", 0, true),
                new ColumnMeta("j", "json", "json", 0, true),
                // An unrecognized type warns but must not block; "variant" is a real StarRocks type.
                new ColumnMeta("u", "variant", "variant", 0, true),
                // A declaration the parser refuses is carried as text: warns, must not block either.
                unparsable));
        Map<String, String> props = base();
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t1");

        newConnector(fake).start(props);
    }

    @Test
    public void testPreflightAcceptsDupAggAndPkWithCdc() {
        FakeCdcClient fake = new FakeCdcClient();
        fake.modelByTable.put("t1", "DUP_KEYS");
        fake.modelByTable.put("t2", "PRIMARY_KEYS");
        fake.cdcEnabledByTable.put("t2", true);
        fake.modelByTable.put("t3", "AGG_KEYS");
        Map<String, String> props = base();
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t1,t2,t3");
        StarRocksCdcSourceConnector connector = newConnector(fake);

        connector.start(props);

        assertSame(StarRocksCdcSourceTask.class, connector.taskClass());
        assertSame(StarRocksCdcSourceConfig.CONFIG_DEF, connector.config());
    }

    /** A metadata query that fails is wrapped once, with the table's name, and the FE's reason kept as the cause. */
    @Test
    public void testPreflightWrapsAMetadataQueryFailureWithTheTableName() {
        FakeCdcClient fake = new FakeCdcClient();
        fake.tableConfigFailure = new SQLException("table not found: db1.t1");
        Map<String, String> props = base();
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t1");

        try {
            newConnector(fake).start(props);
            fail("expected ConnectException when tables_config cannot be read");
        } catch (ConnectException e) {
            assertTrue("message was: " + e.getMessage(), e.getMessage().contains("db1.t1"));
            assertSame(fake.tableConfigFailure, e.getCause());
        }
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
            assertTrue("the FE's own reason must be in the message, was: " + e.getMessage(),
                    e.getMessage().contains("Set enable_bookmark_meta_functions=true"));
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
     * The probe must never borrow the tasks' holder id: {@code bookmark_create} is idempotent per
     * holder, so a shared holder would hand the probe the table's committed bookmark and the
     * probe's release would delete it.
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
        String taskHolder = new StarRocksCdcSourceConfig(props).holderId();
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

    /** A probe bookmark that cannot be released expires by its own TTL: a warning, not a failed start. */
    @Test
    public void testPreflightSurvivesAFailedProbeRelease() {
        FakeCdcClient fake = new FakeCdcClient();
        fake.modelByTable.put("t1", "DUP_KEYS");
        fake.bookmarkReleaseFailure = new SQLException("synthetic release failure");
        Map<String, String> props = base();
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t1");

        newConnector(fake).start(props);

        assertEquals(1, fake.createdBookmarks.size());
        assertEquals("the release must have been attempted", 1, fake.releasedBookmarks.size());
    }

    /**
     * Streaming changes can be exactly-once: the offset a window commits is the bookmark to resume
     * from, and Connect writes it inside the same transaction as the records.
     */
    @Test
    public void testExactlyOnceIsSupportedWithoutASnapshot() {
        Map<String, String> props = base();
        props.put(StarRocksCdcSourceConfig.SNAPSHOT_MODE, StarRocksCdcSourceConfig.SNAPSHOT_MODE_NO_SNAPSHOT);
        assertEquals(ExactlyOnceSupport.SUPPORTED, new StarRocksCdcSourceConnector().exactlyOnceSupport(props));
    }

    /**
     * A snapshot cannot be: its rows carry {@code snapshot_done=false}, so a restart redoes it whole
     * even after its transaction committed, and every one of those rows is a visible duplicate.
     * {@code initial} is also the default, so an operator who never set the key must not be told
     * exactly-once is available.
     */
    @Test
    public void testExactlyOnceIsRefusedWhenASnapshotIsTaken() {
        Map<String, String> withSnapshot = base();
        withSnapshot.put(StarRocksCdcSourceConfig.SNAPSHOT_MODE, StarRocksCdcSourceConfig.SNAPSHOT_MODE_INITIAL);
        assertEquals(ExactlyOnceSupport.UNSUPPORTED,
                new StarRocksCdcSourceConnector().exactlyOnceSupport(withSnapshot));
        assertEquals("the default is initial, so an unset key must refuse too",
                ExactlyOnceSupport.UNSUPPORTED, new StarRocksCdcSourceConnector().exactlyOnceSupport(base()));
    }

    /**
     * The herder calls this during validation, before the config is known to be complete. Throwing
     * here would surface as a validation error naming neither the key nor the reason.
     */
    @Test
    public void testExactlyOnceSupportSurvivesAnIncompleteConfig() {
        assertEquals(ExactlyOnceSupport.UNSUPPORTED,
                new StarRocksCdcSourceConnector().exactlyOnceSupport(new HashMap<>()));
        Map<String, String> onlyMode = new HashMap<>();
        onlyMode.put(StarRocksCdcSourceConfig.SNAPSHOT_MODE,
                " " + StarRocksCdcSourceConfig.SNAPSHOT_MODE_NO_SNAPSHOT + " ");
        assertEquals(ExactlyOnceSupport.SUPPORTED,
                new StarRocksCdcSourceConnector().exactlyOnceSupport(onlyMode));
        Map<String, String> nullMode = new HashMap<>();
        nullMode.put(StarRocksCdcSourceConfig.SNAPSHOT_MODE, null);
        assertEquals(ExactlyOnceSupport.UNSUPPORTED,
                new StarRocksCdcSourceConnector().exactlyOnceSupport(nullMode));
    }

    private static Map<Map<String, ?>, Map<String, ?>> offsetRequest(Object bookmarkId, Object snapshotDone) {
        Map<String, Object> offset = new HashMap<>();
        offset.put(OffsetState.KEY_BOOKMARK_ID, bookmarkId);
        offset.put(OffsetState.KEY_SNAPSHOT_DONE, snapshotDone);
        Map<Map<String, ?>, Map<String, ?>> request = new HashMap<>();
        request.put(OffsetState.sourcePartition("db1", "orders"), offset);
        return request;
    }

    private void assertOffsetRejected(String why, Map<Map<String, ?>, Map<String, ?>> request, String expected) {
        try {
            new StarRocksCdcSourceConnector().alterOffsets(base(), request);
            fail(why);
        } catch (ConnectException e) {
            assertTrue(e.getMessage(), e.getMessage().contains(expected));
        }
    }

    @Test
    public void testAlterOffsetsAcceptsAWellFormedOffset() {
        assertTrue(new StarRocksCdcSourceConnector().alterOffsets(base(), offsetRequest(11955L, true)));
        assertTrue("an int is what the JSON converter hands back for a small id",
                new StarRocksCdcSourceConnector().alterOffsets(base(), offsetRequest(7, false)));
    }

    /**
     * DELETE /offsets hands over every stored partition mapped to null. Refusing one whose shape
     * this connector no longer writes would leave it with no supported way to be cleared, since a
     * reset is the only way.
     */
    @Test
    public void testAlterOffsetsResetsAPartitionItWouldRefuseToWrite() {
        FakeCdcClient fake = new FakeCdcClient();
        Map<Map<String, ?>, Map<String, ?>> request = new HashMap<>();
        request.put(Collections.singletonMap(OffsetState.KEY_DB, "db1"), null);
        request.put(OffsetState.sourcePartition("db1", "orders"), null);
        assertTrue(newConnector(fake).alterOffsets(base(), request));
        // Only the shape a task writes can have references on the FE; the stale one is just cleared.
        assertEquals(Collections.singletonList("db1.orders:kc:default"), fake.heldBookmarkQueries);
    }

    /**
     * Under no_snapshot a task with no offset resumes from the oldest bookmark its holder still
     * references, so a reset that left them would be undone at the next start. Only the partitions
     * the request maps to null are touched: a PATCH resetting one table must not cost another its position.
     */
    @Test
    public void testAlterOffsetsResetReleasesTheHoldersReferences() {
        FakeCdcClient fake = new FakeCdcClient();
        fake.setHeldBookmarks("orders", 3L, 4L);
        fake.setHeldBookmarks("users", 5L);
        Map<String, String> config = base();
        config.put("name", "c1");
        Map<Map<String, ?>, Map<String, ?>> request = new HashMap<>();
        request.put(OffsetState.sourcePartition("db1", "orders"), null);

        assertTrue(newConnector(fake).alterOffsets(config, request));

        assertEquals(Collections.singletonList("db1.orders:kc:c1"), fake.heldBookmarkQueries);
        assertEquals(Arrays.asList("db1.orders:3:kc:c1", "db1.orders:4:kc:c1"), fake.releasedBookmarks);
    }

    /**
     * A DELETE lists only what Connect has stored and arrives empty when that is nothing -- a
     * no_snapshot table that never reached a durable offset, or a connector re-created under an old
     * name. Then every configured table is the target.
     */
    @Test
    public void testAlterOffsetsEmptyResetReleasesEveryConfiguredTable() {
        FakeCdcClient fake = new FakeCdcClient();
        fake.setHeldBookmarks("orders", 3L);
        Map<String, String> config = base();
        config.put("name", "c1");

        assertTrue(newConnector(fake).alterOffsets(config, new HashMap<>()));

        assertEquals(Arrays.asList("db1.orders:kc:c1", "db1.users:kc:c1"), fake.heldBookmarkQueries);
        assertEquals(Collections.singletonList("db1.orders:3:kc:c1"), fake.releasedBookmarks);
    }

    /** A reset left half done would be undone at the next start, so it fails whole. */
    @Test
    public void testAlterOffsetsResetIsRefusedWhenTheReferencesCannotBeReleased() {
        FakeCdcClient fake = new FakeCdcClient();
        fake.heldBookmarksFailure = new SQLException("FE unreachable");
        Map<Map<String, ?>, Map<String, ?>> request = new HashMap<>();
        request.put(OffsetState.sourcePartition("db1", "orders"), null);
        try {
            newConnector(fake).alterOffsets(base(), request);
            fail("expected the reset to be refused");
        } catch (ConnectException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Reset refused"));
            assertTrue(e.getMessage(), e.getMessage().contains("FE unreachable"));
        }
    }

    /** Releasing the references needs the FE, so a reset of a table partition needs a config that parses. */
    @Test
    public void testAlterOffsetsResetRefusesAnUnparsableConfig() {
        Map<Map<String, ?>, Map<String, ?>> request = new HashMap<>();
        request.put(OffsetState.sourcePartition("db1", "orders"), null);
        try {
            newConnector(new FakeCdcClient()).alterOffsets(new HashMap<>(), request);
            fail("expected ConnectException for a reset with an unparsable config");
        } catch (ConnectException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("does not parse"));
        }
        try {
            newConnector(new FakeCdcClient()).alterOffsets(new HashMap<>(), new HashMap<>());
            fail("expected ConnectException for an empty reset with an unparsable config");
        } catch (ConnectException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("does not parse"));
        }
        // A shape this connector never wrote has no references, so clearing it needs no config.
        Map<Map<String, ?>, Map<String, ?>> stale = new HashMap<>();
        stale.put(Collections.singletonMap(OffsetState.KEY_DB, "db1"), null);
        assertTrue(new StarRocksCdcSourceConnector().alterOffsets(new HashMap<>(), stale));
    }

    /**
     * Connect writes the payload to the offset store whether or not this runs, and the task reads a
     * quoted id back as no position at all. Rejecting it here is the only place an operator learns
     * which value was wrong.
     */
    @Test
    public void testAlterOffsetsRejectsAQuotedBookmarkId() {
        assertOffsetRejected("a quoted id must not reach the offset store",
                offsetRequest("11955", true), "String 11955");
    }

    @Test
    public void testAlterOffsetsRejectsANegativeOrMissingBookmarkId() {
        assertOffsetRejected("-1 is the no-position sentinel", offsetRequest(-1L, true), "non-negative number");
        assertOffsetRejected("a missing id names no position", offsetRequest(null, true), "null");
    }

    @Test
    public void testAlterOffsetsRejectsANonBooleanSnapshotDone() {
        assertOffsetRejected("a quoted boolean reads as not-done", offsetRequest(11955L, "true"), "String true");
        assertOffsetRejected("snapshot_done is not optional here", offsetRequest(11955L, null),
                OffsetState.KEY_SNAPSHOT_DONE);
    }

    @Test
    public void testAlterOffsetsRejectsAMalformedPartition() {
        Map<Map<String, ?>, Map<String, ?>> request = new HashMap<>();
        Map<String, Object> offset = new HashMap<>();
        offset.put(OffsetState.KEY_BOOKMARK_ID, 11955L);
        offset.put(OffsetState.KEY_SNAPSHOT_DONE, true);
        request.put(Collections.singletonMap(OffsetState.KEY_DB, "db1"), offset);
        assertOffsetRejected("a partition naming no table matches nothing the task reads",
                request, OffsetState.KEY_TABLE);

        Map<Map<String, ?>, Map<String, ?>> extraKey = new HashMap<>();
        Map<String, Object> partition = new HashMap<>(OffsetState.sourcePartition("db1", "orders"));
        partition.put("shard", 1);
        extraKey.put(partition, offset);
        assertOffsetRejected("a partition with an extra key matches nothing the task reads", extraKey, "exactly");

        Map<Map<String, ?>, Map<String, ?>> nullPartition = new HashMap<>();
        nullPartition.put(null, offset);
        assertOffsetRejected("Connect passes a request's \"partition\": null through as is", nullPartition,
                OffsetState.KEY_DB);
    }

    /** A well-formed partition for a table the config does not list would land and never be read. */
    @Test
    public void testAlterOffsetsRejectsAPartitionNoTaskWouldRead() {
        for (String[] db_table : new String[][] {{"db1", "order"}, {"db2", "orders"}}) {
            Map<Map<String, ?>, Map<String, ?>> request = new HashMap<>();
            Map<String, Object> offset = new HashMap<>();
            offset.put(OffsetState.KEY_BOOKMARK_ID, 11955L);
            offset.put(OffsetState.KEY_SNAPSHOT_DONE, true);
            request.put(OffsetState.sourcePartition(db_table[0], db_table[1]), offset);
            assertOffsetRejected("no task reads " + db_table[0] + "." + db_table[1], request,
                    StarRocksCdcSourceConfig.TABLE_NAMES + "=[orders, users]");
        }
    }
}
