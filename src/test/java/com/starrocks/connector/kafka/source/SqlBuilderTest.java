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
import java.util.Arrays;

import static org.junit.Assert.*;

public class SqlBuilderTest {

    @Test
    public void testChangesSqlHasMandatoryOrderBy() {
        String sql = SqlBuilder.changesSql("db1", "orders", Arrays.asList("k", "v"), 11952L, 11955L);
        assertEquals("SELECT `k`,`v`,__CHANGE_TYPE__,__ROW_VERSION__ FROM `db1`.`orders` " +
                "[_CHANGES_11952_11955_] ORDER BY __ROW_VERSION__, __CHANGE_TYPE__ DESC", sql);
    }

    @Test
    public void testSnapshotSqlPinsBookmark() {
        assertEquals("SELECT `k`,`v` FROM `db1`.`orders` [_BOOKMARK_7_]",
                SqlBuilder.snapshotSql("db1", "orders", Arrays.asList("k", "v"), 7L));
    }

    @Test
    public void testBookmarkFunctionSqlQuotesArgs() {
        assertEquals("SELECT bookmark_create('db1','orders','kc:c1','604800000')",
                SqlBuilder.bookmarkCreateSql("db1", "orders", "kc:c1", 604800000L));
        assertEquals("SELECT bookmark_release('db1','orders','7','kc:c1')",
                SqlBuilder.bookmarkReleaseSql("db1", "orders", 7L, "kc:c1"));
    }

    @Test
    public void testIdentifierBacktickEscaping() {
        assertEquals("`a``b`", SqlBuilder.quoteId("a`b"));
        assertEquals("'it\\'s \\\\here'", SqlBuilder.quoteStr("it's \\here"));
    }

    @Test
    public void testProbeAndMetadataSql() {
        assertEquals("SELECT * FROM `db1`.`t1` LIMIT 0", SqlBuilder.columnsProbeSql("db1", "t1"));
        assertEquals("SELECT TABLE_MODEL, PRIMARY_KEY FROM information_schema.tables_config " +
                "WHERE TABLE_SCHEMA = 'db1' AND TABLE_NAME = 't1'", SqlBuilder.tableConfigSql("db1", "t1"));
        assertEquals("SHOW CREATE TABLE `db1`.`t1`", SqlBuilder.showCreateTableSql("db1", "t1"));
    }

    @Test
    public void testNonTrackableClassification() {
        assertNotNull(NonTrackableException.classify(new SQLException(
                "CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CHANGES window on tablet 1 spans version 3 ...")));
        assertNotNull(NonTrackableException.classify(new SQLException("Bookmark 11952 not found")));
        // FE planning-time SemanticExceptions (partition dropped/rewritten/resharded, or a
        // partition/tablet hint that no longer resolves) never contain "CDC-ERROR-" or
        // "bookmark"+"not found", but always contain "not trackable".
        assertNotNull(NonTrackableException.classify(new SQLException(
                "CHANGES from bookmark 5 to 9 on table 't' not trackable: physical partition 100 dropped")));
        assertNotNull(NonTrackableException.classify(new SQLException(
                "CHANGES on table 't' not trackable: partition p1 not present in the changeset")));
        assertNull(NonTrackableException.classify(new SQLException("Connection refused")));
        assertNull(NonTrackableException.classify(new SQLException((String) null)));
    }
}
