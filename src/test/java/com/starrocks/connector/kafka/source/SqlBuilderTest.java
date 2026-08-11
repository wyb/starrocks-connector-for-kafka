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

    /**
     * The Arrow Flight transport's only route to column metadata.
     *
     * <p>ORDINAL_POSITION ordering is load-bearing, not cosmetic: the column list it produces is
     * matched positionally against the projected result set by {@code RowExtractor}, so any other
     * order silently reads every value into the wrong field.
     */
    @Test
    public void testColumnsMetadataSqlSelectsOrderedByOrdinalPosition() {
        assertEquals("SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_SIZE, DECIMAL_DIGITS"
                        + " FROM information_schema.columns WHERE TABLE_SCHEMA = 'db1'"
                        + " AND TABLE_NAME = 't1' ORDER BY ORDINAL_POSITION",
                SqlBuilder.columnsMetadataSql("db1", "t1"));
    }

    /**
     * DATA_TYPE and COLUMN_TYPE are different answers and both are needed: the first names the
     * StarRocks type ("array", "hll") and is the only way to tell a complex or non-exportable
     * column from a VARCHAR, the second carries the full nesting ("array&lt;int&gt;"). Dropping
     * either one silently disables a guard downstream, so both are asserted by name.
     */
    @Test
    public void testColumnsMetadataSqlSelectsBothTypeColumns() {
        String sql = SqlBuilder.columnsMetadataSql("db1", "t1");
        assertTrue("DATA_TYPE is what the HLL/BITMAP guard reads", sql.contains("DATA_TYPE"));
        assertTrue("COLUMN_TYPE carries the nested type", sql.contains("COLUMN_TYPE"));
    }

    /** Identifiers reach information_schema as string literals, so they are quoted, not backticked. */
    @Test
    public void testColumnsMetadataSqlEscapesStringLiterals() {
        assertTrue(SqlBuilder.columnsMetadataSql("db'1", "t1").contains("TABLE_SCHEMA = 'db\\'1'"));
    }
}
