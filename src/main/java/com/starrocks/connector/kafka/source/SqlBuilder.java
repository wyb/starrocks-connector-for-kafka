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

import java.util.List;

/**
 * Every statement the CDC source issues, as pure functions. Identifiers are backtick-quoted and
 * literals single-quoted; the pseudo-columns {@code __CHANGE_TYPE__} and {@code __ROW_VERSION__}
 * are never quoted.
 */
public final class SqlBuilder {

    private SqlBuilder() {
    }

    /** Wraps an identifier in backticks, doubling any embedded backtick: {@code a`b -> `a``b`}. */
    public static String quoteId(String id) {
        return "`" + id.replace("`", "``") + "`";
    }

    /** Backslashes are escaped before quotes, so the backslash quote-escaping adds is not re-escaped. */
    public static String quoteStr(String s) {
        String escaped = s.replace("\\", "\\\\").replace("'", "\\'");
        return "'" + escaped + "'";
    }

    public static String snapshotSql(String db, String table, List<String> cols, long bookmarkId) {
        return "SELECT " + quoteCols(cols) + " FROM " + qualifiedTable(db, table) +
                " [_BOOKMARK_" + bookmarkId + "_]";
    }

    /**
     * The ORDER BY is a correctness invariant. The BE emits version-descending with INSERT
     * ahead of DELETE inside a version; passed through, an UPDATE would apply as insert-then-delete
     * and the row would vanish downstream.
     */
    public static String changesSql(String db, String table, List<String> cols, long base, long head) {
        return "SELECT " + quoteCols(cols) + ",__CHANGE_TYPE__,__ROW_VERSION__ FROM " +
                qualifiedTable(db, table) + " [_CHANGES_" + base + "_" + head + "_]" +
                " ORDER BY __ROW_VERSION__, __CHANGE_TYPE__ DESC";
    }

    /** The ttl goes in as a decimal string, not a number. */
    public static String bookmarkCreateSql(String db, String table, String holder, long ttlMs) {
        return "SELECT bookmark_create(" + quoteStr(db) + "," + quoteStr(table) + "," +
                quoteStr(holder) + "," + quoteStr(Long.toString(ttlMs)) + ")";
    }

    public static String bookmarkReleaseSql(String db, String table, long bookmarkId, String holder) {
        return "SELECT bookmark_release(" + quoteStr(db) + "," + quoteStr(table) + "," +
                quoteStr(Long.toString(bookmarkId)) + "," + quoteStr(holder) + ")";
    }

    /** Never read for rows; it exists only to get at ResultSetMetaData. */
    public static String columnsProbeSql(String db, String table) {
        return "SELECT * FROM " + qualifiedTable(db, table) + " LIMIT 0";
    }

    /**
     * The column list as rows, asking the server to describe the table rather than asking the
     * driver to describe a query -- which on Arrow Flight is unusable (repeated schema, every
     * column NOT NULL, precision and scale zeroed).
     *
     * <p>Both type columns are needed: {@code DATA_TYPE} is StarRocks' type name ("array", "hll"),
     * the only way to tell a complex or non-exportable column from a VARCHAR, and
     * {@code COLUMN_TYPE} carries the nesting ("array&lt;int&gt;").
     */
    public static String columnsMetadataSql(String db, String table) {
        return "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_SIZE, DECIMAL_DIGITS"
                + " FROM information_schema.columns WHERE TABLE_SCHEMA = " + quoteStr(db)
                + " AND TABLE_NAME = " + quoteStr(table) + " ORDER BY ORDINAL_POSITION";
    }

    public static String tableConfigSql(String db, String table) {
        return "SELECT TABLE_MODEL, PRIMARY_KEY FROM information_schema.tables_config WHERE TABLE_SCHEMA = " +
                quoteStr(db) + " AND TABLE_NAME = " + quoteStr(table);
    }

    public static String showCreateTableSql(String db, String table) {
        return "SHOW CREATE TABLE " + qualifiedTable(db, table);
    }

    private static String qualifiedTable(String db, String table) {
        return quoteId(db) + "." + quoteId(table);
    }

    private static String quoteCols(List<String> cols) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(quoteId(cols.get(i)));
        }
        return sb.toString();
    }
}
