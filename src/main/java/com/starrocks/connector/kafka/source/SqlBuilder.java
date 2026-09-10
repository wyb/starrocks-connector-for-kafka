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

    /** The ttl goes in as a decimal string, as for {@link #bookmarkCreateSql}. */
    public static String bookmarkRenewSql(String db, String table, long bookmarkId, String holder, long ttlMs) {
        return "SELECT bookmark_renew(" + quoteStr(db) + "," + quoteStr(table) + "," +
                quoteStr(Long.toString(bookmarkId)) + "," + quoteStr(holder) + "," +
                quoteStr(Long.toString(ttlMs)) + ")";
    }

    public static String bookmarkReleaseSql(String db, String table, long bookmarkId, String holder) {
        return "SELECT bookmark_release(" + quoteStr(db) + "," + quoteStr(table) + "," +
                quoteStr(Long.toString(bookmarkId)) + "," + quoteStr(holder) + ")";
    }

    /**
     * Both type columns are projected: {@code DATA_TYPE} is StarRocks' type name ("array", "hll")
     * and {@code COLUMN_TYPE} carries the nesting ("array&lt;int&gt;") and BOOLEAN's "tinyint(1)".
     *
     * <p>{@code NUMERIC_PRECISION}/{@code NUMERIC_SCALE}, not StarRocks' own
     * {@code COLUMN_SIZE}/{@code DECIMAL_DIGITS} extensions: the BE fills COLUMN_SIZE from
     * {@code columnLength}, which is a length and not a precision.
     */
    public static String columnsMetadataSql(String db, String table) {
        return "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE"
                + " FROM information_schema.columns WHERE TABLE_SCHEMA = " + quoteStr(db)
                + " AND TABLE_NAME = " + quoteStr(table) + " ORDER BY ORDINAL_POSITION";
    }

    /**
     * {@code PROPERTIES} is the table's property map as JSON (FE builds it with
     * {@code Gson().toJson(table.getProperties())}), which is why the CDC property is read from here
     * rather than matched in {@code SHOW CREATE TABLE} text.
     */
    /**
     * The key columns of any table model, in declaration order. {@code tables_config.PRIMARY_KEY} is
     * not usable here: FE computes the key columns for every model but publishes them only for
     * PRIMARY_KEYS and UNIQUE_KEYS, leaving AGG and DUP tables with an empty string. {@code
     * COLUMN_KEY} carries the model's own tag (PRI/AGG/DUP/UNI) on each key column instead.
     */
    public static String keyColumnsSql(String db, String table) {
        return "SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_SCHEMA = " + quoteStr(db)
                + " AND TABLE_NAME = " + quoteStr(table) + " AND COLUMN_KEY <> ''"
                + " ORDER BY ORDINAL_POSITION";
    }

    public static String tableConfigSql(String db, String table) {
        return "SELECT TABLE_MODEL, PROPERTIES FROM information_schema.tables_config"
                + " WHERE TABLE_SCHEMA = " + quoteStr(db) + " AND TABLE_NAME = " + quoteStr(table);
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
