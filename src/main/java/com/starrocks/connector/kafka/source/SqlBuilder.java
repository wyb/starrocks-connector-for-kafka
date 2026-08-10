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
 * Pure static SQL string builder for the handful of statements the CDC source issues against
 * StarRocks: metadata probes, bookmark functions, and snapshot/CHANGES reads.
 *
 * <p>Identifiers are always backtick-quoted ({@link #quoteId(String)}) and string literals are
 * always single-quote-quoted ({@link #quoteStr(String)}). The two metadata pseudo-columns
 * {@code __CHANGE_TYPE__} and {@code __ROW_VERSION__} are never quoted.
 */
public final class SqlBuilder {

    private SqlBuilder() {
    }

    /** Wraps an identifier in backticks, doubling any embedded backtick: {@code a`b -> `a``b`}. */
    public static String quoteId(String id) {
        return "`" + id.replace("`", "``") + "`";
    }

    /**
     * Wraps a string literal in single quotes. Escapes backslashes first (each {@code \} becomes
     * {@code \\}), then escapes single quotes (each {@code '} becomes {@code \'}) so the
     * backslash introduced by quote-escaping is not itself re-escaped.
     */
    public static String quoteStr(String s) {
        String escaped = s.replace("\\", "\\\\").replace("'", "\\'");
        return "'" + escaped + "'";
    }

    /** SELECT `c1`,`c2` FROM `db`.`t` [_BOOKMARK_&lt;id&gt;_] */
    public static String snapshotSql(String db, String table, List<String> cols, long bookmarkId) {
        return "SELECT " + quoteCols(cols) + " FROM " + qualifiedTable(db, table) +
                " [_BOOKMARK_" + bookmarkId + "_]";
    }

    /**
     * SELECT `c1`,`c2`,__CHANGE_TYPE__,__ROW_VERSION__ FROM `db`.`t` [_CHANGES_&lt;base&gt;_&lt;head&gt;_]
     * ORDER BY __ROW_VERSION__, __CHANGE_TYPE__ DESC
     *
     * <p>The trailing ORDER BY is a correctness invariant, not cosmetic formatting: the BE's
     * natural output order for a CHANGES scan is version-descending with, within a version,
     * INSERT ahead of DELETE. Passing that order straight through would be wrong, so it is always
     * re-sorted to version-ascending with DELETE-before-INSERT ties broken by {@code DESC}.
     */
    public static String changesSql(String db, String table, List<String> cols, long base, long head) {
        return "SELECT " + quoteCols(cols) + ",__CHANGE_TYPE__,__ROW_VERSION__ FROM " +
                qualifiedTable(db, table) + " [_CHANGES_" + base + "_" + head + "_]" +
                " ORDER BY __ROW_VERSION__, __CHANGE_TYPE__ DESC";
    }

    /** SELECT bookmark_create('db','t','holder','&lt;ttl&gt;') -- ttl passed as a decimal string. */
    public static String bookmarkCreateSql(String db, String table, String holder, long ttlMs) {
        return "SELECT bookmark_create(" + quoteStr(db) + "," + quoteStr(table) + "," +
                quoteStr(holder) + "," + quoteStr(Long.toString(ttlMs)) + ")";
    }

    /** SELECT bookmark_release('db','t','&lt;id&gt;','holder') */
    public static String bookmarkReleaseSql(String db, String table, long bookmarkId, String holder) {
        return "SELECT bookmark_release(" + quoteStr(db) + "," + quoteStr(table) + "," +
                quoteStr(Long.toString(bookmarkId)) + "," + quoteStr(holder) + ")";
    }

    /** SELECT * FROM `db`.`t` LIMIT 0 -- used only to read ResultSetMetaData. */
    public static String columnsProbeSql(String db, String table) {
        return "SELECT * FROM " + qualifiedTable(db, table) + " LIMIT 0";
    }

    /**
     * The column list read as ordinary rows instead of as result-set metadata.
     *
     * <p>{@link #columnsProbeSql} asks the driver to describe a query; this asks the server to
     * describe a table. The difference matters on Arrow Flight, where the driver's description is
     * unusable -- it repeats the schema, calls every column NOT NULL, and zeroes precision and
     * scale. These rows are data, so both transports return the same thing.
     *
     * <p>{@code COLUMN_SIZE} and {@code DECIMAL_DIGITS} are StarRocks' JDBC-shaped spellings of
     * precision and scale (the names {@code DatabaseMetaData.getColumns} uses), so they land in
     * {@link ColumnMeta} without reinterpretation.
     *
     * <p>{@code DATA_TYPE} and {@code COLUMN_TYPE} are two different answers and both are needed.
     * The first is StarRocks' own type name ({@code "array"}, {@code "hll"}), which is the only way
     * to tell a complex or non-exportable column from a VARCHAR -- {@code java.sql.Types} cannot
     * express the difference. The second is the full nested type ({@code "array<int>"}), carried
     * for diagnostics now and needed to rebuild nested schemas later. FE fills both from
     * {@code Type.toMysqlDataTypeString()} / {@code toMysqlColumnTypeString()}; the BE scanner only
     * falls back to its own flat primitive mapping when FE leaves them unset.
     */
    public static String columnsMetadataSql(String db, String table) {
        return "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_SIZE, DECIMAL_DIGITS"
                + " FROM information_schema.columns WHERE TABLE_SCHEMA = " + quoteStr(db)
                + " AND TABLE_NAME = " + quoteStr(table) + " ORDER BY ORDINAL_POSITION";
    }

    /** SELECT TABLE_MODEL, PRIMARY_KEY FROM information_schema.tables_config WHERE TABLE_SCHEMA = 'db' AND TABLE_NAME = 't' */
    public static String tableConfigSql(String db, String table) {
        return "SELECT TABLE_MODEL, PRIMARY_KEY FROM information_schema.tables_config WHERE TABLE_SCHEMA = " +
                quoteStr(db) + " AND TABLE_NAME = " + quoteStr(table);
    }

    /** SHOW CREATE TABLE `db`.`t` */
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
