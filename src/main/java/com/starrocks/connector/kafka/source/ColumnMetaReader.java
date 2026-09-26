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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The authoritative column list, used both to build the Connect schema and to read values back out.
 * One strategy for both transports: ask the server.
 */
final class ColumnMetaReader {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnMetaReader.class);

    private final FeConnection connection;

    ColumnMetaReader(FeConnection connection) {
        this.connection = connection;
    }

    /**
     * The column list for one table, in ordinal order, straight from {@code information_schema}.
     *
     * <p>Never from {@link java.sql.ResultSetMetaData}: the Arrow Flight driver repeats the schema,
     * calls every column NOT NULL and zeroes precision and scale, and the MySQL protocol cannot tell
     * an ARRAY, a JSON or an HLL sketch from a VARCHAR. Two descriptions would also mean one table
     * producing two schemas depending on the URL scheme.
     */
    List<ColumnMeta> fetchColumns(String db, String table) throws SQLException {
        String sql = SqlBuilder.columnsMetaSql(db, table);
        try {
            Connection c = connection.get();
            try (Statement stmt = c.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                List<ColumnMeta> result = new ArrayList<>();
                Set<String> seen = new HashSet<>();
                while (rs.next()) {
                    ColumnMeta col = new ColumnMeta(rs.getString("COLUMN_NAME"), rs.getString("DATA_TYPE"),
                            rs.getString("COLUMN_TYPE"), rs.getInt("NUMERIC_SCALE"),
                            !"NO".equalsIgnoreCase(rs.getString("IS_NULLABLE")));
                    if (!seen.add(col.name)) {
                        throw new SQLException("information_schema.columns lists the column '" + col.name
                                + "' more than once for " + db + "." + table + ": " + result + " then " + col);
                    }
                    result.add(col);
                }
                if (result.isEmpty()) {
                    throw new SQLException("table not found: " + db + "." + table
                            + " (information_schema.columns returned no rows)");
                }
                LOG.info("Resolved {} column(s) for {}.{} from information_schema: {}",
                        result.size(), db, table, result);
                return result;
            }
        } catch (SQLException e) {
            connection.closeIfBroken(e);
            throw e;
        }
    }
}
