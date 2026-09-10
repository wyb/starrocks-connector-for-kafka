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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

/**
 * Reads one row into an {@code Object[]}, choosing each getter from the {@link ColumnMeta} the
 * schema was built from. Switching on {@code ResultSetMetaData.getColumnType()} would give one read
 * path two sources of truth, and on Arrow Flight the driver's account is known to be wrong.
 */
final class RowExtractor {

    private RowExtractor() {
    }

    /**
     * The UTC calendar for one streaming read. Without it the driver materializes temporal values in
     * the JVM default zone: a local-midnight {@code java.sql.Date} makes converters throw on
     * Connect's Date logical type, and a Timestamp lands off by the worker's offset. Per-read
     * because {@link Calendar} is not thread-safe.
     */
    static Calendar newUtcCalendar() {
        return Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    }

    /** The leading {@code cols.size()} columns; a CHANGES query's two trailing pseudo-columns are
     * read by the caller. */
    static Object[] extractRow(ResultSet rs, List<ColumnMeta> cols, Calendar utc) throws SQLException {
        Object[] row = new Object[cols.size()];
        for (int i = 0; i < cols.size(); i++) {
            row[i] = extractValue(rs, cols.get(i), i + 1, utc);
        }
        return row;
    }

    static Object extractValue(ResultSet rs, ColumnMeta col, int index, Calendar utc) throws SQLException {
        Object value;
        switch (col.jdbcType) {
            case Types.DECIMAL:
            case Types.NUMERIC:
                value = rs.getBigDecimal(index);
                break;
            case Types.DATE:
                value = rs.getDate(index, utc);
                break;
            case Types.TIMESTAMP:
                value = rs.getTimestamp(index, utc);
                break;
            case Types.BIT:
            case Types.BOOLEAN:
                value = rs.getBoolean(index);
                break;
            case Types.TINYINT:
                value = rs.getByte(index);
                break;
            case Types.SMALLINT:
                value = rs.getShort(index);
                break;
            case Types.INTEGER:
                value = rs.getInt(index);
                break;
            case Types.BIGINT:
                value = rs.getLong(index);
                break;
            case Types.REAL:
                value = rs.getFloat(index);
                break;
            // FLOAT is double precision in JDBC; REAL is the single-precision one.
            case Types.FLOAT:
            case Types.DOUBLE:
                value = rs.getDouble(index);
                break;
            // In step with ChangeRecordMapper.schemaFor, which maps these to BYTES. getString()
            // would charset-decode and lose the original bytes.
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                value = rs.getBytes(index);
                break;
            default:
                value = rs.getString(index);
                break;
        }
        return rs.wasNull() ? null : value;
    }
}
