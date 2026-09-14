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

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

/**
 * Reads one row into an {@code Object[]}, choosing each getter from the {@link ColumnMeta} the
 * schema was built from. Switching on {@code ResultSetMetaData.getColumnType()} would give one read
 * path two sources of truth, and on Arrow Flight the driver's account is known to be wrong.
 *
 * <p>The array is canonical, not raw: a DECIMAL is at its declared scale, a DATE or DATETIME is
 * already {@link TemporalText}, a nested column is the readers' neutral value. This is where the
 * two transports meet, so whatever they disagree on is settled here and {@link ChangeRecordMapper}
 * only has to shape.
 */
final class RowExtractor {

    private RowExtractor() {
    }

    /**
     * The UTC calendar for one streaming read. Without it the driver materializes temporal values in
     * the JVM default zone, and {@link TemporalText} would then print a DATE as the neighbouring
     * day and a DATETIME shifted by the worker's offset -- silently, since both are text now.
     * Per-read because {@link Calendar} is not thread-safe.
     */
    static Calendar newUtcCalendar() {
        return Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    }

    /** The leading {@code cols.size()} columns; a CHANGES query's two trailing pseudo-columns are
     * read by the caller. */
    static Object[] extractRow(ResultSet rs, List<ColumnMeta> cols, Calendar utc) throws SQLException {
        return extractRow(rs, cols, utc, false);
    }

    /**
     * {@code arrowFlight} names the transport because one read differs by driver: the Arrow
     * driver's {@code getTimestamp(i, calendar)} ignores the calendar's zone and builds the value in
     * the JVM's, so the digits must be recovered afterwards ({@link TemporalText#fromJvmWallClock}).
     * Everything else is the same for both.
     */
    static Object[] extractRow(ResultSet rs, List<ColumnMeta> cols, Calendar utc, boolean arrowFlight)
            throws SQLException {
        Object[] row = new Object[cols.size()];
        for (int i = 0; i < cols.size(); i++) {
            row[i] = extractValue(rs, cols.get(i), i + 1, utc, arrowFlight);
        }
        return row;
    }

    static Object extractValue(ResultSet rs, ColumnMeta col, int index, Calendar utc) throws SQLException {
        return extractValue(rs, col, index, utc, false);
    }

    static Object extractValue(ResultSet rs, ColumnMeta col, int index, Calendar utc, boolean arrowFlight)
            throws SQLException {
        if (col.nested != null) {
            return nested(rs, col, index);
        }
        Object value;
        switch (col.jdbcType) {
            // Connect's Decimal wants the BigDecimal at the schema's scale; the Arrow driver hands a
            // DECIMALV2 back at scale 9 and MariaDB at the declared one.
            case Types.DECIMAL:
            case Types.NUMERIC: {
                BigDecimal d = rs.getBigDecimal(index);
                value = d == null ? null : d.setScale(col.scale);
                break;
            }
            case Types.DATE: {
                java.sql.Date d = rs.getDate(index, utc);
                value = d == null ? null : TemporalText.date(d);
                break;
            }
            case Types.TIMESTAMP: {
                Timestamp ts = rs.getTimestamp(index, utc);
                if (arrowFlight && ts != null) {
                    ts = TemporalText.fromJvmWallClock(ts);
                }
                value = ts == null ? null : TemporalText.dateTime(ts);
                break;
            }
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

    /**
     * The two transports hand a nested column over in different shapes: the MySQL driver gives the
     * BE's text as a String, the Arrow Flight driver gives the vector's objects. The class of what
     * getObject returns is the only thing that tells them apart, and each goes to its reader.
     *
     * <p>On Arrow, getObject is Avatica's, which dispatches on the column's JDBC type id: a MAP or
     * STRUCT column is JAVA_OBJECT and arrives as the vector's Map, but an ARRAY column is
     * Types.ARRAY and arrives as a {@link java.sql.Array} whose getArray() holds the element objects.
     * Only the top level is wrapped this way; a nested list is a plain List.
     */
    private static Object nested(ResultSet rs, ColumnMeta col, int index) throws SQLException {
        Object raw = rs.getObject(index);
        if (raw == null || rs.wasNull()) {
            return null;
        }
        if (raw instanceof String) {
            return MysqlTextReader.read(col.nested, (String) raw, MysqlTextReader.SESSION_BINARY_ENCODING);
        }
        if (raw instanceof java.sql.Array) {
            Object elements = ((java.sql.Array) raw).getArray();
            raw = elements instanceof Object[] ? Arrays.asList((Object[]) elements) : elements;
        }
        return ArrowValueReader.read(col.nested, raw);
    }
}
