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
 * Reads one result-set row into an {@code Object[]}, choosing the JDBC getter from the
 * {@link ColumnMeta} the schema was built from.
 *
 * <p><b>That last part is the point of this class.</b> It used to switch on
 * {@code ResultSetMetaData.getColumnType()}, re-asking the driver what each column was -- while
 * {@code ChangeRecordMapper} built the Connect schema from {@link ColumnMetadataReader}'s list.
 * Two sources of truth for one read path, and on Arrow Flight the driver's own account is known to
 * be wrong: it repeats the schema, calls every column NOT NULL, and zeroes precision and scale.
 * Any disagreement between the two lands as a {@code DataException} at serialization time -- schema
 * says {@code Date}, value arrives a {@code String} -- far from the cause. Reading through the same
 * list the schema came from means the two cannot drift apart.
 */
final class RowExtractor {

    private RowExtractor() {
    }

    /**
     * Builds the UTC calendar handed to every {@code getDate}/{@code getTimestamp} call of one
     * streaming read.
     *
     * <p>Without an explicit calendar the driver materializes temporal values in the JVM default
     * zone, which breaks Kafka Connect's logical types on any worker not running in UTC. Connect's
     * {@code Date} logical type is defined as UTC midnight, so a local-midnight {@code
     * java.sql.Date} makes JsonConverter/AvroConverter throw {@code DataException("Kafka Connect
     * Date type should not have any time fields set to non-zero values")}; a {@code Timestamp}
     * read in local time silently lands the instant off by the worker's UTC offset.
     *
     * <p>{@link Calendar} is not thread-safe, so this is deliberately not a shared static: one
     * instance is created per streaming read and stays confined to the poll thread driving it
     * (the MariaDB date/timestamp codecs {@code clear()} it before each use, so reuse across the
     * rows of a single read is safe).
     */
    static Calendar newUtcCalendar() {
        return Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    }

    /**
     * Reads the leading {@code cols.size()} columns of the current row.
     *
     * <p>A CHANGES query projects {@code __CHANGE_TYPE__} and {@code __ROW_VERSION__} after the
     * business columns; those are not described by {@code cols} and are read by the caller.
     */
    static Object[] extractRow(ResultSet rs, List<ColumnMeta> cols, Calendar utc) throws SQLException {
        Object[] row = new Object[cols.size()];
        for (int i = 0; i < cols.size(); i++) {
            row[i] = extractValue(rs, cols.get(i), i + 1, utc);
        }
        return row;
    }

    /** Reads one column, using {@code col}'s declared type rather than the driver's opinion. */
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
            case Types.FLOAT:
                value = rs.getFloat(index);
                break;
            case Types.DOUBLE:
                value = rs.getDouble(index);
                break;
            // Kept in step with ChangeRecordMapper.schemaFor, which maps these to a BYTES schema.
            // getString() here would hand back charset-decoded text and lose the original bytes.
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
