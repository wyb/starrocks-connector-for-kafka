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
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

/** Reads JDBC rows; the selected value converter handles transport-specific nested values. */
final class JdbcRowReader {
    private final ValueReader values;
    private final boolean arrowFlight;
    private final Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    private JdbcRowReader(ValueReader values, boolean arrowFlight) {
        this.values = values;
        this.arrowFlight = arrowFlight;
    }

    static JdbcRowReader forTransport(boolean arrowFlight) {
        return new JdbcRowReader(arrowFlight ? new ArrowJdbcValueReader() : new MysqlValueReader(), arrowFlight);
    }

    /** Reads the table columns; a CHANGES query's two trailing pseudo-columns belong to the caller. */
    Object[] readRow(ResultSet rs, List<ColumnMeta> cols) throws SQLException {
        Object[] row = new Object[cols.size()];
        for (int i = 0; i < cols.size(); i++) {
            row[i] = read(rs, i + 1, cols.get(i).type);
        }
        return row;
    }

    Object read(ResultSet rs, int index, ColumnType type) throws SQLException {
        Object value;
        switch (type.kind) {
            case BOOLEAN:
                value = rs.getBoolean(index);
                break;
            case TINYINT:
                value = rs.getByte(index);
                break;
            case SMALLINT:
                value = rs.getShort(index);
                break;
            case INT:
                value = rs.getInt(index);
                break;
            case BIGINT:
                value = rs.getLong(index);
                break;
            case FLOAT:
                value = rs.getFloat(index);
                break;
            case DOUBLE:
                value = rs.getDouble(index);
                break;
            case DECIMAL:
            case LARGEINT: {
                BigDecimal d = rs.getBigDecimal(index);
                value = d == null ? null : d.setScale(type.scale);
                break;
            }
            case DATE: {
                java.sql.Date d = rs.getDate(index, utc);
                value = d == null ? null : ValueReader.dateText(d);
                break;
            }
            case DATETIME: {
                Timestamp ts = rs.getTimestamp(index, utc);
                if (arrowFlight && ts != null) {
                    ts = ArrowJdbcValueReader.fromJvmWallClock(ts);
                }
                value = ts == null ? null : ValueReader.dateTimeText(ts);
                break;
            }
            case BYTES:
                value = rs.getBytes(index);
                break;
            case ARRAY:
            case MAP:
            case STRUCT: {
                Object raw = rs.getObject(index);
                return raw == null || rs.wasNull() ? null : values.nested(type, raw);
            }
            case STRING:
            case JSON:
            case OPAQUE:
            default:
                value = rs.getString(index);
                break;
        }
        return rs.wasNull() ? null : value;
    }

    Calendar utcCalendar() {
        return utc;
    }

}
