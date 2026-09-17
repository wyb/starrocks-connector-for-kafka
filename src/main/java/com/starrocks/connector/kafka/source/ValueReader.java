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

import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Reads one row out of a {@link ResultSet} into the canonical {@code Object[]} the mapper shapes:
 * decimals at the declared scale, DATE and DATETIME as the text StarRocks prints, nested columns as the
 * neutral value ({@code List}, {@code Map<String, Object>} for both maps and structs, canonical
 * scalars). Every decision is driven by the column's {@link ColumnType}, never by what the driver
 * says a column is.
 *
 * <p>Scalars go through the typed getters -- JDBC's own cross-driver coercion, and the one place a
 * UTC calendar can be handed to the driver -- so they read the same on both transports. Nested
 * columns have no typed getter; {@code getObject} hands over whatever the driver has, and the
 * transport's subclass turns that into the neutral value. The recursion over ARRAY, MAP and STRUCT
 * lives here; a subclass supplies {@link #scalar} for its raw leaves and may widen
 * {@link #asMap}.
 *
 * <p>One instance per streaming read: the calendar is not thread-safe.
 */
abstract class ValueReader {

    private final Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    /** The reader for the connection's transport, chosen where the transport is known. */
    static ValueReader forTransport(boolean arrowFlight) {
        return arrowFlight ? new ArrowValueReader() : new MysqlValueReader();
    }

    /** The leading {@code cols.size()} columns; a CHANGES query's two trailing pseudo-columns are
     * read by the caller. */
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
            case ARRAY:
            case MAP:
            case STRUCT: {
                Object raw = rs.getObject(index);
                return raw == null || rs.wasNull() ? null : nested(type, raw);
            }
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
            // Connect's Decimal wants the BigDecimal at the schema's scale; the Arrow driver hands a
            // DECIMALV2 back at scale 9 and MariaDB at the declared one. LARGEINT's scale is 0.
            case DECIMAL:
            case LARGEINT: {
                BigDecimal d = rs.getBigDecimal(index);
                value = d == null ? null : d.setScale(type.scale);
                break;
            }
            case DATE: {
                java.sql.Date d = rs.getDate(index, utc);
                value = d == null ? null : dateText(d);
                break;
            }
            case DATETIME: {
                Timestamp ts = timestamp(rs, index);
                value = ts == null ? null : dateTimeText(ts);
                break;
            }
            // getString would charset-decode and lose the bytes; the schema side says BYTES.
            case BYTES:
                value = rs.getBytes(index);
                break;
            case STRING:
            case JSON:
            case OPAQUE:
            default:
                value = rs.getString(index);
                break;
        }
        // Primitive getters return 0/false for SQL NULL; only wasNull tells.
        return rs.wasNull() ? null : value;
    }

    /**
     * DATETIME through the UTC calendar, which makes the instant's UTC fields the stored digits.
     * The Arrow reader overrides this to undo its driver's zone shift.
     */
    protected Timestamp timestamp(ResultSet rs, int index) throws SQLException {
        return rs.getTimestamp(index, utc);
    }

    /** Package-visible for tests: the calendar this instance hands to temporal getters. */
    Calendar utcCalendar() {
        return utc;
    }

    // DATE and DATETIME as the text StarRocks prints: 2026-08-05 and 2026-08-05 12:34:56, .ffffff only
    // when the microseconds are not zero (BE timestamp::to_string). Values come through the UTC
    // calendar, so an instant's UTC fields are the stored digits.

    private static final DateTimeFormatter SECONDS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    static String dateText(java.util.Date value) {
        return utcSeconds(value.getTime()).toLocalDate().toString();
    }

    static String dateTimeText(java.util.Date value) {
        long micros = value instanceof Timestamp
                ? ((Timestamp) value).getNanos() / 1_000L
                : Math.floorMod(value.getTime(), 1_000L) * 1_000L;
        String text = SECONDS.format(utcSeconds(value.getTime()));
        return micros == 0 ? text : String.format("%s.%06d", text, micros);
    }

    // Timestamp.getTime() already folds the fraction into millis; keep only whole seconds here.
    private static LocalDateTime utcSeconds(long epochMillis) {
        return LocalDateTime.ofEpochSecond(Math.floorDiv(epochMillis, 1_000L), 0, ZoneOffset.UTC);
    }

    /** The transport's raw form of a nested column to the neutral value. */
    protected abstract Object nested(ColumnType type, Object raw);

    /** The transport's raw form of a nested scalar to the canonical scalar. */
    protected abstract Object scalar(ColumnType type, Object raw);

    /** The recursion both transports share, once they have a raw tree of lists, maps and leaves. */
    protected final Object readNested(ColumnType type, Object raw) {
        if (raw == null) {
            return null;
        }
        switch (type.kind) {
            case ARRAY: {
                List<Object> out = new ArrayList<>();
                for (Object item : asList(raw, type)) {
                    out.add(readNested(type.element, item));
                }
                return out;
            }
            case MAP: {
                Map<?, ?> in = asMap(raw, type);
                Map<String, Object> out = new LinkedHashMap<>();
                for (Map.Entry<?, ?> e : in.entrySet()) {
                    out.put(keyText(type.key, e.getKey()), readNested(type.value, e.getValue()));
                }
                return out;
            }
            case STRUCT: {
                Map<?, ?> in = asMap(raw, type);
                for (Object name : in.keySet()) {
                    if (!isDeclared(type, name)) {
                        throw new DataException("struct field '" + name + "' is not declared in " + type);
                    }
                }
                // Declared order, and a field the driver omitted is null.
                Map<String, Object> out = new LinkedHashMap<>();
                for (ColumnType.Field f : type.fields) {
                    out.put(f.name, readNested(f.type, in.get(f.name)));
                }
                return out;
            }
            default:
                return scalar(type, raw);
        }
    }

    /** The map key as the STRING the wire schema declares, spelled per the declared key type. */
    private String keyText(ColumnType keyType, Object rawKey) {
        Object key = rawKey == null ? null : scalar(keyType, rawKey);
        if (key == null) {
            throw new DataException("null map key in a " + keyType + "-keyed map");
        }
        return String.valueOf(key);
    }

    private static boolean isDeclared(ColumnType struct, Object name) {
        for (ColumnType.Field f : struct.fields) {
            if (f.name.equals(name)) {
                return true;
            }
        }
        return false;
    }

    protected List<?> asList(Object raw, ColumnType type) {
        return as(List.class, raw, type);
    }

    protected Map<?, ?> asMap(Object raw, ColumnType type) {
        return as(Map.class, raw, type);
    }

    /** A shape the declared type cannot explain is schema drift: reported, never coerced. */
    protected static <T> T as(Class<T> expected, Object raw, ColumnType type) {
        if (!expected.isInstance(raw)) {
            throw new DataException("expected " + expected.getSimpleName() + " for " + type
                    + " but the driver returned " + raw.getClass().getName());
        }
        return expected.cast(raw);
    }
}
