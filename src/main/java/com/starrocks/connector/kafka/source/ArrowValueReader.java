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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The Arrow Flight JDBC driver's quirks, and nothing else.
 *
 * <p>Top level: its {@code getTimestamp(i, calendar)} ends in {@code Timestamp.valueOf(LocalDateTime)}
 * and so builds the value in the JVM's zone whatever calendar was passed; and Avatica's
 * {@code getObject} dispatches on the JDBC type id, so a top-level ARRAY arrives as a
 * {@link java.sql.Array} while MAP and STRUCT arrive as the vector's {@code Map}.
 *
 * <p>Nested: the vectors' own objects. {@code List} for a list, {@code Map} of field name to value
 * for a struct, {@code Text} for varchar, and for what StarRocks sends as something else on the
 * wire, the raw number -- an {@code Integer} of days for a DATE, a {@code Long} of microseconds for
 * a DATETIME, indistinguishable from an INT and a BIGINT by class alone. A map nested inside a list
 * or struct is whatever {@code MapVector.getObject} returns, which {@code MapVector} inherits from
 * {@code ListVector}: a {@code List} of {@code {key, value}} entry maps.
 */
final class ArrowValueReader extends ValueReader {

    /** Arrow's names for the two fields of a map entry struct (MapVector.KEY_NAME / VALUE_NAME). */
    private static final String ENTRY_KEY = "key";
    private static final String ENTRY_VALUE = "value";

    @Override
    protected Timestamp timestamp(ResultSet rs, int index) throws SQLException {
        Timestamp ts = super.timestamp(rs, index);
        return ts == null ? null : fromJvmWallClock(ts);
    }

    @Override
    protected Object nested(ColumnType type, Object raw) {
        if (raw instanceof java.sql.Array) {
            try {
                Object elements = ((java.sql.Array) raw).getArray();
                raw = elements instanceof Object[] ? Arrays.asList((Object[]) elements) : elements;
            } catch (SQLException e) {
                throw new DataException("could not read the elements of " + type, e);
            }
        }
        return readNested(type, raw);
    }

    @Override
    protected Map<?, ?> asMap(Object raw, ColumnType type) {
        if (raw instanceof List && type.kind == ColumnType.Kind.MAP) {
            Map<Object, Object> entries = new LinkedHashMap<>();
            for (Object entry : (List<?>) raw) {
                Map<?, ?> kv = as(Map.class, entry, type);
                entries.put(kv.get(ENTRY_KEY), kv.get(ENTRY_VALUE));
            }
            return entries;
        }
        return super.asMap(raw, type);
    }

    @Override
    protected Object scalar(ColumnType type, Object value) {
        switch (type.kind) {
            case DATE:
                return dateOfEpochDays(as(Number.class, value, type).intValue());
            case DATETIME:
                return dateTimeOfEpochMicros(as(Number.class, value, type).longValue());
            case DECIMAL:
            case LARGEINT:
                return as(BigDecimal.class, value, type).setScale(type.scale);
            case STRING:
            case JSON:
                // Text is a CharSequence; a plain String passes through unchanged.
                return value.toString();
            case BYTES:
                return as(byte[].class, value, type);
            case BOOLEAN:
                return as(Boolean.class, value, type);
            case TINYINT:
                return as(Number.class, value, type).byteValue();
            case SMALLINT:
                return as(Number.class, value, type).shortValue();
            case INT:
                return as(Number.class, value, type).intValue();
            case BIGINT:
                return as(Number.class, value, type).longValue();
            case FLOAT:
                return as(Number.class, value, type).floatValue();
            case DOUBLE:
                return as(Number.class, value, type).doubleValue();
            default:
                throw new DataException("no Arrow reader for " + type);
        }
    }

    /** Arrow's date32: days since the epoch. */
    static String dateOfEpochDays(int days) {
        return LocalDate.ofEpochDay(days).toString();
    }

    /** Arrow's timestamp(MICRO): microseconds since the epoch. */
    static String dateTimeOfEpochMicros(long micros) {
        Timestamp ts = new Timestamp(Math.floorDiv(micros, 1_000L));
        ts.setNanos((int) (Math.floorMod(micros, 1_000_000L) * 1_000L));
        return dateTimeText(ts);
    }

    /**
     * Undo the driver's zone shift. Its timestamp accessor ends in {@code Timestamp.valueOf(LocalDateTime)},
     * which reads the stored digits in the JVM's default zone whatever Calendar was passed (a UTC+8
     * worker turns 12:34:56 into the instant 04:34:56Z). {@code toLocalDateTime()} in that same zone
     * hands the digits back; re-anchoring them in UTC is what {@link #dateTimeText} expects. Not
     * invertible inside a DST gap -- the one hour a year a DST-zone worker can still print a shifted value.
     */
    static Timestamp fromJvmWallClock(Timestamp shifted) {
        LocalDateTime digits = shifted.toLocalDateTime();
        return Timestamp.from(digits.toInstant(ZoneOffset.UTC));
    }
}
