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

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts transport-specific nested values into the connector's neutral values.
 * JDBC ResultSet access belongs to JdbcRowReader; Arrow ADBC uses this conversion directly.
 */
abstract class ValueReader {

    /** The transport's raw form of a nested column to the neutral value. */
    protected abstract Object nested(ColumnType type, Object raw);

    /** A leaf of the raw tree -- an element, a map key or value, a struct field -- to the canonical scalar. */
    protected abstract Object leaf(ColumnType type, Object raw);

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
                    if (!type.fieldNames.contains(name)) {
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
                return leaf(type, raw);
        }
    }

    /** The map key as the STRING the wire schema declares, spelled per the declared key type. */
    protected final String keyText(ColumnType keyType, Object rawKey) {
        Object key = rawKey == null ? null : leaf(keyType, rawKey);
        if (key == null) {
            throw new DataException("null map key in a " + keyType + "-keyed map");
        }
        return String.valueOf(key);
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

}
