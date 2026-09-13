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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns what the Arrow Flight JDBC driver's {@code getObject()} returns for a nested column into
 * the neutral value {@link ColumnType#toConnectValue} assembles from.
 *
 * <p>The driver hands back the vectors' own objects: {@code List} for a list, a {@code Map} of
 * field name to value for a struct, {@code Text} for varchar, and for the types StarRocks sends
 * as something else on the wire, the raw number -- an {@code Integer} of days for a DATE, a
 * {@code Long} of microseconds for a DATETIME. Those two are indistinguishable from an INT and a
 * BIGINT by class alone, which is why every step here is driven by the declared
 * {@link ColumnType} and not by what the object looks like.
 *
 * <p>A map comes in two shapes. At the top level the driver's accessor converts it to a
 * {@code Map}; nested inside a list or a struct it is whatever {@code MapVector.getObject}
 * returns, and {@code MapVector} inherits that from {@code ListVector}: a {@code List} of
 * {@code {key, value}} entry maps.
 */
final class ArrowValueReader {

    /** Arrow's names for the two fields of a map entry struct (MapVector.KEY_NAME / VALUE_NAME). */
    private static final String ENTRY_KEY = "key";
    private static final String ENTRY_VALUE = "value";

    private ArrowValueReader() {
    }

    static Object read(ColumnType type, Object value) {
        if (value == null) {
            return null;
        }
        switch (type.kind) {
            case ARRAY: {
                List<Object> out = new ArrayList<>();
                for (Object item : as(List.class, value, type)) {
                    out.add(read(type.element, item));
                }
                return out;
            }
            case MAP: {
                Map<String, Object> out = new LinkedHashMap<>();
                if (value instanceof Map) {
                    for (Map.Entry<?, ?> e : ((Map<?, ?>) value).entrySet()) {
                        out.put(keyText(type.key, e.getKey()), read(type.value, e.getValue()));
                    }
                    return out;
                }
                for (Object entry : as(List.class, value, type)) {
                    Map<?, ?> kv = as(Map.class, entry, type);
                    out.put(keyText(type.key, kv.get(ENTRY_KEY)), read(type.value, kv.get(ENTRY_VALUE)));
                }
                return out;
            }
            case STRUCT: {
                Map<?, ?> in = as(Map.class, value, type);
                Map<String, Object> out = new LinkedHashMap<>();
                for (ColumnType.Field f : type.fields) {
                    out.put(f.name, read(f.type, in.get(f.name)));
                }
                return out;
            }
            case DATE:
                return TemporalText.dateOfEpochDays(as(Number.class, value, type).intValue());
            case DATETIME:
                return TemporalText.dateTimeOfEpochMicros(as(Number.class, value, type).longValue());
            case DECIMAL:
                return as(BigDecimal.class, value, type).setScale(type.scale);
            case LARGEINT:
                return as(BigDecimal.class, value, type).setScale(0);
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

    /** The map key as the STRING the wire schema declares, spelled per the declared key type. */
    private static String keyText(ColumnType keyType, Object key) {
        if (key == null) {
            throw new DataException("null map key in a " + keyType + "-keyed map");
        }
        switch (keyType.kind) {
            case DATE:
            case DATETIME:
            case DECIMAL:
            case LARGEINT:
                return String.valueOf(read(keyType, key));
            default:
                return key.toString();
        }
    }

    private static <T> T as(Class<T> expected, Object value, ColumnType type) {
        if (!expected.isInstance(value)) {
            throw new DataException("expected " + expected.getSimpleName() + " for " + type
                    + " but the driver returned " + value.getClass().getName());
        }
        return expected.cast(value);
    }
}
