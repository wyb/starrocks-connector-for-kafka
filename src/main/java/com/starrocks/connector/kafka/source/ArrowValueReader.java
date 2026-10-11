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

import org.apache.arrow.vector.util.Text;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts Arrow materialized values to the connector's neutral values.
 * JDBC and ADBC readers share this nested conversion but extract rows differently.
 */
class ArrowValueReader extends ValueReader {

    /** Arrow's names for the two fields of a map entry struct (MapVector.KEY_NAME / VALUE_NAME). */
    private static final String ENTRY_KEY = "key";
    private static final String ENTRY_VALUE = "value";

    @Override
    protected Object nested(ColumnType type, Object raw) {
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
    protected Object leaf(ColumnType type, Object raw) {
        switch (type.kind) {
            case DATE:
                return dateOfEpochDays(as(Number.class, raw, type).intValue());
            case DATETIME:
                return dateTimeOfEpochMicros(as(Number.class, raw, type).longValue());
            case DECIMAL:
            case LARGEINT:
                return as(BigDecimal.class, raw, type).setScale(type.scale);
            case STRING:
            case JSON:
                return textValue(type, raw);
            case BYTES:
                return as(byte[].class, raw, type);
            case BOOLEAN:
                if (raw instanceof Boolean) {
                    return raw;
                }
                return as(Number.class, raw, type).intValue() != 0;
            case TINYINT:
                return as(Number.class, raw, type).byteValue();
            case SMALLINT:
                return as(Number.class, raw, type).shortValue();
            case INT:
                return as(Number.class, raw, type).intValue();
            case BIGINT:
                return as(Number.class, raw, type).longValue();
            case FLOAT:
                return as(Number.class, raw, type).floatValue();
            case DOUBLE:
                return as(Number.class, raw, type).doubleValue();
            default:
                throw new DataException("no Arrow reader for " + type);
        }
    }

    static String textValue(ColumnType type, Object raw) {
        if (raw instanceof CharSequence || raw instanceof Text) {
            return raw.toString();
        }
        throw new DataException("expected text for " + type + " but Arrow returned " + raw.getClass().getName());
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
}
