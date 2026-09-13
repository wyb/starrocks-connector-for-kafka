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

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * DATE and DATETIME as the text StarRocks itself prints: {@code 2026-08-05} and
 * {@code 2026-08-05 12:34:56}, with {@code .ffffff} appended only when the microseconds are not
 * zero (BE {@code timestamp::to_string}). One formatter for both transports, so a value reads the
 * same whichever driver fetched it.
 *
 * <p>Values are read with a UTC calendar, so the instant's UTC fields are the wall-clock digits
 * StarRocks stored; DATETIME has no zone and none is claimed here.
 */
final class TemporalText {

    static final String DATE_LOGICAL_NAME = "com.starrocks.data.Date";
    static final String DATETIME_LOGICAL_NAME = "com.starrocks.data.DateTime";

    private static final DateTimeFormatter SECONDS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private TemporalText() {
    }

    static String date(java.util.Date value) {
        return utcSeconds(value.getTime()).toLocalDate().toString();
    }

    static String dateTime(java.util.Date value) {
        long micros = value instanceof Timestamp
                ? ((Timestamp) value).getNanos() / 1_000L
                : Math.floorMod(value.getTime(), 1_000L) * 1_000L;
        String text = SECONDS.format(utcSeconds(value.getTime()));
        return micros == 0 ? text : String.format("%s.%06d", text, micros);
    }

    /** Arrow's date32: days since the epoch. */
    static String dateOfEpochDays(int days) {
        return LocalDate.ofEpochDay(days).toString();
    }

    /** Arrow's timestamp(MICRO): microseconds since the epoch. */
    static String dateTimeOfEpochMicros(long micros) {
        Timestamp ts = new Timestamp(Math.floorDiv(micros, 1_000L));
        ts.setNanos((int) (Math.floorMod(micros, 1_000_000L) * 1_000L));
        return dateTime(ts);
    }

    // Timestamp.getTime() already folds the fraction into millis; keep only whole seconds here.
    private static LocalDateTime utcSeconds(long epochMillis) {
        return LocalDateTime.ofEpochSecond(Math.floorDiv(epochMillis, 1_000L), 0, ZoneOffset.UTC);
    }
}
