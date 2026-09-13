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

import org.junit.Test;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

/**
 * The temporal text must match what StarRocks prints for the same value, digit for digit.
 *
 * <p>Fixtures are built as UTC instants, the way {@code getTimestamp(i, utcCalendar)} hands them
 * over; {@code Timestamp.valueOf} would read the digits in the JVM's default zone instead.
 */
public class TemporalTextTest {

    private static Timestamp at(String seconds, int nanos) {
        Timestamp ts = Timestamp.from(LocalDateTime.parse(seconds.replace(' ', 'T')).toInstant(ZoneOffset.UTC));
        ts.setNanos(nanos);
        return ts;
    }

    private static java.sql.Date on(String day) {
        return new java.sql.Date(LocalDate.parse(day).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
    }

    @Test
    public void testDateIsIsoCalendarDate() {
        assertEquals("1970-01-01", TemporalText.date(new Date(0L)));
        assertEquals("2026-08-05", TemporalText.date(on("2026-08-05")));
    }

    /** The fraction is printed only when it is non-zero, and then always as six digits. */
    @Test
    public void testDateTimeFractionFollowsStarRocks() {
        assertEquals("2026-08-05 12:34:56", TemporalText.dateTime(at("2026-08-05 12:34:56", 0)));
        assertEquals("2026-08-05 12:34:56.123456", TemporalText.dateTime(at("2026-08-05 12:34:56", 123_456_000)));
        assertEquals("2026-08-05 12:34:56.120000", TemporalText.dateTime(at("2026-08-05 12:34:56", 120_000_000)));
    }

    /** A plain java.util.Date carries millis only; they become the first three fraction digits. */
    @Test
    public void testPlainDateUsesItsMillis() {
        assertEquals("1970-01-01 00:00:01.500000", TemporalText.dateTime(new Date(1_500L)));
        assertEquals("1970-01-01 00:00:00", TemporalText.dateTime(new Date(0L)));
    }

    /** The epoch-based entry points Arrow needs agree with the Date-based ones. */
    @Test
    public void testEpochDaysAndMicrosMatchTheDateForms() {
        assertEquals("2026-08-05", TemporalText.dateOfEpochDays(20670));
        assertEquals("1969-12-31", TemporalText.dateOfEpochDays(-1));
        assertEquals("2026-08-05 12:34:56.123456", TemporalText.dateTimeOfEpochMicros(1785933296123456L));
        assertEquals("2026-08-05 12:34:56", TemporalText.dateTimeOfEpochMicros(1785933296000000L));
        assertEquals("1969-12-31 23:59:59.999999", TemporalText.dateTimeOfEpochMicros(-1L));
    }

    /** valueOf(LocalDateTime) then fromJvmWallClock must give the digits back, nanos included. */
    @Test
    public void testFromJvmWallClockUndoesValueOf() {
        TimeZone previous = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        try {
            LocalDateTime digits = LocalDateTime.of(2026, 8, 5, 12, 34, 56, 123_456_000);
            Timestamp shifted = Timestamp.valueOf(digits);            // what the Arrow driver does
            assertEquals("2026-08-05 04:34:56.123456", TemporalText.dateTime(shifted));
            assertEquals("2026-08-05 12:34:56.123456", TemporalText.dateTime(TemporalText.fromJvmWallClock(shifted)));
        } finally {
            TimeZone.setDefault(previous);
        }
    }

    /** Before the epoch the seconds and the fraction must still be split with floor semantics. */
    @Test
    public void testBeforeEpoch() {
        assertEquals("1969-12-31", TemporalText.date(new Date(-1L)));
        assertEquals("1969-12-31 23:59:59.999000", TemporalText.dateTime(new Date(-1L)));
    }
}
