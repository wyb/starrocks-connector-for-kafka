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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * The single funnel every value passes through on both read paths, and it had no test. The
 * getter chosen here comes from the {@link ColumnMeta} the Connect schema was built from; picking
 * a different one surfaces as a DataException at serialization time, far from the cause.
 */
public class RowExtractorTest {

    /** Records which getter was called, so a test can assert the choice and not just the value. */
    private static final class RecordingResultSet implements InvocationHandler {
        final List<String> calls = new ArrayList<>();
        Object next;
        boolean wasNull;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            String name = method.getName();
            if ("wasNull".equals(name)) {
                return wasNull;
            }
            calls.add(name);
            if (next != null) {
                return next;
            }
            // Primitive getters must not return null through the proxy.
            Class<?> r = method.getReturnType();
            if (r == boolean.class) {
                return false;
            } else if (r == byte.class) {
                return (byte) 0;
            } else if (r == short.class) {
                return (short) 0;
            } else if (r == int.class) {
                return 0;
            } else if (r == long.class) {
                return 0L;
            } else if (r == float.class) {
                return 0f;
            } else if (r == double.class) {
                return 0d;
            }
            return null;
        }
    }

    private static ResultSet proxyFor(RecordingResultSet handler) {
        return (ResultSet) Proxy.newProxyInstance(RowExtractorTest.class.getClassLoader(),
                new Class<?>[] {ResultSet.class}, handler);
    }

    private static ColumnMeta col(int jdbcType) {
        return new ColumnMeta("c", jdbcType, 0, 0, true);
    }

    private static Object extract(int jdbcType, Object value, RecordingResultSet handler) throws Exception {
        handler.next = value;
        return RowExtractor.extractValue(proxyFor(handler), col(jdbcType), 1, RowExtractor.newUtcCalendar());
    }

    @Test
    public void testEachTypeUsesItsOwnGetter() throws Exception {
        Object[][] cases = {
            {Types.DECIMAL, new BigDecimal("1.5"), "getBigDecimal"},
            {Types.NUMERIC, new BigDecimal("2.5"), "getBigDecimal"},
            {Types.DATE, new Date(0L), "getDate"},
            {Types.TIMESTAMP, new Timestamp(0L), "getTimestamp"},
            {Types.BIT, Boolean.TRUE, "getBoolean"},
            {Types.BOOLEAN, Boolean.TRUE, "getBoolean"},
            {Types.TINYINT, (byte) 7, "getByte"},
            {Types.SMALLINT, (short) 8, "getShort"},
            {Types.INTEGER, 9, "getInt"},
            {Types.BIGINT, 10L, "getLong"},
            {Types.REAL, 1.5f, "getFloat"},
            // JDBC: FLOAT is double precision, REAL is the single-precision one.
            {Types.FLOAT, 2.5d, "getDouble"},
            {Types.DOUBLE, 3.5d, "getDouble"},
            {Types.VARCHAR, "s", "getString"},
            {Types.OTHER, "carried as text", "getString"},
        };
        for (Object[] c : cases) {
            RecordingResultSet h = new RecordingResultSet();
            Object got = extract((Integer) c[0], c[1], h);
            assertEquals("value for jdbcType " + c[0], c[1], got);
            assertEquals("getter for jdbcType " + c[0], Arrays.asList(c[2]), h.calls);
        }
    }

    /**
     * BINARY must go through getBytes. getString would charset-decode and lose the original
     * bytes, and the schema side maps these to BYTES -- a mismatch only shows up at serialization.
     */
    @Test
    public void testBinaryTypesReadRawBytes() throws Exception {
        for (int t : new int[] {Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY}) {
            RecordingResultSet h = new RecordingResultSet();
            byte[] bytes = {1, 2, 3};
            Object got = extract(t, bytes, h);
            assertSame(bytes, got);
            assertEquals(Arrays.asList("getBytes"), h.calls);
        }
    }

    /**
     * wasNull is checked after the getter, not before: a primitive getter returns 0/false for a
     * SQL NULL, so without this every null numeric would reach Kafka as a real zero.
     */
    @Test
    public void testSqlNullBecomesNullForPrimitiveGetters() throws Exception {
        for (int t : new int[] {Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT,
                                Types.REAL, Types.DOUBLE, Types.BOOLEAN}) {
            RecordingResultSet h = new RecordingResultSet();
            h.wasNull = true;
            assertNull("jdbcType " + t + " must yield null, not its zero value", extract(t, null, h));
        }
    }

    @Test
    public void testSqlNullBecomesNullForObjectGetters() throws Exception {
        for (int t : new int[] {Types.DECIMAL, Types.DATE, Types.TIMESTAMP, Types.VARCHAR, Types.BINARY}) {
            RecordingResultSet h = new RecordingResultSet();
            h.wasNull = true;
            assertNull(extract(t, null, h));
        }
    }

    /**
     * Temporal reads pass an explicit UTC calendar; without it the driver uses the JVM zone. This
     * pins that we pass it, not that a driver honours it -- the Arrow Flight driver takes the
     * calendar and still resolves through the JVM default zone, which is Limitation 11.
     */
    @Test
    public void testTemporalGettersReceiveTheUtcCalendar() throws Exception {
        Calendar utc = RowExtractor.newUtcCalendar();
        assertEquals("UTC", utc.getTimeZone().getID());

        final List<Object> seen = new ArrayList<>();
        ResultSet rs = (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {ResultSet.class}, (proxy, method, args) -> {
                    if ("wasNull".equals(method.getName())) {
                        return false;
                    }
                    seen.add(args == null || args.length < 2 ? null : args[1]);
                    return "getDate".equals(method.getName()) ? new Date(0L) : new Timestamp(0L);
                });

        RowExtractor.extractValue(rs, col(Types.DATE), 1, utc);
        RowExtractor.extractValue(rs, col(Types.TIMESTAMP), 1, utc);
        assertEquals(Arrays.asList(utc, utc), seen);
    }

    /** Each Calendar is per-read: Calendar is not thread-safe and the poll thread shares nothing. */
    @Test
    public void testEachUtcCalendarIsANewInstance() {
        Calendar a = RowExtractor.newUtcCalendar();
        Calendar b = RowExtractor.newUtcCalendar();
        assertTrue(a != b);
        assertEquals(TimeZone.getTimeZone("UTC"), a.getTimeZone());
    }

    /** extractRow reads exactly the leading columns, leaving a CHANGES query's pseudo-columns. */
    @Test
    public void testExtractRowReadsOnlyTheDeclaredColumns() throws Exception {
        RecordingResultSet h = new RecordingResultSet();
        h.next = 42;
        Object[] row = RowExtractor.extractRow(proxyFor(h), Arrays.asList(col(Types.INTEGER), col(Types.INTEGER)),
                RowExtractor.newUtcCalendar());
        assertArrayEquals(new Object[] {42, 42}, row);
        assertEquals(Arrays.asList("getInt", "getInt"), h.calls);
    }
}
