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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * The single funnel every value passes through on both read paths. The getter chosen here comes
 * from the {@link ColumnType} the Connect schema was built from; picking a different one surfaces
 * as a DataException at serialization time, far from the cause. Driven through the MySQL reader
 * unless the Arrow reader is the point.
 */
public class ValueReaderTest {

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
        return (ResultSet) Proxy.newProxyInstance(ValueReaderTest.class.getClassLoader(),
                new Class<?>[] {ResultSet.class}, handler);
    }

    private static ColumnMeta col(int jdbcType) {
        return col(jdbcType, 0);
    }

    private static ColumnMeta col(int jdbcType, int scale) {
        return new ColumnMeta("c", jdbcType, 0, scale, true);
    }

    private static Object extract(int jdbcType, Object value, RecordingResultSet handler) throws Exception {
        handler.next = value;
        return new MysqlValueReader().read(proxyFor(handler), 1, col(jdbcType).type);
    }

    @Test
    public void testEachTypeUsesItsOwnGetter() throws Exception {
        Object[][] cases = {
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
     * Decimals and temporals leave here canonical, so both transports meet in an identical row: the
     * BigDecimal at the declared scale, DATE and DATETIME as StarRocks text with the microseconds.
     */
    @Test
    public void testDecimalsAndTemporalsAreCanonicalizedHere() throws Exception {
        ValueReader reader = new MysqlValueReader();
        RecordingResultSet h = new RecordingResultSet();
        h.next = new BigDecimal("1.5");
        assertEquals(new BigDecimal("1.50"), reader.read(proxyFor(h), 1, col(Types.DECIMAL, 2).type));
        assertEquals(Arrays.asList("getBigDecimal"), h.calls);

        h = new RecordingResultSet();
        h.next = new BigDecimal("7");
        assertEquals(new BigDecimal("7"), reader.read(proxyFor(h), 1, col(Types.NUMERIC).type));

        h = new RecordingResultSet();
        h.next = new Date(0L);
        assertEquals("1970-01-01", reader.read(proxyFor(h), 1, col(Types.DATE).type));
        assertEquals(Arrays.asList("getDate"), h.calls);

        Timestamp withMicros = new Timestamp(0L);
        withMicros.setNanos(123_456_000);
        h = new RecordingResultSet();
        h.next = withMicros;
        assertEquals("1970-01-01 00:00:00.123456", reader.read(proxyFor(h), 1, col(Types.TIMESTAMP).type));
        assertEquals(Arrays.asList("getTimestamp"), h.calls);
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
     * calendar and still builds the value in the JVM default zone, which its reader undoes.
     */
    @Test
    public void testTemporalGettersReceiveTheUtcCalendar() throws Exception {
        final List<Object> seen = new ArrayList<>();
        ResultSet rs = (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {ResultSet.class}, (proxy, method, args) -> {
                    if ("wasNull".equals(method.getName())) {
                        return false;
                    }
                    seen.add(args == null || args.length < 2 ? null : args[1]);
                    return "getDate".equals(method.getName()) ? new Date(0L) : new Timestamp(0L);
                });

        ValueReader reader = new MysqlValueReader();
        reader.read(rs, 1, col(Types.DATE).type);
        reader.read(rs, 1, col(Types.TIMESTAMP).type);
        assertEquals(2, seen.size());
        for (Object cal : seen) {
            assertEquals("UTC", ((Calendar) cal).getTimeZone().getID());
        }
    }

    /**
     * The Arrow driver hands back Timestamp.valueOf(digits) in the JVM zone whatever calendar it
     * got; the Arrow reader routes that through fromJvmWallClock. The MySQL reader takes the value
     * as MariaDB gave it, which is right there.
     */
    @Test
    public void testArrowReaderUndoesItsDriversZoneShift() throws Exception {
        TimeZone previous = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        try {
            Timestamp shifted = Timestamp.valueOf(java.time.LocalDateTime.of(2026, 8, 5, 12, 34, 56, 123_456_000));
            ResultSet rs = (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {ResultSet.class},
                    (proxy, method, args) -> "wasNull".equals(method.getName()) ? Boolean.FALSE : shifted);
            ColumnType ts = col(Types.TIMESTAMP).type;
            assertEquals("2026-08-05 12:34:56.123456", new ArrowValueReader().read(rs, 1, ts));
            assertEquals("2026-08-05 04:34:56.123456", new MysqlValueReader().read(rs, 1, ts));
        } finally {
            TimeZone.setDefault(previous);
        }
    }

    /** Each reader is per-read and owns its Calendar: Calendar is not thread-safe. */
    @Test
    public void testEachReaderOwnsAUtcCalendar() {
        ValueReader a = ValueReader.forTransport(false);
        ValueReader b = ValueReader.forTransport(false);
        assertNotSame(a.utcCalendar(), b.utcCalendar());
        assertEquals(TimeZone.getTimeZone("UTC"), a.utcCalendar().getTimeZone());
    }

    /** The transport picks the reader; nothing downstream has to sniff the driver. */
    @Test
    public void testTransportPicksTheReader() {
        assertTrue(ValueReader.forTransport(true) instanceof ArrowValueReader);
        assertTrue(ValueReader.forTransport(false) instanceof MysqlValueReader);
    }

    /**
     * A nested column is read with getObject; the MySQL reader gets the BE text, the Arrow reader
     * the vector's List. Both end in the same neutral value.
     */
    @Test
    public void testNestedColumnReadsTheSameThroughEitherReader() throws Exception {
        ColumnMeta nested = new ColumnMeta("arr", Types.OTHER, 0, 0, true, "array", "array<date>");

        RecordingResultSet mysql = new RecordingResultSet();
        mysql.next = "[\"2026-08-05\",null]";
        Object fromText = new MysqlValueReader().read(proxyFor(mysql), 1, nested.type);
        assertEquals(Arrays.asList("getObject"), mysql.calls);

        RecordingResultSet arrow = new RecordingResultSet();
        arrow.next = Arrays.asList(20670, null);
        Object fromArrow = new ArrowValueReader().read(proxyFor(arrow), 1, nested.type);
        assertEquals(Arrays.asList("getObject"), arrow.calls);

        assertEquals(Arrays.asList("2026-08-05", null), fromText);
        assertEquals(fromText, fromArrow);
    }

    /**
     * Avatica's getObject hands a top-level ARRAY over as java.sql.Array (its dispatch is by JDBC
     * type id), and getArray() holds the vector's element objects. The first live run died here.
     */
    @Test
    public void testArrowReaderUnwrapsATopLevelJavaSqlArray() throws Exception {
        ColumnMeta nested = new ColumnMeta("arr", Types.OTHER, 0, 0, true, "array", "array<date>");
        java.sql.Array array = (java.sql.Array) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {java.sql.Array.class}, (proxy, method, args) ->
                        "getArray".equals(method.getName()) && (args == null || args.length == 0)
                                ? new Object[] {20670, null} : null);
        RecordingResultSet h = new RecordingResultSet();
        h.next = array;
        assertEquals(Arrays.asList("2026-08-05", null), new ArrowValueReader().read(proxyFor(h), 1, nested.type));
        assertEquals(Arrays.asList("getObject"), h.calls);
    }

    /** A complex column whose COLUMN_TYPE did not parse is OPAQUE and keeps the getString path. */
    @Test
    public void testUnparsedComplexColumnStillReadsText() throws Exception {
        ColumnMeta unparsed = new ColumnMeta("s", Types.OTHER, 0, 0, true, "struct", "struct<x int>");
        RecordingResultSet h = new RecordingResultSet();
        h.next = "{\"x\":1}";
        assertEquals("{\"x\":1}", new MysqlValueReader().read(proxyFor(h), 1, unparsed.type));
        assertEquals(Arrays.asList("getString"), h.calls);
    }

    /** readRow reads exactly the leading columns, leaving a CHANGES query's pseudo-columns. */
    @Test
    public void testReadRowReadsOnlyTheDeclaredColumns() throws Exception {
        RecordingResultSet h = new RecordingResultSet();
        h.next = 42;
        Object[] row = new MysqlValueReader().readRow(proxyFor(h), Arrays.asList(col(Types.INTEGER), col(Types.INTEGER)));
        assertArrayEquals(new Object[] {42, 42}, row);
        assertEquals(Arrays.asList("getInt", "getInt"), h.calls);
    }
}
