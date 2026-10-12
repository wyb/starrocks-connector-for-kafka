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
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

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

    private static ColumnMeta col(String dataType) {
        return col(dataType, dataType, 0);
    }

    private static ColumnMeta col(String dataType, String columnType, int scale) {
        return new ColumnMeta("c", dataType, columnType, scale, true);
    }

    private static ColumnMeta bool() {
        return col("tinyint", "tinyint(1)", 0);
    }

    private static Object extract(ColumnMeta col, Object value, RecordingResultSet handler) throws Exception {
        handler.next = value;
        return JdbcRowReader.forTransport(false).read(proxyFor(handler), 1, col.type);
    }

    @Test
    public void testEachTypeUsesItsOwnGetter() throws Exception {
        Object[][] cases = {
            {bool(), Boolean.TRUE, "getBoolean"},
            {col("tinyint"), (byte) 7, "getByte"},
            {col("smallint"), (short) 8, "getShort"},
            {col("int"), 9, "getInt"},
            {col("bigint"), 10L, "getLong"},
            {col("float"), 1.5f, "getFloat"},
            {col("double"), 3.5d, "getDouble"},
            {col("varchar"), "s", "getString"},
            {col("variant"), "carried as text", "getString"},
        };
        for (Object[] c : cases) {
            RecordingResultSet h = new RecordingResultSet();
            ColumnMeta col = (ColumnMeta) c[0];
            Object got = extract(col, c[1], h);
            assertEquals("value for " + col.type, c[1], got);
            assertEquals("getter for " + col.type, Arrays.asList(c[2]), h.calls);
        }
    }

    /**
     * Decimals and temporals leave here canonical, so both transports meet in an identical row: the
     * BigDecimal at the declared scale, DATE and DATETIME as StarRocks text with the microseconds.
     */
    @Test
    public void testDecimalsAndTemporalsAreCanonicalizedHere() throws Exception {
        JdbcRowReader reader = JdbcRowReader.forTransport(false);
        RecordingResultSet h = new RecordingResultSet();
        h.next = new BigDecimal("1.5");
        assertEquals(new BigDecimal("1.50"), reader.read(proxyFor(h), 1, col("decimal", "decimal(18, 2)", 2).type));
        assertEquals(Arrays.asList("getBigDecimal"), h.calls);

        h = new RecordingResultSet();
        h.next = new BigDecimal("7");
        assertEquals(new BigDecimal("7"), reader.read(proxyFor(h), 1, col("decimal", "decimal(9, 0)", 0).type));

        h = new RecordingResultSet();
        h.next = "1970-01-01";
        assertEquals("1970-01-01", reader.read(proxyFor(h), 1, col("date").type));
        assertEquals(Arrays.asList("getString"), h.calls);

        h = new RecordingResultSet();
        h.next = "1970-01-01 00:00:00.123456";
        assertEquals("1970-01-01 00:00:00.123456", reader.read(proxyFor(h), 1, col("datetime").type));
        assertEquals(Arrays.asList("getString"), h.calls);
    }

    /**
     * BINARY must go through getBytes. getString would charset-decode and lose the original
     * bytes, and the schema side maps these to BYTES -- a mismatch only shows up at serialization.
     */
    @Test
    public void testBinaryTypesReadRawBytes() throws Exception {
        for (ColumnMeta t : new ColumnMeta[] {col("binary", "binary(4)", 0), col("varbinary", "varbinary(16)", 0)}) {
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
        for (ColumnMeta t : new ColumnMeta[] {col("tinyint"), col("smallint"), col("int"), col("bigint"),
                                              col("float"), col("double"), bool()}) {
            RecordingResultSet h = new RecordingResultSet();
            h.wasNull = true;
            assertNull(t.type + " must yield null, not its zero value", extract(t, null, h));
        }
    }

    @Test
    public void testSqlNullBecomesNullForObjectGetters() throws Exception {
        for (ColumnMeta t : new ColumnMeta[] {col("decimal", "decimal(18, 2)", 2), col("date"), col("datetime"),
                                              col("varchar"), col("binary")}) {
            RecordingResultSet h = new RecordingResultSet();
            h.wasNull = true;
            assertNull(extract(t, null, h));
        }
    }

    /**
     * A nested column is read with getObject; the MySQL reader gets the BE text, the Arrow reader
     * the vector's List. Both end in the same neutral value.
     */
    @Test
    public void testNestedColumnReadsTheSameThroughEitherReader() throws Exception {
        ColumnMeta nested = new ColumnMeta("arr", "array", "array<date>", 0, true);

        RecordingResultSet mysql = new RecordingResultSet();
        mysql.next = "[\"2026-08-05\",null]";
        Object fromText = JdbcRowReader.forTransport(false).read(proxyFor(mysql), 1, nested.type);
        assertEquals(Arrays.asList("getObject"), mysql.calls);

        RecordingResultSet arrow = new RecordingResultSet();
        arrow.next = Arrays.asList(20670, null);
        Object fromArrow = JdbcRowReader.forTransport(true).read(proxyFor(arrow), 1, nested.type);
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
        ColumnMeta nested = new ColumnMeta("arr", "array", "array<date>", 0, true);
        java.sql.Array array = (java.sql.Array) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {java.sql.Array.class}, (proxy, method, args) ->
                        "getArray".equals(method.getName()) && (args == null || args.length == 0)
                                ? new Object[] {20670, null} : null);
        RecordingResultSet h = new RecordingResultSet();
        h.next = array;
        assertEquals(Arrays.asList("2026-08-05", null), JdbcRowReader.forTransport(true).read(proxyFor(h), 1, nested.type));
        assertEquals(Arrays.asList("getObject"), h.calls);
    }

    /** A complex column whose COLUMN_TYPE did not parse is OPAQUE and keeps the getString path. */
    @Test
    public void testUnparsedComplexColumnStillReadsText() throws Exception {
        ColumnMeta unparsed = new ColumnMeta("s", "struct", "struct<x int>", 0, true);
        RecordingResultSet h = new RecordingResultSet();
        h.next = "{\"x\":1}";
        assertEquals("{\"x\":1}", JdbcRowReader.forTransport(false).read(proxyFor(h), 1, unparsed.type));
        assertEquals(Arrays.asList("getString"), h.calls);
    }

    /** readRow reads exactly the leading columns, leaving a CHANGES query's pseudo-columns. */
    @Test
    public void testReadRowReadsOnlyTheDeclaredColumns() throws Exception {
        RecordingResultSet h = new RecordingResultSet();
        h.next = 42;
        Object[] row = JdbcRowReader.forTransport(false).readRow(proxyFor(h), Arrays.asList(col("int"), col("int")));
        assertArrayEquals(new Object[] {42, 42}, row);
        assertEquals(Arrays.asList("getInt", "getInt"), h.calls);
    }

    /** The fraction is absent for whole seconds and otherwise has exactly six digits. */
    @Test
    public void testDateTimeTextFractionFollowsStarRocks() {
        for (String text : new String[] {"2026-08-05 12:34:56", "2026-08-05 12:34:56.123456",
                "2026-08-05 12:34:56.120000", "1969-12-31 23:59:59.999999",
                "1500-01-01 00:00:00", "0000-01-01 00:00:00"}) {
            assertEquals(text, ValueReader.dateTimeText(LocalDateTime.parse(text.replace(' ', 'T'))));
        }
    }
}
