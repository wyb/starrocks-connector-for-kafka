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

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcVectorSchemaRootResultSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.result.CompleteResult;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Exercises actual JDBC ResultSets and ADBC vectors without requiring a running server. */
public class JdbcTemporalValueReaderTest {
    private static final String[][] ROWS = {
        {"2026-03-08", "2026-03-08 02:30:00.123456"}, // Los Angeles DST gap
        {"2026-11-01", "2026-11-01 01:30:00.120000"}, // Los Angeles DST overlap
        {"1991-04-14", "1991-04-14 02:30:00"},        // Shanghai historical DST gap
        {"1500-01-01", "1500-01-01 12:34:56.123456"}, // Julian/Gregorian mismatch
        {"1582-10-10", "1582-10-10 12:34:56"},        // GregorianCalendar cutover gap
        {"0000-01-01", "0000-01-01 00:00:00"},
        {"0001-01-01", "0001-01-01 00:00:00"},
        {"1969-12-31", "1969-12-31 23:59:59.999999"},
        {"1970-01-01", "1970-01-01 00:00:00.000001"},
        {"2000-02-29", "2000-02-29 12:34:56.120000"},
        {"9999-12-31", "9999-12-31 23:59:59.999999"},
        {null, null},
    };
    private static final List<ColumnMeta> COLUMNS = Arrays.asList(
            new ColumnMeta("d", "date", "date", 0, true),
            new ColumnMeta("ts", "datetime", "datetime", 0, true));

    @Test
    public void testMysqlTemporalTextAcrossTimeZones() throws Exception {
        TimeZone previous = TimeZone.getDefault();
        try {
            for (String zone : new String[] {"UTC", "Asia/Shanghai", "America/Los_Angeles"}) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                // Only the connection context is stubbed; metadata, row decoding and getters are real.
                Context context = (Context) Proxy.newProxyInstance(getClass().getClassLoader(),
                        new Class<?>[] {Context.class}, (proxy, method, args) -> {
                            if ("getExceptionFactory".equals(method.getName())) {
                                return null;
                            }
                            throw new AssertionError("Unexpected connection access: " + method.getName());
                        });
                try (ResultSet rs = CompleteResult.createResultSet(new String[] {"d", "ts"},
                        new DataType[] {DataType.DATE, DataType.DATETIME}, ROWS,
                        context, 33, ResultSet.TYPE_FORWARD_ONLY)) {
                    JdbcRowReader reader = JdbcRowReader.forTransport(false);
                    for (String[] expected : ROWS) {
                        assertTrue(rs.next());
                        assertArrayEquals(zone, expected, reader.readRow(rs, COLUMNS));
                    }
                    assertFalse(rs.next());
                }
            }
        } finally {
            TimeZone.setDefault(previous);
        }
    }

    @Test
    public void testArrowJdbcAndAdbcTemporalParityAcrossTimeZones() throws Exception {
        TimeZone previous = TimeZone.getDefault();
        Schema schema = new Schema(Arrays.asList(
                new Field("d", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null),
                new Field("ts", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")), null)));
        try {
            for (String zone : new String[] {"UTC", "Asia/Shanghai", "America/Los_Angeles"}) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                try (BufferAllocator allocator = new RootAllocator();
                     VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                    root.allocateNew();
                    DateDayVector dates = (DateDayVector) root.getVector(0);
                    TimeStampMicroTZVector times = (TimeStampMicroTZVector) root.getVector(1);
                    for (int row = 0; row < ROWS.length; row++) {
                        if (ROWS[row][0] == null) {
                            dates.setNull(row);
                            times.setNull(row);
                        } else {
                            dates.setSafe(row, (int) LocalDate.parse(ROWS[row][0]).toEpochDay());
                            LocalDateTime time = LocalDateTime.parse(ROWS[row][1].replace(' ', 'T'));
                            times.setSafe(row, time.toEpochSecond(ZoneOffset.UTC) * 1_000_000L
                                    + time.getNano() / 1_000);
                        }
                    }
                    root.setRowCount(ROWS.length);
                    JdbcRowReader jdbc = JdbcRowReader.forTransport(true);
                    ArrowAdbcValueReader adbc = new ArrowAdbcValueReader();
                    try (ResultSet rs = ArrowFlightJdbcVectorSchemaRootResultSet.fromVectorSchemaRoot(root)) {
                        for (int row = 0; row < ROWS.length; row++) {
                            assertTrue(rs.next());
                            assertArrayEquals(zone + " JDBC row " + row, ROWS[row], jdbc.readRow(rs, COLUMNS));
                            assertArrayEquals(zone + " ADBC row " + row, ROWS[row],
                                    adbc.readRow(root.getFieldVectors(), row, COLUMNS));
                        }
                        assertFalse(rs.next());
                    }
                }
            }
        } finally {
            TimeZone.setDefault(previous);
        }
    }
}
