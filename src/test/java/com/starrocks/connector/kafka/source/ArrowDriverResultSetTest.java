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
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.BitVector;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.IntVector;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The real Arrow Flight JDBC driver, in-process: a {@code VectorSchemaRoot} holding one row of
 * every vector shape StarRocks' {@code convert_to_arrow_type_for_flight_sql} produces, turned into
 * the driver's own {@code ResultSet} and read through {@link RowExtractor} exactly as at runtime.
 * The same row's BE text form goes through {@link MysqlTextReader}; the two must agree.
 *
 * <p>This is the test the first live run showed was missing: a fake ResultSet only echoes the
 * shapes one believes the driver returns. Two of those beliefs were wrong (Avatica hands a
 * top-level ARRAY over as {@code java.sql.Array}; a nested map is a List of entries).
 */
public class ArrowDriverResultSetTest {

    private static final int DAYS = 20670;                    // 2026-08-05
    private static final long MICROS = 1785933296123456L;     // 2026-08-05 12:34:56.123456 UTC
    private static final byte[] BYTES = {1, 2, (byte) 0xff};

    private static final ArrowType INT = new ArrowType.Int(32, true);
    private static final ArrowType DATE = new ArrowType.Date(DateUnit.DAY);
    private static final ArrowType TS = new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
    private static final ArrowType UTF8 = ArrowType.Utf8.INSTANCE;
    private static final ArrowType BOOL = ArrowType.Bool.INSTANCE;
    private static final ArrowType BIN = ArrowType.Binary.INSTANCE;
    private static final ArrowType DEC = new ArrowType.Decimal(18, 2, 128);
    private static final ArrowType LARGEINT = new ArrowType.Decimal(38, 0, 128);

    private static final String D = ListVector.DATA_VECTOR_NAME;

    private static Field leaf(String name, ArrowType t) {
        return new Field(name, FieldType.nullable(t), null);
    }

    private static Field key(ArrowType t) {
        return new Field(MapVector.KEY_NAME, FieldType.notNullable(t), null);
    }

    private static Field list(String name, Field element) {
        return new Field(name, FieldType.nullable(new ArrowType.List()), Collections.singletonList(element));
    }

    private static Field map(String name, Field key, Field value) {
        Field entries = new Field(MapVector.DATA_VECTOR_NAME, FieldType.notNullable(new ArrowType.Struct()),
                Arrays.asList(key, value));
        return new Field(name, FieldType.nullable(new ArrowType.Map(false)), Collections.singletonList(entries));
    }

    private static Field struct(String name, Field... fields) {
        return new Field(name, FieldType.nullable(new ArrowType.Struct()), Arrays.asList(fields));
    }

    /** Columns in the order they are declared below; COLUMN_TYPE spelled as FE prints it. */
    private static final String[][] COLUMNS = {
        {"d", "date", "date"},
        {"ts", "datetime", "datetime"},
        {"big", "bigint unsigned", "bigint(20) unsigned"},
        {"arr", "array", "array<int(11)>"},
        {"ad", "array", "array<date>"},
        {"adt", "array", "array<datetime>"},
        {"mi", "map", "map<int(11),int(11)>"},
        {"ab", "array", "array<varbinary>"},
        {"abool", "array", "array<boolean>"},
        {"sd", "struct", "struct<`d` date, `j` json>"},
        {"am", "array", "array<map<varchar(10),int(11)>>"},
        {"aa", "array", "array<array<int(11)>>"},
        {"adec", "array", "array<DECIMAL64(18,2)>"},
        {"m", "map", "map<varchar(10),int(11)>"},
    };

    /** The same row as BE's MySQL-protocol text prints it (nested VARBINARY hex, nested JSON in ''). */
    private static final Map<String, String> BE_TEXT = new LinkedHashMap<>();

    static {
        BE_TEXT.put("arr", "[10,20,30]");
        BE_TEXT.put("ad", "[\"2026-08-05\"]");
        BE_TEXT.put("adt", "[\"2026-08-05 12:34:56.123456\"]");
        BE_TEXT.put("mi", "{1:2}");
        BE_TEXT.put("ab", "[\"0102ff\"]");
        BE_TEXT.put("abool", "[1,0]");
        BE_TEXT.put("sd", "{\"d\":\"2026-08-05\",\"j\":'{\"a\": 1}'}");
        BE_TEXT.put("am", "[{\"k\":1}]");
        BE_TEXT.put("aa", "[[1,2],[]]");
        BE_TEXT.put("adec", "[1.50]");
        BE_TEXT.put("m", "{\"mk\":11}");
    }

    private static Schema schema() {
        return new Schema(Arrays.asList(
                leaf("d", DATE), leaf("ts", TS), leaf("big", LARGEINT),
                list("arr", leaf(D, INT)), list("ad", leaf(D, DATE)), list("adt", leaf(D, TS)),
                map("mi", key(INT), leaf(MapVector.VALUE_NAME, INT)),
                list("ab", leaf(D, BIN)), list("abool", leaf(D, BOOL)),
                struct("sd", leaf("d", DATE), leaf("j", UTF8)),
                list("am", map(D, key(UTF8), leaf(MapVector.VALUE_NAME, INT))),
                list("aa", list(D, leaf(D, INT))),
                list("adec", leaf(D, DEC)),
                map("m", key(UTF8), leaf(MapVector.VALUE_NAME, INT))));
    }

    private static void fillRow(VectorSchemaRoot root) {
        ((DateDayVector) root.getVector("d")).setSafe(0, DAYS);
        ((TimeStampMicroTZVector) root.getVector("ts")).setSafe(0, MICROS);
        ((DecimalVector) root.getVector("big")).setSafe(0, new BigDecimal("99999999999999999999999999999999999999"));

        ListVector arr = (ListVector) root.getVector("arr");
        int o = arr.startNewValue(0);
        IntVector arrData = (IntVector) arr.getDataVector();
        arrData.setSafe(o, 10);
        arrData.setSafe(o + 1, 20);
        arrData.setSafe(o + 2, 30);
        arr.endValue(0, 3);

        ListVector ad = (ListVector) root.getVector("ad");
        o = ad.startNewValue(0);
        ((DateDayVector) ad.getDataVector()).setSafe(o, DAYS);
        ad.endValue(0, 1);

        ListVector adt = (ListVector) root.getVector("adt");
        o = adt.startNewValue(0);
        ((TimeStampMicroTZVector) adt.getDataVector()).setSafe(o, MICROS);
        adt.endValue(0, 1);

        MapVector mi = (MapVector) root.getVector("mi");
        o = mi.startNewValue(0);
        StructVector miEntries = (StructVector) mi.getDataVector();
        ((IntVector) miEntries.getChild(MapVector.KEY_NAME)).setSafe(o, 1);
        ((IntVector) miEntries.getChild(MapVector.VALUE_NAME)).setSafe(o, 2);
        miEntries.setIndexDefined(o);
        mi.endValue(0, 1);

        ListVector ab = (ListVector) root.getVector("ab");
        o = ab.startNewValue(0);
        ((VarBinaryVector) ab.getDataVector()).setSafe(o, BYTES);
        ab.endValue(0, 1);

        ListVector abool = (ListVector) root.getVector("abool");
        o = abool.startNewValue(0);
        BitVector bits = (BitVector) abool.getDataVector();
        bits.setSafe(o, 1);
        bits.setSafe(o + 1, 0);
        abool.endValue(0, 2);

        StructVector sd = (StructVector) root.getVector("sd");
        ((DateDayVector) sd.getChild("d")).setSafe(0, DAYS);
        ((VarCharVector) sd.getChild("j")).setSafe(0, "{\"a\": 1}".getBytes(StandardCharsets.UTF_8));
        sd.setIndexDefined(0);

        ListVector am = (ListVector) root.getVector("am");
        o = am.startNewValue(0);
        MapVector amInner = (MapVector) am.getDataVector();
        int io = amInner.startNewValue(o);
        StructVector amEntries = (StructVector) amInner.getDataVector();
        ((VarCharVector) amEntries.getChild(MapVector.KEY_NAME)).setSafe(io, "k".getBytes(StandardCharsets.UTF_8));
        ((IntVector) amEntries.getChild(MapVector.VALUE_NAME)).setSafe(io, 1);
        amEntries.setIndexDefined(io);
        amInner.endValue(o, 1);
        am.endValue(0, 1);

        ListVector aa = (ListVector) root.getVector("aa");
        o = aa.startNewValue(0);
        ListVector aaInner = (ListVector) aa.getDataVector();
        io = aaInner.startNewValue(o);
        IntVector aaData = (IntVector) aaInner.getDataVector();
        aaData.setSafe(io, 1);
        aaData.setSafe(io + 1, 2);
        aaInner.endValue(o, 2);
        aaInner.startNewValue(o + 1);
        aaInner.endValue(o + 1, 0);
        aa.endValue(0, 2);

        ListVector adec = (ListVector) root.getVector("adec");
        o = adec.startNewValue(0);
        ((DecimalVector) adec.getDataVector()).setSafe(o, new BigDecimal("1.50"));
        adec.endValue(0, 1);

        MapVector m = (MapVector) root.getVector("m");
        o = m.startNewValue(0);
        StructVector mEntries = (StructVector) m.getDataVector();
        ((VarCharVector) mEntries.getChild(MapVector.KEY_NAME)).setSafe(o, "mk".getBytes(StandardCharsets.UTF_8));
        ((IntVector) mEntries.getChild(MapVector.VALUE_NAME)).setSafe(o, 11);
        mEntries.setIndexDefined(o);
        m.endValue(0, 1);

        root.setRowCount(1);
    }

    private static List<ColumnMeta> columns() {
        List<ColumnMeta> cols = new ArrayList<>();
        for (String[] c : COLUMNS) {
            cols.add(new ColumnMeta(c[0], c[1], c[2], 0, true));
        }
        return cols;
    }

    /** byte[] compares by identity; hex it so nested binaries compare by content. */
    private static Object comparable(Object v) {
        if (v instanceof byte[]) {
            StringBuilder sb = new StringBuilder();
            for (byte b : (byte[]) v) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        }
        if (v instanceof List) {
            List<Object> out = new ArrayList<>();
            for (Object o : (List<?>) v) {
                out.add(comparable(o));
            }
            return out;
        }
        if (v instanceof Map) {
            Map<Object, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                out.put(e.getKey(), comparable(e.getValue()));
            }
            return out;
        }
        return v;
    }

    /**
     * Every nested column read through the real driver must equal the same value read from BE's
     * text, and the top-level DATE and DATETIME must print the stored digits whatever zone the
     * worker runs in. The default zone is forced to UTC+8 so a zone leak cannot hide.
     */
    @Test
    public void testRealDriverRowMatchesTheMysqlTextRow() throws Exception {
        TimeZone previous = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema(), allocator)) {
            root.allocateNew();
            fillRow(root);
            List<ColumnMeta> cols = columns();

            try (ResultSet rs = ArrowFlightJdbcVectorSchemaRootResultSet.fromVectorSchemaRoot(root)) {
                assertTrue(rs.next());
                Object[] row = new ArrowValueReader().readRow(rs, cols);

                // Canonical already: the extractor formats temporals, so the row itself is comparable.
                assertEquals("2026-08-05", row[0]);
                assertEquals("2026-08-05 12:34:56.123456", row[1]);
                assertEquals(new BigDecimal("99999999999999999999999999999999999999"), row[2]);

                for (int i = 3; i < cols.size(); i++) {
                    ColumnMeta col = cols.get(i);
                    Object viaText = new MysqlValueReader().nested(col.type, BE_TEXT.get(col.name));
                    assertEquals("column " + col.name + " (" + col.srColumnType + ")",
                            comparable(viaText), comparable(row[i]));
                }
            }
        } finally {
            TimeZone.setDefault(previous);
        }
    }

    /** The one place the extractor branches on the driver: what getObject hands over per column. */
    @Test
    public void testAvaticaShapesArrowHandsOver() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema(), allocator)) {
            root.allocateNew();
            fillRow(root);
            try (ResultSet rs = ArrowFlightJdbcVectorSchemaRootResultSet.fromVectorSchemaRoot(root)) {
                assertTrue(rs.next());
                assertTrue("top-level ARRAY is Types.ARRAY and comes as java.sql.Array",
                        rs.getObject(4) instanceof java.sql.Array);
                assertTrue("top-level MAP is JAVA_OBJECT and comes as a Map",
                        rs.getObject(7) instanceof Map);
                assertTrue("top-level STRUCT is JAVA_OBJECT and comes as a Map",
                        rs.getObject(10) instanceof Map);
                Object[] am = (Object[]) ((java.sql.Array) rs.getObject(11)).getArray();
                assertTrue("a nested map is MapVector.getObject: a List of entries, not a Map",
                        am[0] instanceof List);
                // The shift itself, pinned: without the transport flag a UTC+8 JVM reads 12:34:56
                // back as 04:34:56Z. This is what fromJvmWallClock undoes.
                TimeZone previous = TimeZone.getDefault();
                TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
                try {
                    java.sql.Timestamp raw = rs.getTimestamp(2, java.util.Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                    assertEquals("2026-08-05 04:34:56.123456", ValueReader.dateTimeText(raw));
                    assertEquals("2026-08-05 12:34:56.123456", ValueReader.dateTimeText(ArrowValueReader.fromJvmWallClock(raw)));
                } finally {
                    TimeZone.setDefault(previous);
                }
                assertEquals(Types.ARRAY, rs.getMetaData().getColumnType(4));
                assertEquals(Types.JAVA_OBJECT, rs.getMetaData().getColumnType(7));
            }
        }
    }
}
