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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ColumnTypeConnectValueTest {
    private static ColumnType type(String sql) {
        return ColumnTypeParser.parse(sql).get();
    }

    @Test
    public void scalarContainersAreAlreadyConnectValues() {
        ColumnType type = type("array<map<int,array<int>>>");
        Schema schema = type.toConnectSchema("test.value", true);
        List<?> input = Arrays.asList(Collections.singletonMap("1", Arrays.asList(2, null)), null);
        ColumnType.ConnectValueConverter converter = type.bindConnectValue(schema);
        assertSame(input, converter.convert(input));
        assertNull(converter.convert(null));
        org.apache.kafka.connect.data.ConnectSchema.validateValue(schema, converter.convert(input));
    }

    @Test
    public void onlyPathsToStructsAreRebuiltAndBindingsStayWithTheirSchema() {
        ColumnType type = type("map<int,array<struct<`n` int, `a` array<int>, `m` map<int,int>>>>");
        Schema schema = type.toConnectSchema("first.value", true);
        Schema otherSchema = type.toConnectSchema("second.value", true);
        List<Integer> leaves = Arrays.asList(1, null, 3);
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("n", 7);
        fields.put("a", leaves);
        Map<String, Integer> scalarMap = Collections.singletonMap("2", 8);
        fields.put("m", scalarMap);
        List<?> elements = Arrays.asList(fields, null, Collections.emptyMap());
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("1", elements);
        input.put("2", null);
        input.put("3", Collections.emptyList());
        ColumnType.ConnectValueConverter converter = type.bindConnectValue(schema);
        Map<?, ?> output = (Map<?, ?>) converter.convert(input);
        assertNotSame(input, output);
        List<?> converted = (List<?>) output.get("1");
        assertNotSame(elements, converted);
        Struct struct = (Struct) converted.get(0);
        assertEquals(7, struct.get("n"));
        assertSame(leaves, struct.get("a"));
        assertSame(scalarMap, struct.get("m"));
        assertNull(converted.get(1));
        assertNull(((Struct) converted.get(2)).get("n"));
        assertNull(output.get("2"));
        assertEquals(Collections.emptyList(), output.get("3"));
        assertSame(fields, elements.get(0)); // Input was not changed into a Struct in place.
        org.apache.kafka.connect.data.ConnectSchema.validateValue(schema, output);
        Map<?, ?> otherOutput = (Map<?, ?>) type.bindConnectValue(otherSchema).convert(input);
        Struct other = (Struct) ((List<?>) otherOutput.get("1")).get(0);
        assertSame(otherSchema.valueSchema().valueSchema(), other.schema());
        assertSame(schema.valueSchema().valueSchema(), struct.schema());
        assertNotSame(struct, ((List<?>) ((Map<?, ?>) converter.convert(input)).get("1")).get(0));
    }

    @Test(expected = DataException.class)
    public void boundStructFieldsStillValidateValues() {
        ColumnType type = type("struct<`n` int>");
        type.bindConnectValue(type.toConnectSchema("test.value", true))
                .convert(Collections.singletonMap("n", "not an integer"));
    }
}
