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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class PipelineBenchTest {
    private static final List<ColumnMeta> COLUMNS = Arrays.asList(
            new ColumnMeta("id", "int", "int", 0, false, true),
            new ColumnMeta("amount", "decimal", "decimal(18,2)", 2, true),
            new ColumnMeta("bytes", "varbinary", "varbinary", 0, true),
            new ColumnMeta("a", "array", "array<int>", 0, true),
            new ColumnMeta("m", "map", "map<varchar(10),int>", 0, true),
            new ColumnMeta("st", "struct", "struct<`x` int>", 0, true));
    private static final Object[] ROW = {7, new BigDecimal("12.34"), new byte[]{1, 2, (byte) 255},
            Arrays.asList(10, null, 20), Collections.singletonMap("k", 11), Collections.singletonMap("x", 9)};

    @Test
    public void converterSettingsProduceClusterJsonShape() throws Exception {
        ChangeRecordMapper mapper = new ChangeRecordMapper("db", "orders", "sr.db.orders", COLUMNS);
        ObjectMapper json = new ObjectMapper();
        try (JsonConverter key = PipelineBench.jsonConverter(true);
                JsonConverter value = PipelineBench.jsonConverter(false)) {
            List<SourceRecord> records = Arrays.asList(
                    mapper.toSnapshotRecord(ROW, 10),
                    mapper.toChangeRecord(ROW, 1, 11, 10, 11),
                    mapper.toChangeRecord(ROW, 0, 11, 10, 11));
            String[] ops = {"r", "d", "c"};
            for (int i = 0; i < records.size(); i++) {
                SourceRecord record = records.get(i);
                JsonNode keyJson = json.readTree(key.fromConnectData(record.topic(), record.keySchema(), record.key()));
                assertEquals(json.readTree("{\"id\":7}"), keyJson);
                JsonNode envelope = json.readTree(value.fromConnectData(
                        record.topic(), record.valueSchema(), record.value()));
                assertFalse(envelope.has("schema"));
                assertEquals(ops[i], envelope.get("op").asText());
                JsonNode row = envelope.get(i == 1 ? "before" : "after");
                assertTrue(envelope.get(i == 1 ? "after" : "before").isNull());
                assertTrue(row.get("amount").isNumber());
                assertEquals(new BigDecimal("12.34"), row.get("amount").decimalValue());
                assertEquals("AQL/", row.get("bytes").asText());
                assertEquals(json.readTree("[10,null,20]"), row.get("a"));
                assertEquals(11, row.get("m").get("k").asInt());
                assertEquals(9, row.get("st").get("x").asInt());
                assertEquals(10, envelope.get("source").get("bookmark").get("base").asInt());
                assertEquals(i == 0 ? 10 : 11,
                        envelope.get("source").get("bookmark").get("head").asInt());
            }
        }
    }

    @Test
    public void pipelineRunsWithAndWithoutStageTimingAndResetsEachRound() throws Exception {
        for (PipelineBench.PipelineMode mode : PipelineBench.PipelineMode.values()) {
            for (boolean phases : new boolean[]{false, true}) {
                try (PipelineBench.RowPipeline pipeline = new PipelineBench.RowPipeline(
                        "db", "orders", COLUMNS, phases, mode)) {
                    pipeline.snapshot(ROW, 10);
                    long snapshotBytes = pipeline.serializedBytes;
                    assertEquals(mode.serializes, snapshotBytes > 0);
                    pipeline.change(ROW, 1, 11, 10, 11);
                    pipeline.change(ROW, 0, 11, 10, 11);
                    assertEquals(3, pipeline.rows);
                    assertEquals(mode.serializes, pipeline.serializedBytes > 2 * snapshotBytes);
                    if (mode == PipelineBench.PipelineMode.MAP) {
                        assertNotNull(pipeline.mappedRecord);
                        assertEquals("c", ((Struct) pipeline.mappedRecord.value()).getString("op"));
                    } else {
                        assertNull(pipeline.mappedRecord);
                    }
                    assertEquals(phases && mode.maps, pipeline.mapNanos > 0);
                    assertEquals(phases && mode.serializes, pipeline.serializeNanos > 0);
                    pipeline.reset();
                    assertNull(pipeline.mappedRecord);
                    assertEquals(0, pipeline.rows);
                    assertEquals(0, pipeline.serializedBytes);
                    assertEquals(0, pipeline.mapNanos);
                    assertEquals(0, pipeline.serializeNanos);
                    pipeline.snapshot(ROW, 10);
                    assertEquals(1, pipeline.rows);
                    assertEquals(snapshotBytes, pipeline.serializedBytes);
                }
            }
        }
    }

    @Test
    public void meterAndReportIncludePipelineWorkWithoutAccumulatingEarlierRounds() throws Exception {
        for (PipelineBench.PipelineMode mode : PipelineBench.PipelineMode.values()) {
            for (boolean phases : new boolean[]{false, true}) {
                PipelineBench.Meter meter = new PipelineBench.Meter(phases);
                PipelineBench.Report report = new PipelineBench.Report(phases, mode);
                try (PipelineBench.RowPipeline pipeline = new PipelineBench.RowPipeline(
                        "db", "orders", COLUMNS, phases, mode)) {
                    for (String transport : Arrays.asList("mysql", "arrow-flight", "arrow-adbc")) {
                        PipelineBench.Sample sample = meter.measure(pipeline, () -> {
                            pipeline.snapshot(ROW, 10);
                            pipeline.change(ROW, 1, 11, 10, 11);
                            pipeline.change(ROW, 0, 11, 10, 11);
                            return pipeline.rows;
                        });
                        assertEquals(3, sample.rows);
                        assertEquals(mode.serializes, sample.serializedBytes > 0);
                        assertTrue(sample.wallNanos >= sample.mapNanos + sample.serializeNanos);
                        assertEquals(phases, sample.timings != null);
                        assertEquals(phases && mode.maps, sample.mapNanos > 0);
                        assertEquals(phases && mode.serializes, sample.serializeNanos > 0);
                        report.add("orders", "snapshot", transport, sample);
                    }
                }
                String rendered = report.render();
                assertTrue(rendered.contains("json MB"));
                assertTrue(rendered.contains("Pipeline: " + mode.label()));
                assertTrue(rendered.contains(mode.scope));
                String summaryRow = Arrays.stream(rendered.split("\n"))
                        .filter(line -> line.startsWith("orders") && line.contains("mysql")).findFirst().get();
                assertEquals(!mode.serializes, summaryRow.trim().endsWith("-"));
                if (phases) {
                    String stageRow = Arrays.stream(rendered.substring(rendered.indexOf("Pipeline stages")).split("\n"))
                            .filter(line -> line.startsWith("orders") && line.contains("mysql")).findFirst().get();
                    String[] fields = stageRow.trim().split("\\s+");
                    assertEquals(!mode.maps, fields[7].equals("-"));
                    assertEquals(!mode.serializes, fields[8].equals("-"));
                }
                assertTrue(rendered.contains("mysql"));
                assertTrue(rendered.contains("arrow-flight"));
                assertTrue(rendered.contains("arrow-adbc"));
                assertEquals(phases, rendered.contains("Pipeline stages (independent p50 ms)"));
                assertEquals(phases, rendered.contains("ADBC batch waits"));
            }
        }
    }

    @Test
    public void reportUsesTheSameMedianForThroughputAndBookmarkLatency() {
        long[] millis = {900, 100, 200};
        double[] expected = {900, 500, 200};
        PipelineBench.Report report = new PipelineBench.Report(false, PipelineBench.PipelineMode.READ);
        for (int i = 0; i < millis.length; i++) {
            PipelineBench.Sample sample = new PipelineBench.Sample();
            sample.rows = 1000;
            sample.wallNanos = millis[i] * 1000000;
            report.add("orders", "snapshot", "mysql", sample);
            String summary = Arrays.stream(report.render().split("\n"))
                    .filter(line -> line.startsWith("orders")).findFirst().get();
            String[] fields = summary.trim().split("\\s+");
            assertEquals(expected[i], Double.parseDouble(fields[4]), 0);
            assertEquals(Math.round(1000000 / expected[i]), Long.parseLong(fields[6]));

            PipelineBench.Report latency = new PipelineBench.Report(false, PipelineBench.PipelineMode.READ);
            long[] nanos = Arrays.stream(Arrays.copyOf(millis, i + 1)).map(ms -> ms * 1000000).toArray();
            latency.latency("bookmark", "mysql", nanos);
            String line = Arrays.stream(latency.render().split("\n"))
                    .filter(row -> row.startsWith("bookmark")).findFirst().get();
            assertEquals(expected[i], Double.parseDouble(line.split("p50\\s+")[1].split("\\s+")[0]), 0);
        }
    }

    @Test
    public void readModeOnlyCountsDecodedRows() {
        try (PipelineBench.RowPipeline pipeline = new PipelineBench.RowPipeline(
                "db", "orders", COLUMNS, true, PipelineBench.PipelineMode.READ)) {
            // These are deliberately not mappable rows; read mode must not invoke the mapper.
            pipeline.snapshot(new Object[0], 10);
            pipeline.change(new Object[0], 1, 11, 10, 11);
            assertEquals(2, pipeline.rows);
            assertEquals(0, pipeline.mapNanos);
            assertEquals(0, pipeline.serializeNanos);
            assertEquals(0, pipeline.serializedBytes);
        }
    }

    @Test
    public void pipelineModeRejectsUnknownValues() {
        for (PipelineBench.PipelineMode mode : PipelineBench.PipelineMode.values()) {
            assertSame(mode, PipelineBench.PipelineMode.parse(mode.label()));
        }
        for (String invalid : Arrays.asList("", "all", "READ")) {
            try {
                PipelineBench.PipelineMode.parse(invalid);
                fail("accepted invalid mode: " + invalid);
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage().contains("bench.pipeline"));
            }
        }
    }
}
