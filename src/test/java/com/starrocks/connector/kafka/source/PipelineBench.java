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

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assume;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Snapshot and CHANGES reads over selected transports: same tables, same bookmark, same JVM,
 * transports alternating round by round. One pipeline mode applies to all selected transports; no Kafka sends.
 *
 * <p>Properties: {@code bench.db}, {@code bench.tables} (csv), {@code bench.user} (root),
 * {@code bench.password}, {@code bench.transports} (mysql,arrow-flight by default),
 * {@code bench.pipeline} (read by default; read, map, serialize), {@code bench.adbc.uri},
 * {@code bench.rounds} (3, first dropped), {@code bench.meta.iterations} (20),
 * {@code bench.daily.update.rows} and {@code bench.burst.update.rows} (disjoint ID ranges, two
 * CHANGES windows), or {@code bench.mutation.sql} (one custom CHANGES window with
 * {@code {db}}/{@code {table}} substituted), {@code bench.read.timings} (false; read, map and serialize stages).
 * Output also goes to {@code target/pipeline-bench.txt}.
 */
public class PipelineBench {

    private static final long BOOKMARK_TTL_MS = 3_600_000L;

    enum PipelineMode {
        READ(false, false, "read + decode"),
        MAP(true, false, "read + decode + SourceRecord map"),
        SERIALIZE(true, true, "read + decode + SourceRecord map + key/value JSON serialization");

        final boolean maps;
        final boolean serializes;
        final String scope;

        PipelineMode(boolean maps, boolean serializes, String scope) {
            this.maps = maps;
            this.serializes = serializes;
            this.scope = scope;
        }

        static PipelineMode parse(String value) {
            for (PipelineMode mode : values()) {
                if (mode.label().equals(value)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("bench.pipeline must be read, map, or serialize: " + value);
        }

        String label() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    @Test
    public void run() throws Exception {
        PipelineMode mode = PipelineMode.parse(System.getProperty("bench.pipeline", "read"));
        String mysqlUrl = System.getProperty("bench.mysql.url");
        String arrowUrl = System.getProperty("bench.arrow.url");
        List<String> selected = split(System.getProperty("bench.transports", "mysql,arrow-flight"));
        Assume.assumeTrue("set -Dbench.mysql.url to run the pipeline bench", mysqlUrl != null);
        if (selected.isEmpty()) {
            throw new IllegalArgumentException("bench.transports must name at least one transport");
        }
        String db = required("bench.db");
        List<String> tables = split(required("bench.tables"));
        String user = System.getProperty("bench.user", "root");
        String password = System.getProperty("bench.password", "");
        int rounds = Integer.getInteger("bench.rounds", 3);
        int metaIterations = Integer.getInteger("bench.meta.iterations", 20);
        String mutation = System.getProperty("bench.mutation.sql");
        String dailyProperty = System.getProperty("bench.daily.update.rows");
        String burstProperty = System.getProperty("bench.burst.update.rows");
        boolean stagedMutations = dailyProperty != null || burstProperty != null;
        if (stagedMutations && (dailyProperty == null || burstProperty == null || mutation != null)) {
            throw new IllegalArgumentException("set both bench.daily.update.rows and bench.burst.update.rows, "
                    + "without bench.mutation.sql");
        }
        long dailyRows = stagedMutations ? positiveLong("bench.daily.update.rows", dailyProperty) : 0;
        long burstRows = stagedMutations ? positiveLong("bench.burst.update.rows", burstProperty) : 0;
        long totalUpdateRows = Math.addExact(dailyRows, burstRows);
        boolean phases = Boolean.getBoolean("bench.read.timings");
        String holder = "bench:" + ManagementFactory.getRuntimeMXBean().getName();

        StarRocksCdcSourceConfig mysqlCfg = config(mysqlUrl, db, tables, user, password);
        StarRocksJdbcClient mysql = new StarRocksJdbcClient(mysqlCfg);
        List<Transport> transports = new ArrayList<>();
        Report report = new Report(phases, mode);
        report.header("mysql=" + mysqlUrl + "  arrow=" + arrowUrl + "  adbc="
                + System.getProperty("bench.adbc.uri") + "  transports=" + selected + "  db=" + db + "  tables=" + tables
                + "  rounds=" + rounds + " (first dropped)  jvm=" + System.getProperty("java.version")
                + "  tz=" + java.util.TimeZone.getDefault().getID() + "  read_timings=" + phases + "  pipeline=" + mode.label()
                + (stagedMutations ? "  changes=daily:" + Math.multiplyExact(dailyRows, 2)
                + ",burst:" + Math.multiplyExact(burstRows, 2) : ""));
        List<Held> held = new ArrayList<>();
        try {
            for (String name : selected) {
                switch (name) {
                    case "mysql":
                        transports.add(new Transport("mysql", mysql));
                        break;
                    case "arrow-flight":
                        transports.add(new Transport("arrow-flight", new StarRocksJdbcClient(
                                config(required("bench.arrow.url"), db, tables, user, password))));
                        break;
                    case "arrow-adbc":
                        transports.add(new Transport(name,
                                CdcClientFactory.create(adbcConfig(mysqlUrl, db, tables, user, password))));
                        break;
                    default:
                        throw new IllegalArgumentException("unknown bench transport: " + name);
                }
            }
            for (String table : tables) {
                benchTable(db, table, mysql, transports, holder, rounds, mutation, dailyRows,
                        burstRows, totalUpdateRows, mysqlCfg, report, held);
            }
            benchCreateLatency(db, tables.get(0), transports, holder, metaIterations, report, held);
        } finally {
            for (Held h : held) {
                try {
                    mysql.bookmarkRelease(db, h.table, h.bookmarkId, h.holder);
                } catch (SQLException e) {
                    System.err.println("could not release bookmark " + h.bookmarkId + " on " + h.table + ": " + e);
                }
            }
            try {
                for (Transport t : transports) {
                    if (t.client != mysql) {
                        t.close();
                    }
                }
            } finally {
                mysql.close();
            }
        }
        report.print();
    }

    /** Snapshot at one bookmark, then read each mutation's fixed CHANGES window over every path. */
    private void benchTable(String db, String table, StarRocksJdbcClient mysql, List<Transport> transports,
                            String holder, int rounds,
                            String mutation, long dailyRows, long burstRows, long totalUpdateRows,
                            StarRocksCdcSourceConfig mysqlCfg, Report report, List<Held> held)
            throws Exception {
        progress("%s preparing snapshot bookmark and columns", table);
        List<ColumnMeta> cols = mysql.fetchColumns(db, table);
        long base = mysql.bookmarkCreate(db, table, holder, BOOKMARK_TTL_MS);
        held.add(new Held(table, base, holder));
        Meter meter = new Meter(report.phases);
        try (RowPipeline pipeline = new RowPipeline(db, table, cols, report.phases, report.mode)) {
            long snapshotRows = -1;
            for (int round = 0; round < rounds; round++) {
                for (final Transport t : order(transports, round)) {
                    Sample s = meter.measure(pipeline, new Callable<Long>() {
                        @Override
                        public Long call() throws Exception {
                            t.streamSnapshot(db, table, cols, base, new CdcClient.RowConsumer() {
                                @Override
                                public void accept(Object[] row) {
                                    pipeline.snapshot(row, base);
                                }
                            });
                            return pipeline.rows;
                        }
                    });
                    readFinished(table, "snapshot", t.name, round, rounds, s);
                    snapshotRows = checkRows(table, "snapshot", t.name, snapshotRows, s.rows);
                    if (round > 0) {
                        report.add(table, "snapshot", t.name, s);
                    }
                }
            }
            if (mutation == null && totalUpdateRows == 0) {
                return;
            }
            if (!mysql.fetchTableConfig(db, table).cdcEnabled()) {
                throw new IllegalStateException(table + ": enable_change_data_capture is off");
            }
            if (totalUpdateRows > 0) {
                if (totalUpdateRows > snapshotRows) {
                    throw new IllegalArgumentException(table + " has " + snapshotRows + " snapshot rows, fewer than "
                            + totalUpdateRows + " requested update rows");
                }
                String update = "UPDATE " + db + "." + table + " SET v = v + 1 WHERE id > ";
                long dailyHead = benchChangesWindow(db, table, cols, mysql, transports, holder, rounds,
                        update + "0 AND id <= " + dailyRows, "changes/daily", base,
                        Math.multiplyExact(dailyRows, 2), mysqlCfg, report, held, meter, pipeline);
                benchChangesWindow(db, table, cols, mysql, transports, holder, rounds,
                        update + dailyRows + " AND id <= " + totalUpdateRows, "changes/burst", dailyHead,
                        Math.multiplyExact(burstRows, 2), mysqlCfg, report, held, meter, pipeline);
            } else {
                benchChangesWindow(db, table, cols, mysql, transports, holder, rounds,
                        mutation.replace("{db}", db).replace("{table}", table), "changes", base,
                        -1, mysqlCfg, report, held, meter, pipeline);
            }
        }
    }

    private long benchChangesWindow(String db, String table, List<ColumnMeta> cols, StarRocksJdbcClient mysql,
                                    List<Transport> transports, String holder, int rounds, String sql, String path,
                                    long base, long expectedRows, StarRocksCdcSourceConfig mysqlCfg,
                                    Report report, List<Held> held, Meter meter, RowPipeline pipeline) throws Exception {
        progress("%s %s applying mutation", table, path);
        runOverMysql(mysqlCfg, sql);
        progress("%s %s creating head bookmark", table, path);
        long head = mysql.bookmarkCreate(db, table, holder, BOOKMARK_TTL_MS);
        if (head == base) {
            // The holder already references it, so it is the same id: not a second thing to release.
            if (expectedRows >= 0) {
                throw new IllegalStateException(table + " " + path + " published no new version");
            }
            report.note(table + ": the mutation published no new version (does it name {db}.{table}?), CHANGES skipped");
            return head;
        }
        held.add(new Held(table, head, holder));
        long changeRows = expectedRows;
        for (int round = 0; round < rounds; round++) {
            for (final Transport t : order(transports, round)) {
                Sample s = meter.measure(pipeline, new Callable<Long>() {
                    @Override
                    public Long call() throws Exception {
                        t.streamChanges(db, table, cols, base, head, new CdcClient.ChangeRowConsumer() {
                            @Override
                            public void accept(Object[] row, int changeType, long rowVersion) {
                                pipeline.change(row, changeType, rowVersion, base, head);
                            }
                        });
                        return pipeline.rows;
                    }
                });
                readFinished(table, path, t.name, round, rounds, s);
                changeRows = checkRows(table, path, t.name, changeRows, s.rows);
                if (round > 0) {
                    report.add(table, path, t.name, s);
                }
            }
        }
        return head;
    }

    // Print outside Meter.measure so logging is excluded from the sample's wall/CPU/allocation totals.
    private static void readFinished(String table, String path, String transport, int round, int rounds, Sample s) {
        progress("%s %s %s round=%d/%d (%s) done rows=%d elapsed=%.1f ms", table, path, transport,
                round + 1, rounds, round == 0 ? "warmup, discarded" : "measured",
                s.rows, s.wallNanos / 1_000_000.0);
    }

    private static void progress(String format, Object... args) {
        System.out.printf(Locale.ROOT, "[bench] " + format + "%n", args);
        System.out.flush();
    }

    private static long positiveLong(String name, String value) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed > 0) {
                return parsed;
            }
        } catch (NumberFormatException ignored) {
            // Report the property name below.
        }
        throw new IllegalArgumentException(name + " must be a positive integer");
    }

    private static long checkRows(String table, String path, String transport, long expected, long actual) {
        if (expected >= 0 && actual != expected) {
            throw new IllegalStateException(table + " " + path + " " + transport + " returned " + actual
                    + " rows; expected " + expected);
        }
        return actual;
    }

    /** The idle poll's fixed cost: bookmark_create on an unchanged table. One holder per transport. */
    private void benchCreateLatency(String db, String table, List<Transport> transports, String holder,
                                    int iterations, Report report, List<Held> held) throws Exception {
        for (Transport t : transports) {
            String own = holder + ":" + t.name;
            long id = t.client.bookmarkCreate(db, table, own, BOOKMARK_TTL_MS);
            held.add(new Held(table, id, own));
            long[] nanos = new long[iterations];
            for (int i = 0; i < iterations; i++) {
                long t0 = System.nanoTime();
                t.client.bookmarkCreate(db, table, own, BOOKMARK_TTL_MS);
                nanos[i] = System.nanoTime() - t0;
            }
            report.latency("bookmark_create (idle)", t.name, nanos);
            progress("%s bookmark_create done (%d iterations)", t.name, iterations);
        }
    }

    private static void runOverMysql(StarRocksCdcSourceConfig cfg, String sql) throws SQLException {
        FeConnection fe = new FeConnection(cfg);
        try {
            Connection c = fe.get();
            try (Statement st = c.createStatement()) {
                st.execute(sql);
            }
        } finally {
            fe.close();
        }
    }

    /** Alternates which transport goes first, so neither always pays for the cold page cache. */
    private static List<Transport> order(List<Transport> transports, int round) {
        List<Transport> out = new ArrayList<>(transports);
        if (round % 2 == 1) {
            Collections.reverse(out);
        }
        return out;
    }

    private static StarRocksCdcSourceConfig config(String url, String db, List<String> tables, String user,
                                                   String password) {
        Map<String, String> m = new HashMap<>();
        m.put(StarRocksCdcSourceConfig.JDBC_URL, url);
        m.put(StarRocksCdcSourceConfig.DATABASE_NAME, db);
        m.put(StarRocksCdcSourceConfig.USERNAME, user);
        m.put(StarRocksCdcSourceConfig.PASSWORD, password);
        m.put(StarRocksCdcSourceConfig.TABLE_NAMES, String.join(",", tables));
        m.put(StarRocksCdcSourceConfig.CONNECTOR_NAME, "bench");
        return new StarRocksCdcSourceConfig(m);
    }

    private static StarRocksCdcSourceConfig adbcConfig(String url, String db, List<String> tables, String user,
                                                       String password) {
        Map<String, String> m = new HashMap<>();
        m.put(StarRocksCdcSourceConfig.JDBC_URL, url);
        m.put(StarRocksCdcSourceConfig.DATABASE_NAME, db);
        m.put(StarRocksCdcSourceConfig.USERNAME, user);
        m.put(StarRocksCdcSourceConfig.PASSWORD, password);
        m.put(StarRocksCdcSourceConfig.TABLE_NAMES, String.join(",", tables));
        m.put(StarRocksCdcSourceConfig.CONNECTOR_NAME, "bench");
        m.put(StarRocksCdcSourceConfig.READ_TRANSPORT, StarRocksCdcSourceConfig.READ_TRANSPORT_ADBC);
        m.put(StarRocksCdcSourceConfig.ADBC_URI, required("bench.adbc.uri"));
        return new StarRocksCdcSourceConfig(m);
    }

    private static String required(String key) {
        String v = System.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            throw new IllegalArgumentException("-D" + key + " is required");
        }
        return v.trim();
    }

    private static List<String> split(String csv) {
        List<String> out = new ArrayList<>();
        for (String s : csv.split(",")) {
            if (!s.trim().isEmpty()) {
                out.add(s.trim());
            }
        }
        return out;
    }

    private static final class Transport {
        final String name;
        final CdcClient client;

        Transport(String name, CdcClient client) {
            this.name = name;
            this.client = client;
        }

        void streamSnapshot(String db, String table, List<ColumnMeta> cols, long bookmark,
                            CdcClient.RowConsumer consumer) throws Exception {
            client.streamSnapshot(db, table, cols, bookmark, consumer);
        }

        void streamChanges(String db, String table, List<ColumnMeta> cols, long base, long head,
                           CdcClient.ChangeRowConsumer consumer) throws Exception {
            client.streamChanges(db, table, cols, base, head, consumer);
        }

        void close() throws Exception {
            client.close();
        }
    }

    private static final class Held {
        final String table;
        final long bookmarkId;
        final String holder;

        Held(String table, long bookmarkId, String holder) {
            this.table = table;
            this.bookmarkId = bookmarkId;
            this.holder = holder;
        }
    }

    /** The production row mapper and the same JSON settings as bench-cluster.sh. */
    static final class RowPipeline implements AutoCloseable {
        private final PipelineMode mode;
        // Make map-only results escape JIT elimination, retaining only the latest record.
        volatile SourceRecord mappedRecord;
        private final ChangeRecordMapper mapper;
        private final JsonConverter keyConverter;
        private final JsonConverter valueConverter;
        private final boolean phases;
        long rows;
        long serializedBytes;
        long mapNanos;
        long serializeNanos;

        RowPipeline(String db, String table, List<ColumnMeta> cols, boolean phases, PipelineMode mode) {
            this.mode = mode;
            this.mapper = mode.maps ? new ChangeRecordMapper(db, table, "sr." + db + "." + table, cols) : null;
            this.phases = phases;
            this.keyConverter = mode.serializes ? jsonConverter(true) : null;
            this.valueConverter = mode.serializes ? jsonConverter(false) : null;
        }

        void reset() {
            mappedRecord = null;
            rows = 0;
            serializedBytes = 0;
            mapNanos = 0;
            serializeNanos = 0;
        }

        void snapshot(Object[] row, long bookmark) {
            if (!mode.maps) {
                rows++;
                return;
            }
            long start = phases ? System.nanoTime() : 0;
            SourceRecord record = mapper.toSnapshotRecord(row, bookmark);
            if (phases) {
                mapNanos += System.nanoTime() - start;
            }
            consume(record);
        }

        void change(Object[] row, int changeType, long rowVersion, long base, long head) {
            if (!mode.maps) {
                rows++;
                return;
            }
            long start = phases ? System.nanoTime() : 0;
            SourceRecord record = mapper.toChangeRecord(row, changeType, rowVersion, base, head);
            if (phases) {
                mapNanos += System.nanoTime() - start;
            }
            consume(record);
        }

        private void consume(SourceRecord record) {
            if (!mode.serializes) {
                mappedRecord = record;
                rows++;
                return;
            }
            long start = phases ? System.nanoTime() : 0;
            byte[] key = keyConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
            byte[] value = valueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            if (phases) {
                serializeNanos += System.nanoTime() - start;
            }
            // Consume the actual encoded output without retaining records or byte arrays across rows.
            serializedBytes += (key == null ? 0 : key.length) + (value == null ? 0 : value.length);
            rows++;
        }

        @Override
        public void close() {
            try {
                if (keyConverter != null) {
                    keyConverter.close();
                }
            } finally {
                if (valueConverter != null) {
                    valueConverter.close();
                }
                mappedRecord = null;
            }
        }
    }

    static JsonConverter jsonConverter(boolean key) {
        Map<String, Object> config = new HashMap<>();
        config.put("schemas.enable", false);
        if (!key) {
            config.put("decimal.format", "NUMERIC");
        }
        JsonConverter converter = new JsonConverter();
        converter.configure(config, key);
        return converter;
    }

    static final class Sample {
        long rows;
        long serializedBytes;
        long mapNanos;
        long serializeNanos;
        long wallNanos;
        long cpuNanos;
        long allocBytes;
        long rssKb;
        CdcReadTimings timings;
    }

    /** Wall, process CPU, allocation on every live thread, and RSS around one complete read/map/serialize pipeline. */
    static final class Meter {
        private final boolean phases;
        private final com.sun.management.ThreadMXBean threads =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        private final com.sun.management.OperatingSystemMXBean os =
                (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

        Meter(boolean phases) {
            this.phases = phases;
        }

        Sample measure(RowPipeline pipeline, Callable<Long> body) throws Exception {
            pipeline.reset();
            long alloc0 = allocated();
            long cpu0 = os.getProcessCpuTime();
            CdcReadTimings timings = phases ? CdcReadTimings.beginBenchmark() : null;
            long t0 = System.nanoTime();
            long rows;
            try {
                rows = body.call();
            } finally {
                if (phases) {
                    CdcReadTimings.endBenchmark();
                }
            }
            Sample s = new Sample();
            s.wallNanos = System.nanoTime() - t0;
            s.cpuNanos = os.getProcessCpuTime() - cpu0;
            s.allocBytes = allocated() - alloc0;
            s.rows = rows;
            s.serializedBytes = pipeline.serializedBytes;
            s.mapNanos = pipeline.mapNanos;
            s.serializeNanos = pipeline.serializeNanos;
            s.rssKb = rssKb();
            s.timings = timings;
            return s;
        }

        private long allocated() {
            if (!threads.isThreadAllocatedMemorySupported()) {
                return 0;
            }
            long sum = 0;
            for (long id : threads.getAllThreadIds()) {
                long b = threads.getThreadAllocatedBytes(id);
                if (b > 0) {
                    sum += b;
                }
            }
            return sum;
        }
    }

    /** Linux reads /proc, anything else asks ps; -1 when neither answers. */
    private static long rssKb() {
        try {
            for (String line : Files.readAllLines(Paths.get("/proc/self/status"), StandardCharsets.UTF_8)) {
                if (line.startsWith("VmRSS:")) {
                    return Long.parseLong(line.replaceAll("[^0-9]", ""));
                }
            }
        } catch (IOException | RuntimeException ignored) {
            // not Linux, or not readable
        }
        try {
            String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
            Process p = new ProcessBuilder("ps", "-o", "rss=", "-p", pid).redirectErrorStream(true).start();
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line = r.readLine();
                p.waitFor();
                return line == null ? -1 : Long.parseLong(line.trim());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return -1;
        } catch (IOException | RuntimeException ignored) {
            return -1;
        }
    }

    /** Collects samples per (table, path, transport) and prints p50-based rows plus the latency rows. */
    static final class Report {
        private final PipelineMode mode;
        private final boolean phases;
        private final List<String> lines = new ArrayList<>();
        private final Map<String, List<Sample>> samples = new HashMap<>();
        private final List<String> keys = new ArrayList<>();
        private final List<String> latencies = new ArrayList<>();
        private final List<String> notes = new ArrayList<>();

        Report(boolean phases, PipelineMode mode) {
            this.mode = mode;
            this.phases = phases;
        }

        void header(String text) {
            lines.add(text);
        }

        void add(String table, String path, String transport, Sample s) {
            String key = table + "\t" + path + "\t" + transport;
            if (!samples.containsKey(key)) {
                samples.put(key, new ArrayList<Sample>());
                keys.add(key);
            }
            samples.get(key).add(s);
        }

        void latency(String what, String transport, long[] nanos) {
            long[] sorted = nanos.clone();
            Arrays.sort(sorted);
            latencies.add(String.format(Locale.ROOT, "%-24s %-6s n=%-4d p50 %8.2f ms   p99 %8.2f ms   max %8.2f ms",
                    what, transport, nanos.length, sorted[sorted.length / 2] / 1e6,
                    sorted[Math.min(sorted.length - 1, (int) Math.ceil(sorted.length * 0.99) - 1)] / 1e6,
                    sorted[sorted.length - 1] / 1e6));
        }

        void note(String text) {
            notes.add(text);
        }

        String render() {
            StringBuilder sb = new StringBuilder();
            for (String l : lines) {
                sb.append(l).append('\n');
            }
            sb.append("\nPipeline: ").append(mode.label()).append("; scope: ").append(mode.scope)
                    .append("; no Kafka sends.\n");
            sb.append(String.format(Locale.ROOT, "%-20s %-14s %-14s %10s %9s %9s %10s %7s %9s %8s %9s%n",
                    "table", "path", "via", "rows", "p50 ms", "min ms", "rows/s", "cpu s", "alloc MB", "rss MB", "json MB"));
            for (String key : keys) {
                String[] parts = key.split("\t");
                List<Sample> list = samples.get(key);
                long[] wall = new long[list.size()];
                long[] cpu = new long[list.size()];
                long[] alloc = new long[list.size()];
                long[] encoded = new long[list.size()];
                long rss = 0;
                long rows = 0;
                for (int i = 0; i < list.size(); i++) {
                    wall[i] = list.get(i).wallNanos;
                    cpu[i] = list.get(i).cpuNanos;
                    alloc[i] = list.get(i).allocBytes;
                    encoded[i] = list.get(i).serializedBytes;
                    rss = Math.max(rss, list.get(i).rssKb);
                    rows = list.get(i).rows;
                }
                double p50Ms = median(wall) / 1e6;
                sb.append(String.format(Locale.ROOT, "%-20s %-14s %-14s %10d %9.0f %9.0f %10.0f %7.2f %9.0f %8.0f %9s%n",
                        parts[0], parts[1], parts[2], rows, p50Ms, min(wall) / 1e6,
                        p50Ms > 0 ? rows / (p50Ms / 1000.0) : 0, median(cpu) / 1e9, median(alloc) / 1048576.0,
                        rss / 1024.0, mode.serializes
                                ? String.format(Locale.ROOT, "%.1f", median(encoded) / 1048576.0) : "-"));
            }
            if (phases) {
                sb.append("\nPipeline stages (independent p50 ms):\n");
                sb.append(String.format(Locale.ROOT, "%-20s %-14s %-14s %10s %10s %10s %10s %10s %10s%n",
                        "table", "path", "via", "query", "next", "batch", "decode", "map", "serialize"));
                for (String key : keys) {
                    String[] parts = key.split("\t");
                    List<Sample> list = samples.get(key);
                    if (list.get(0).timings == null) {
                        sb.append(String.format(Locale.ROOT, "%-20s %-14s %-14s %10s %10s %10s %10s %10s %10s%n",
                                parts[0], parts[1], parts[2], "-", "-", "-", "-", "-", "-"));
                        continue;
                    }
                    long[] query = new long[list.size()];
                    long[] advance = new long[list.size()];
                    long[] decode = new long[list.size()];
                    long[] map = new long[list.size()];
                    long[] serialize = new long[list.size()];
                    for (int i = 0; i < list.size(); i++) {
                        query[i] = list.get(i).timings.queryNanos;
                        advance[i] = list.get(i).timings.advanceNanos;
                        decode[i] = list.get(i).timings.decodeNanos;
                        map[i] = list.get(i).mapNanos;
                        serialize[i] = list.get(i).serializeNanos;
                    }
                    String advanceMs = String.format(Locale.ROOT, "%.1f", median(advance) / 1e6);
                    boolean adbc = "arrow-adbc".equals(parts[2]);
                    sb.append(String.format(Locale.ROOT, "%-20s %-14s %-14s %10.1f %10s %10s %10.1f %10s %10s%n",
                            parts[0], parts[1], parts[2], median(query) / 1e6, adbc ? "-" : advanceMs,
                            adbc ? advanceMs : "-", median(decode) / 1e6,
                            mode.maps ? String.format(Locale.ROOT, "%.1f", median(map) / 1e6) : "-",
                            mode.serializes ? String.format(Locale.ROOT, "%.1f", median(serialize) / 1e6) : "-"));
                }
                sb.append("query is executeQuery() through ResultSet/ADBC QueryResult return; ")
                        .append("next is JDBC ResultSet.next() through EOF; batch is ADBC loadNextBatch() through EOF. ")
                        .append("decode is per-row value reading; map builds SourceRecord; serialize encodes both key and value JSON. ")
                        .append("Stage medians are independent and need not sum to total. Per-row timing adds overhead, ")
                        .append("so compare throughput with phases off.\n");

                sb.append("\nADBC batch waits (independent p50 per read; loaded batches only except EOF):\n");
                sb.append(String.format(Locale.ROOT,
                        "%-20s %-14s %9s %10s %10s %10s %9s %10s %10s %10s%n",
                        "table", "path", "batches", "first ms", "later ms", "max ms", "EOF ms",
                        "rows/avg", "rows/min", "rows/max"));
                for (String key : keys) {
                    String[] parts = key.split("\t");
                    if (!"arrow-adbc".equals(parts[2])) {
                        continue;
                    }
                    List<Sample> list = samples.get(key);
                    long[] batches = new long[list.size()];
                    long[] first = new long[list.size()];
                    long[] later = new long[list.size()];
                    long[] laterMax = new long[list.size()];
                    long[] eof = new long[list.size()];
                    long[] avgRows = new long[list.size()];
                    long[] minRows = new long[list.size()];
                    long[] maxRows = new long[list.size()];
                    for (int i = 0; i < list.size(); i++) {
                        CdcReadTimings t = list.get(i).timings;
                        batches[i] = t.batchCount;
                        first[i] = t.firstBatchNanos;
                        later[i] = t.laterBatchNanos;
                        laterMax[i] = t.maxLaterBatchNanos;
                        eof[i] = t.eofBatchNanos;
                        avgRows[i] = Math.round(t.averageBatchRows());
                        minRows[i] = t.minBatchRows();
                        maxRows[i] = t.maxBatchRows;
                    }
                    sb.append(String.format(Locale.ROOT,
                            "%-20s %-14s %9.0f %10.1f %10.1f %10.1f %9.1f %10.0f %10.0f %10.0f%n",
                            parts[0], parts[1], median(batches), median(first) / 1e6,
                            median(later) / 1e6, median(laterMax) / 1e6, median(eof) / 1e6,
                            median(avgRows), median(minRows), median(maxRows)));
                }
                sb.append("Every loadNextBatch call, including EOF, is in target/pipeline-batches.tsv ")
                        .append("(measured rounds only).\n");
            }
            if (!latencies.isEmpty()) {
                sb.append('\n');
                for (String l : latencies) {
                    sb.append(l).append('\n');
                }
            }
            for (String n : notes) {
                sb.append("note: ").append(n).append('\n');
            }
            sb.append("\nrows/s is from the p50 wall time; cpu s is process CPU over the round (all threads);")
                    .append(" alloc MB sums Java allocations across live threads (not Arrow native buffers);")
                    .append(" rss MB is the process peak seen after a round; json MB is encoded key+value bytes (MiB).\n")
                    .append("Totals measure pipeline=").append(mode.label())
                    .append(" regardless of read_timings; - means a stage was not run. Compare totals within the same mode.\n");
            return sb.toString();
        }

        void print() throws IOException {
            String text = render();
            System.out.print(text);
            File out = new File("target", "pipeline-bench.txt");
            out.getParentFile().mkdirs();
            try (PrintStream ps = new PrintStream(out, "UTF-8")) {
                ps.print(text);
            }
            if (phases) {
                File batches = new File("target", "pipeline-batches.tsv");
                try (PrintStream ps = new PrintStream(batches, "UTF-8")) {
                    ps.println("table\tpath\tvia\tround\tbatch\trows\twait_ms\teof");
                    for (String key : keys) {
                        String[] parts = key.split("\t");
                        if (!"arrow-adbc".equals(parts[2])) {
                            continue;
                        }
                        List<Sample> list = samples.get(key);
                        for (int i = 0; i < list.size(); i++) {
                            List<CdcReadTimings.AdbcBatch> calls = list.get(i).timings.adbcBatches;
                            for (int j = 0; j < calls.size(); j++) {
                                CdcReadTimings.AdbcBatch call = calls.get(j);
                                ps.printf(Locale.ROOT, "%s\t%s\t%s\t%d\t%d\t%d\t%.3f\t%s%n",
                                        parts[0], parts[1], parts[2], i + 2, j + 1,
                                        call.rows, call.waitNanos / 1e6, j == calls.size() - 1);
                            }
                        }
                    }
                }
            }
        }

        private static double median(long[] values) {
            long[] sorted = values.clone();
            Arrays.sort(sorted);
            return sorted[sorted.length / 2];
        }

        private static double min(long[] values) {
            long m = Long.MAX_VALUE;
            for (long v : values) {
                m = Math.min(m, v);
            }
            return m;
        }
    }
}
