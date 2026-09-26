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
 * Snapshot, CHANGES and bookmark_create latency over both transports: same tables, same bookmark,
 * same JVM, transports alternating round by round. Runs only when both URLs are given, via
 * {@code mvn test -Dtest=TransportBench -Dbench.mysql.url=... -Dbench.arrow.url=...}.
 *
 * <p>Properties: {@code bench.db}, {@code bench.tables} (csv), {@code bench.user} (root),
 * {@code bench.password}, {@code bench.rounds} (5, first dropped), {@code bench.meta.iterations} (20),
 * {@code bench.mutation.sql} (run once per table with {@code {db}}/{@code {table}} substituted; both
 * transports then read the same CHANGES window). Output also goes to {@code target/transport-bench.txt}.
 */
public class TransportBench {

    private static final long BOOKMARK_TTL_MS = 3_600_000L;

    @Test
    public void run() throws Exception {
        String mysqlUrl = System.getProperty("bench.mysql.url");
        String arrowUrl = System.getProperty("bench.arrow.url");
        Assume.assumeTrue("set -Dbench.mysql.url and -Dbench.arrow.url to run the transport bench",
                mysqlUrl != null && arrowUrl != null);
        String db = required("bench.db");
        List<String> tables = split(required("bench.tables"));
        String user = System.getProperty("bench.user", "root");
        String password = System.getProperty("bench.password", "");
        int rounds = Integer.getInteger("bench.rounds", 5);
        int metaIterations = Integer.getInteger("bench.meta.iterations", 20);
        String mutation = System.getProperty("bench.mutation.sql");
        String holder = "bench:" + ManagementFactory.getRuntimeMXBean().getName();

        StarRocksCdcSourceConfig mysqlCfg = config(mysqlUrl, db, tables, user, password);
        List<Transport> transports = Arrays.asList(
                new Transport("mysql", new StarRocksJdbcClient(mysqlCfg)),
                new Transport("arrow", new StarRocksJdbcClient(config(arrowUrl, db, tables, user, password))));
        Report report = new Report();
        report.header("mysql=" + mysqlUrl + "  arrow=" + arrowUrl + "  db=" + db + "  tables=" + tables
                + "  rounds=" + rounds + " (first dropped)  jvm=" + System.getProperty("java.version")
                + "  tz=" + java.util.TimeZone.getDefault().getID());
        List<Held> held = new ArrayList<>();
        try {
            for (String table : tables) {
                benchTable(db, table, transports, holder, rounds, mutation, mysqlCfg, report, held);
            }
            benchCreateLatency(db, tables.get(0), transports, holder, metaIterations, report, held);
        } finally {
            for (Held h : held) {
                try {
                    transports.get(0).client.bookmarkRelease(db, h.table, h.bookmarkId, h.holder);
                } catch (SQLException e) {
                    System.err.println("could not release bookmark " + h.bookmarkId + " on " + h.table + ": " + e);
                }
            }
            for (Transport t : transports) {
                t.client.close();
            }
        }
        report.print();
    }

    /** Snapshot at one bookmark, then (with a mutation) one CHANGES window, both read by both transports. */
    private void benchTable(String db, String table, List<Transport> transports, String holder, int rounds,
                            String mutation, StarRocksCdcSourceConfig mysqlCfg, Report report, List<Held> held)
            throws Exception {
        StarRocksJdbcClient mysql = transports.get(0).client;
        List<ColumnMeta> cols = mysql.fetchColumns(db, table);
        long base = mysql.bookmarkCreate(db, table, holder, BOOKMARK_TTL_MS);
        held.add(new Held(table, base, holder));
        Meter meter = new Meter();
        for (int round = 0; round < rounds; round++) {
            for (final Transport t : order(transports, round)) {
                Sample s = meter.measure(new Callable<Long>() {
                    @Override
                    public Long call() throws Exception {
                        final long[] n = {0};
                        t.client.streamSnapshot(db, table, cols, base, new CdcClient.RowConsumer() {
                            @Override
                            public void accept(Object[] row) {
                                n[0]++;
                            }
                        });
                        return n[0];
                    }
                });
                if (round > 0) {
                    report.add(table, "snapshot", t.name, s);
                }
            }
        }
        if (mutation == null) {
            return;
        }
        if (!mysql.fetchTableConfig(db, table).cdcEnabled()) {
            report.note(table + ": enable_change_data_capture is off, CHANGES skipped");
            return;
        }
        runOverMysql(mysqlCfg, mutation.replace("{db}", db).replace("{table}", table));
        long head = mysql.bookmarkCreate(db, table, holder, BOOKMARK_TTL_MS);
        if (head == base) {
            // The holder already references it, so it is the same id: not a second thing to release.
            report.note(table + ": the mutation published no new version (does it name {db}.{table}?), CHANGES skipped");
            return;
        }
        held.add(new Held(table, head, holder));
        for (int round = 0; round < rounds; round++) {
            for (final Transport t : order(transports, round)) {
                Sample s = meter.measure(new Callable<Long>() {
                    @Override
                    public Long call() throws Exception {
                        final long[] n = {0};
                        t.client.streamChanges(db, table, cols, base, head, new CdcClient.ChangeRowConsumer() {
                            @Override
                            public void accept(Object[] row, int changeType, long rowVersion) {
                                n[0]++;
                            }
                        });
                        return n[0];
                    }
                });
                if (round > 0) {
                    report.add(table, "changes", t.name, s);
                }
            }
        }
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
        final StarRocksJdbcClient client;

        Transport(String name, StarRocksJdbcClient client) {
            this.name = name;
            this.client = client;
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

    private static final class Sample {
        long rows;
        long wallNanos;
        long cpuNanos;
        long allocBytes;
        long rssKb;
    }

    /** Wall, process CPU, allocation on every live thread, and RSS around one read. */
    private static final class Meter {
        private final com.sun.management.ThreadMXBean threads =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        private final com.sun.management.OperatingSystemMXBean os =
                (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

        Sample measure(Callable<Long> body) throws Exception {
            long alloc0 = allocated();
            long cpu0 = os.getProcessCpuTime();
            long t0 = System.nanoTime();
            long rows = body.call();
            Sample s = new Sample();
            s.wallNanos = System.nanoTime() - t0;
            s.cpuNanos = os.getProcessCpuTime() - cpu0;
            s.allocBytes = allocated() - alloc0;
            s.rows = rows;
            s.rssKb = rssKb();
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
    private static final class Report {
        private final List<String> lines = new ArrayList<>();
        private final Map<String, List<Sample>> samples = new HashMap<>();
        private final List<String> keys = new ArrayList<>();
        private final List<String> latencies = new ArrayList<>();
        private final List<String> notes = new ArrayList<>();

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

        void print() throws IOException {
            StringBuilder sb = new StringBuilder();
            for (String l : lines) {
                sb.append(l).append('\n');
            }
            sb.append('\n');
            sb.append(String.format(Locale.ROOT, "%-20s %-9s %-6s %10s %9s %9s %10s %7s %9s %8s%n",
                    "table", "path", "via", "rows", "p50 ms", "min ms", "rows/s", "cpu s", "alloc MB", "rss MB"));
            for (String key : keys) {
                String[] parts = key.split("\t");
                List<Sample> list = samples.get(key);
                long[] wall = new long[list.size()];
                long[] cpu = new long[list.size()];
                long[] alloc = new long[list.size()];
                long rss = 0;
                long rows = 0;
                for (int i = 0; i < list.size(); i++) {
                    wall[i] = list.get(i).wallNanos;
                    cpu[i] = list.get(i).cpuNanos;
                    alloc[i] = list.get(i).allocBytes;
                    rss = Math.max(rss, list.get(i).rssKb);
                    rows = list.get(i).rows;
                }
                double p50Ms = median(wall) / 1e6;
                sb.append(String.format(Locale.ROOT, "%-20s %-9s %-6s %10d %9.0f %9.0f %10.0f %7.2f %9.0f %8.0f%n",
                        parts[0], parts[1], parts[2], rows, p50Ms, min(wall) / 1e6,
                        p50Ms > 0 ? rows / (p50Ms / 1000.0) : 0, median(cpu) / 1e9, median(alloc) / 1048576.0,
                        rss / 1024.0));
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
                    .append(" alloc MB sums every live thread; rss MB is the process peak seen after a round.\n");
            String text = sb.toString();
            System.out.print(text);
            File out = new File("target", "transport-bench.txt");
            out.getParentFile().mkdirs();
            try (PrintStream ps = new PrintStream(out, "UTF-8")) {
                ps.print(text);
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
