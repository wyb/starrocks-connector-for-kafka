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

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StarRocksAdbcClientTest {
    @Test
    public void timedOutQueryRotatesToNextFlightEndpoint() throws Exception {
        assertReadTimeoutRotates("query", false);
    }

    @Test
    public void timedOutBatchRotatesToNextFlightEndpoint() throws Exception {
        assertReadTimeoutRotates("batch", false);
    }

    @Test
    public void timedOutChangesBatchRotatesToNextFlightEndpoint() throws Exception {
        assertReadTimeoutRotates("batch", true);
    }

    @Test
    public void timedOutChangesQueryRotatesToNextFlightEndpoint() throws Exception {
        assertReadTimeoutRotates("query", true);
    }

    private void assertReadTimeoutRotates(String hangingStage, boolean changes) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:mysql://localhost:9030");
        props.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db");
        props.put(StarRocksCdcSourceConfig.USERNAME, "root");
        props.put(StarRocksCdcSourceConfig.PASSWORD, "");
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t");
        props.put(StarRocksCdcSourceConfig.CONNECTOR_NAME, "test");
        props.put(StarRocksCdcSourceConfig.READ_TRANSPORT, StarRocksCdcSourceConfig.READ_TRANSPORT_ADBC);
        props.put(StarRocksCdcSourceConfig.ADBC_URI, "grpc+tcp://down:9408,grpc+tcp://up:9408");
        props.put(StarRocksCdcSourceConfig.READ_TIMEOUT_MS, "100");
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch firstStatementClosed = new CountDownLatch(1);
        CountDownLatch firstConnectionClosed = new CountDownLatch(1);
        AtomicBoolean firstClosedTooEarly = new AtomicBoolean();
        AtomicBoolean firstConnectionClosedTooEarly = new AtomicBoolean();
        List<String> opened = new ArrayList<>();
        StarRocksAdbcClient client = new StarRocksAdbcClient(new StarRocksCdcSourceConfig(props),
                (allocator, options) -> {
                    String uri = AdbcDriver.PARAM_URI.get(options);
                    opened.add(uri);
                    boolean first = uri.contains("down");
                    return new AdbcDatabase() {
                        @Override
                        public AdbcConnection connect() {
                            AdbcStatement statement = (AdbcStatement) Proxy.newProxyInstance(
                                    getClass().getClassLoader(), new Class<?>[] {AdbcStatement.class},
                                    (proxy, method, args) -> {
                                        if ("executeQuery".equals(method.getName())) {
                                            if (first && "query".equals(hangingStage)) {
                                                awaitIgnoringInterrupts(release);
                                            }
                                            return new AdbcStatement.QueryResult(0,
                                                    emptyReader(allocator, first && "batch".equals(hangingStage),
                                                            release));
                                        }
                                        if ("close".equals(method.getName()) && first) {
                                            if (release.getCount() != 0) {
                                                firstClosedTooEarly.set(true);
                                            }
                                            firstStatementClosed.countDown();
                                        }
                                        return null;
                                    });
                            return (AdbcConnection) Proxy.newProxyInstance(getClass().getClassLoader(),
                                    new Class<?>[] {AdbcConnection.class}, (proxy, method, args) -> {
                                        if ("createStatement".equals(method.getName())) {
                                            return statement;
                                        }
                                        if ("close".equals(method.getName()) && first) {
                                            if (release.getCount() != 0) {
                                                firstConnectionClosedTooEarly.set(true);
                                            }
                                            firstConnectionClosed.countDown();
                                        }
                                        return null;
                                    });
                        }

                        @Override
                        public void close() {
                        }
                    };
                });
        List<ColumnMeta> cols = Collections.singletonList(new ColumnMeta("id", "bigint", "bigint", 0, false));
        try {
            try {
                if (changes) {
                    client.streamChanges("db", "t", cols, 1, 2, (row, type, version) -> { });
                } else {
                    CdcClient.SnapshotCursor first = client.openSnapshot("db", "t", cols, 1);
                    first.next();
                }
                fail("Expected the " + hangingStage + " to time out");
            } catch (SQLException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("timed out"));
            }
            if (changes) {
                client.streamChanges("db", "t", cols, 1, 2, (row, type, version) -> { });
            } else {
                try (CdcClient.SnapshotCursor recovered = client.openSnapshot("db", "t", cols, 1)) {
                    assertTrue(recovered.next() == null);
                }
            }
            assertTrue(opened.toString(), opened.equals(Arrays.asList(
                    "grpc+tcp://down:9408", "grpc+tcp://up:9408")));
        } finally {
            release.countDown();
            client.close();
        }
        assertTrue(firstStatementClosed.await(2, TimeUnit.SECONDS));
        assertFalse("the timed-out read was closed while its worker still used it", firstClosedTooEarly.get());
        if (changes) {
            assertTrue(firstConnectionClosed.await(2, TimeUnit.SECONDS));
            assertFalse("the timed-out read lost its connection before its worker stopped",
                    firstConnectionClosedTooEarly.get());
        }
    }

    private static ArrowReader emptyReader(BufferAllocator allocator, boolean block, CountDownLatch release) {
        return emptyReader(allocator, block, release, null);
    }

    private static ArrowReader emptyReader(BufferAllocator allocator, boolean block, CountDownLatch release,
                                           CountDownLatch entered) {
        return new ArrowReader(allocator) {
            @Override
            public boolean loadNextBatch() {
                if (block) {
                    if (entered != null) {
                        entered.countDown();
                    }
                    awaitIgnoringInterrupts(release);
                }
                return false;
            }

            @Override
            public long bytesRead() {
                return 0;
            }

            @Override
            protected void closeReadSource() {
            }

            @Override
            protected Schema readSchema() {
                return new Schema(Collections.emptyList());
            }
        };
    }

    private static void awaitIgnoringInterrupts(CountDownLatch release) {
        while (true) {
            try {
                release.await();
                return;
            } catch (InterruptedException ignored) {
                // Simulate an ADBC call that does not respond to cancellation.
            }
        }
    }

    @Test
    public void failedEndpointIsReplacedByTheNextFlightUri() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:mysql://localhost:9030");
        props.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db");
        props.put(StarRocksCdcSourceConfig.USERNAME, "root");
        props.put(StarRocksCdcSourceConfig.PASSWORD, "");
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t");
        props.put(StarRocksCdcSourceConfig.CONNECTOR_NAME, "test");
        props.put(StarRocksCdcSourceConfig.READ_TRANSPORT, StarRocksCdcSourceConfig.READ_TRANSPORT_ADBC);
        props.put(StarRocksCdcSourceConfig.ADBC_URI,
                "grpc+tcp://down:9408,grpc+tcp://up:9408");
        List<String> opened = new ArrayList<>();
        AdbcConnection healthy = (AdbcConnection) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {AdbcConnection.class}, (proxy, method, args) -> null);
        StarRocksAdbcClient client = new StarRocksAdbcClient(new StarRocksCdcSourceConfig(props),
                (allocator, options) -> {
                    String uri = AdbcDriver.PARAM_URI.get(options);
                    opened.add(uri);
                    return new AdbcDatabase() {
                        @Override
                        public AdbcConnection connect() throws AdbcException {
                            if (uri.contains("down")) {
                                throw AdbcException.io("FE unavailable");
                            }
                            return healthy;
                        }

                        @Override
                        public void close() {
                        }
                    };
                });
        try {
            try {
                client.connectAdbc();
                fail("The first FE must fail");
            } catch (SQLException expected) {
                assertTrue(expected.getMessage().contains("FE unavailable"));
            }
            client.connectAdbc();
            assertTrue(opened.toString(), opened.equals(Arrays.asList(
                    "grpc+tcp://down:9408", "grpc+tcp://up:9408")));
        } finally {
            client.close();
        }
    }

    @Test
    public void failedConnectDoesNotBlockStopOnDatabaseClose() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:mysql://localhost:9030");
        props.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db");
        props.put(StarRocksCdcSourceConfig.USERNAME, "root");
        props.put(StarRocksCdcSourceConfig.PASSWORD, "");
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t");
        props.put(StarRocksCdcSourceConfig.CONNECTOR_NAME, "test");
        props.put(StarRocksCdcSourceConfig.ADBC_URI, "grpc+tcp://localhost:9408");
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        StarRocksAdbcClient client = new StarRocksAdbcClient(new StarRocksCdcSourceConfig(props),
                (allocator, options) -> new AdbcDatabase() {
                    @Override
                    public AdbcConnection connect() throws AdbcException {
                        throw AdbcException.io("FE unavailable");
                    }

                    @Override
                    public void close() {
                        closing.countDown();
                        awaitIgnoringInterrupts(release);
                    }
                });
        Thread stopper = new Thread(client::close);
        try {
            try {
                client.connectAdbc();
                fail("Expected a connection failure");
            } catch (SQLException expected) {
                assertTrue(expected.getMessage().contains("FE unavailable"));
            }
            stopper.start();
            assertTrue(closing.await(2, TimeUnit.SECONDS));
            stopper.join(1000);
            assertFalse("stop waited for a stuck ADBC database close", stopper.isAlive());
        } finally {
            release.countDown();
            stopper.join(2000);
            client.close();
        }
    }

    @Test
    public void stuckJdbcCloseDoesNotDelayAdbcCleanup() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:mysql://localhost:9030");
        props.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db");
        props.put(StarRocksCdcSourceConfig.USERNAME, "root");
        props.put(StarRocksCdcSourceConfig.PASSWORD, "");
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t");
        props.put(StarRocksCdcSourceConfig.CONNECTOR_NAME, "test");
        props.put(StarRocksCdcSourceConfig.ADBC_URI, "grpc+tcp://localhost:9408");
        CountDownLatch jdbcClosing = new CountDownLatch(1);
        CountDownLatch releaseJdbc = new CountDownLatch(1);
        CountDownLatch databaseClosed = new CountDownLatch(1);
        StarRocksAdbcClient client = new StarRocksAdbcClient(new StarRocksCdcSourceConfig(props),
                (allocator, options) -> new AdbcDatabase() {
                    @Override
                    public AdbcConnection connect() {
                        return (AdbcConnection) Proxy.newProxyInstance(getClass().getClassLoader(),
                                new Class<?>[] {AdbcConnection.class}, (proxy, method, args) -> null);
                    }

                    @Override
                    public void close() {
                        databaseClosed.countDown();
                    }
                });
        java.sql.Connection stuckJdbc = (java.sql.Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {java.sql.Connection.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        jdbcClosing.countDown();
                        awaitIgnoringInterrupts(releaseJdbc);
                    }
                    return null;
                });
        java.lang.reflect.Field jdbcField = StarRocksAdbcClient.class.getDeclaredField("jdbc");
        jdbcField.setAccessible(true);
        java.lang.reflect.Field connectionField = StarRocksJdbcClient.class.getDeclaredField("connection");
        connectionField.setAccessible(true);
        java.lang.reflect.Field rawConnectionField = FeConnection.class.getDeclaredField("conn");
        rawConnectionField.setAccessible(true);
        rawConnectionField.set(connectionField.get(jdbcField.get(client)), stuckJdbc);
        Thread stopper = new Thread(client::close);
        try {
            client.connectAdbc();
            stopper.start();
            assertTrue(jdbcClosing.await(2, TimeUnit.SECONDS));
            assertTrue("ADBC database close waited for JDBC close", databaseClosed.await(2, TimeUnit.SECONDS));
            stopper.join(1000);
            assertFalse("client close waited for JDBC close", stopper.isAlive());
        } finally {
            releaseJdbc.countDown();
            stopper.join(2000);
            client.close();
        }
    }

    @Test
    public void rejectsOpaqueColumnsBeforeReading() throws SQLException {
        ColumnMeta supported = new ColumnMeta("id", "bigint", "bigint", 0, false);
        ColumnMeta opaque = new ColumnMeta("payload", "struct", "struct<unsupported>", 0, true);
        try {
            StarRocksAdbcClient.ensureSupportedColumns("db", "t", Arrays.asList(supported, opaque));
            fail("Expected an unsupported ADBC column to fail at preflight");
        } catch (SQLException expected) {
            assertTrue(expected.getMessage().contains("db.t.payload"));
            assertTrue(expected.getMessage().contains("source.read.transport=jdbc"));
        }
    }

    @Test
    public void flightStreamFailuresEnterTheSqlRetryPath() {
        FlightRuntimeException unavailable = CallStatus.UNAVAILABLE.withDescription("stream interrupted")
                .toRuntimeException();
        SQLException failure = StarRocksAdbcClient.readFailure(unavailable);
        assertTrue(failure.getCause() == unavailable);
        assertTrue(failure.getMessage().contains("stream interrupted"));

        DataException invalidValue = new DataException("invalid Arrow value");
        try {
            StarRocksAdbcClient.readFailure(invalidValue);
            fail("Value conversion failures must stay fatal");
        } catch (DataException expected) {
            assertTrue(expected == invalidValue);
        }
    }

    @Test
    public void timedOutConnectDoesNotBlockStopAndClosesLateConnection() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:mysql://localhost:9030");
        props.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db");
        props.put(StarRocksCdcSourceConfig.USERNAME, "root");
        props.put(StarRocksCdcSourceConfig.PASSWORD, "");
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t");
        props.put(StarRocksCdcSourceConfig.CONNECTOR_NAME, "test");
        props.put(StarRocksCdcSourceConfig.ADBC_URI, "grpc+tcp://localhost:9408");
        props.put(StarRocksCdcSourceConfig.CONNECT_TIMEOUT_MS, "100");
        StarRocksAdbcClient client = new StarRocksAdbcClient(new StarRocksCdcSourceConfig(props));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch connectionClosed = new CountDownLatch(1);
        AdbcConnection connection = (AdbcConnection) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {AdbcConnection.class}, (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        connectionClosed.countDown();
                    }
                    return null;
                });
        AdbcDatabase slow = new AdbcDatabase() {
            @Override
            public AdbcConnection connect() throws AdbcException {
                entered.countDown();
                boolean interrupted = false;
                while (true) {
                    try {
                        release.await();
                        break;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
                return connection;
            }

            @Override
            public void close() {
            }
        };
        AdbcConnection recovered = (AdbcConnection) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {AdbcConnection.class}, (proxy, method, args) -> null);
        AdbcDatabase healthy = new AdbcDatabase() {
            @Override
            public AdbcConnection connect() {
                return recovered;
            }

            @Override
            public void close() {
            }
        };
        Thread closer = new Thread(client::close);
        try {
            try {
                client.connectWithTimeout(slow);
                fail("Expected bounded ADBC connection wait");
            } catch (SQLException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("timed out"));
            }
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            assertTrue("a timed-out attempt must not prevent a new connection",
                    client.connectWithTimeout(healthy) == recovered);
            closer.start();
            closer.join(1000);
            assertFalse("stop must not wait for a stuck ADBC connect", closer.isAlive());
            assertFalse("late connection must not close before connect returns",
                    connectionClosed.await(50, TimeUnit.MILLISECONDS));
        } finally {
            release.countDown();
            closer.join(2000);
            client.close();
        }
        assertFalse("client close must finish after connect unwinds", closer.isAlive());
        assertTrue(connectionClosed.await(2, TimeUnit.SECONDS));
    }

    @Test
    public void exhaustedCancelledCallsFailTaskClearly() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:mysql://localhost:9030");
        props.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db");
        props.put(StarRocksCdcSourceConfig.USERNAME, "root");
        props.put(StarRocksCdcSourceConfig.PASSWORD, "");
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t");
        props.put(StarRocksCdcSourceConfig.CONNECTOR_NAME, "test");
        props.put(StarRocksCdcSourceConfig.ADBC_URI, "grpc+tcp://localhost:9408");
        props.put(StarRocksCdcSourceConfig.CONNECT_TIMEOUT_MS, "100");
        StarRocksAdbcClient client = new StarRocksAdbcClient(new StarRocksCdcSourceConfig(props));
        CountDownLatch release = new CountDownLatch(1);
        AdbcDatabase stuck = new AdbcDatabase() {
            @Override
            public AdbcConnection connect() {
                awaitIgnoringInterrupts(release);
                return (AdbcConnection) Proxy.newProxyInstance(getClass().getClassLoader(),
                        new Class<?>[] {AdbcConnection.class}, (proxy, method, args) -> null);
            }

            @Override
            public void close() {
            }
        };
        try {
            for (int i = 0; i < 8; i++) {
                try {
                    client.connectWithTimeout(stuck);
                    fail("Expected connection timeout");
                } catch (SQLException expected) {
                    assertTrue(expected.getMessage().contains("timed out"));
                }
            }
            try {
                client.connectWithTimeout(stuck);
                fail("The task must fail once cancelled calls exhaust the worker limit");
            } catch (ConnectException expected) {
                assertTrue(expected.getMessage().contains("cannot safely retry"));
            }
        } finally {
            release.countDown();
            client.close();
        }
    }

    @Test
    public void stuckConnectionClosesAlsoStopRetries() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:mysql://localhost:9030");
        props.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db");
        props.put(StarRocksCdcSourceConfig.USERNAME, "root");
        props.put(StarRocksCdcSourceConfig.PASSWORD, "");
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t");
        props.put(StarRocksCdcSourceConfig.CONNECTOR_NAME, "test");
        props.put(StarRocksCdcSourceConfig.ADBC_URI, "grpc+tcp://localhost:9408");
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch closing = new CountDownLatch(8);
        StarRocksAdbcClient client = new StarRocksAdbcClient(new StarRocksCdcSourceConfig(props),
                (allocator, options) -> new AdbcDatabase() {
                    @Override
                    public AdbcConnection connect() {
                        AdbcStatement statement = (AdbcStatement) Proxy.newProxyInstance(
                                getClass().getClassLoader(), new Class<?>[] {AdbcStatement.class},
                                (proxy, method, args) -> {
                                    if ("executeQuery".equals(method.getName())) {
                                        throw AdbcException.io("FE unavailable");
                                    }
                                    return null;
                                });
                        return (AdbcConnection) Proxy.newProxyInstance(getClass().getClassLoader(),
                                new Class<?>[] {AdbcConnection.class}, (proxy, method, args) -> {
                                    if ("createStatement".equals(method.getName())) {
                                        return statement;
                                    }
                                    if ("close".equals(method.getName())) {
                                        closing.countDown();
                                        awaitIgnoringInterrupts(release);
                                    }
                                    return null;
                                });
                    }

                    @Override
                    public void close() {
                    }
                });
        List<ColumnMeta> cols = Collections.singletonList(new ColumnMeta("id", "bigint", "bigint", 0, false));
        try {
            for (int i = 0; i < 8; i++) {
                try {
                    client.streamChanges("db", "t", cols, 1, 2, (row, type, version) -> { });
                    fail("Expected read failure");
                } catch (SQLException expected) {
                    assertTrue(expected.getMessage().contains("FE unavailable"));
                }
            }
            assertTrue(closing.await(2, TimeUnit.SECONDS));
            try {
                client.streamChanges("db", "t", cols, 1, 2, (row, type, version) -> { });
                fail("Stuck connection closes must bound retries");
            } catch (ConnectException expected) {
                assertTrue(expected.getMessage().contains("cannot safely retry"));
            }
        } finally {
            release.countDown();
            client.close();
        }
    }

    @Test
    public void stopDefersConnectionCloseUntilChangesReaderReturns() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:mysql://localhost:9030");
        props.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db");
        props.put(StarRocksCdcSourceConfig.USERNAME, "root");
        props.put(StarRocksCdcSourceConfig.PASSWORD, "");
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t");
        props.put(StarRocksCdcSourceConfig.CONNECTOR_NAME, "test");
        props.put(StarRocksCdcSourceConfig.ADBC_URI, "grpc+tcp://localhost:9408");
        props.put(StarRocksCdcSourceConfig.READ_TIMEOUT_MS, "5000");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch connectionClosed = new CountDownLatch(1);
        StarRocksAdbcClient client = new StarRocksAdbcClient(new StarRocksCdcSourceConfig(props),
                (allocator, options) -> new AdbcDatabase() {
                    @Override
                    public AdbcConnection connect() {
                        AdbcStatement statement = (AdbcStatement) Proxy.newProxyInstance(
                                getClass().getClassLoader(), new Class<?>[] {AdbcStatement.class},
                                (proxy, method, args) -> "executeQuery".equals(method.getName())
                                        ? new AdbcStatement.QueryResult(0,
                                                emptyReader(allocator, true, release, entered)) : null);
                        return (AdbcConnection) Proxy.newProxyInstance(getClass().getClassLoader(),
                                new Class<?>[] {AdbcConnection.class}, (proxy, method, args) -> {
                                    if ("createStatement".equals(method.getName())) {
                                        return statement;
                                    }
                                    if ("close".equals(method.getName())) {
                                        connectionClosed.countDown();
                                    }
                                    return null;
                                });
                    }

                    @Override
                    public void close() {
                    }
                });
        AtomicReference<Throwable> readFailure = new AtomicReference<>();
        Thread reader = new Thread(() -> {
            try {
                List<ColumnMeta> cols = Collections.singletonList(
                        new ColumnMeta("id", "bigint", "bigint", 0, false));
                client.streamChanges("db", "t", cols, 1, 2, (row, type, version) -> { });
            } catch (Throwable e) {
                readFailure.set(e);
            }
        });
        try {
            reader.start();
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            client.close();
            assertFalse("stop closed the connection while loadNextBatch was running",
                    connectionClosed.await(50, TimeUnit.MILLISECONDS));
        } finally {
            release.countDown();
            reader.join(2000);
            client.close();
        }
        assertFalse("the read thread did not exit", reader.isAlive());
        assertTrue(readFailure.get() instanceof SQLException);
        assertTrue(connectionClosed.await(2, TimeUnit.SECONDS));
    }

    @Test
    public void closingSnapshotWaitsForRowDecode() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:mysql://localhost:9030");
        props.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db");
        props.put(StarRocksCdcSourceConfig.USERNAME, "root");
        props.put(StarRocksCdcSourceConfig.PASSWORD, "");
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t");
        props.put(StarRocksCdcSourceConfig.CONNECTOR_NAME, "test");
        props.put(StarRocksCdcSourceConfig.ADBC_URI, "grpc+tcp://localhost:9408");
        CountDownLatch decoding = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch readerClosed = new CountDownLatch(1);
        AtomicBoolean closedDuringDecode = new AtomicBoolean();
        Field field = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
        FieldVector vector = (FieldVector) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {FieldVector.class}, (proxy, method, args) -> {
                    if ("isNull".equals(method.getName())) {
                        return false;
                    }
                    if ("getObject".equals(method.getName())) {
                        decoding.countDown();
                        awaitIgnoringInterrupts(release);
                        return 42;
                    }
                    if ("getField".equals(method.getName())) {
                        return field;
                    }
                    return null;
                });
        VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(field),
                Collections.singletonList(vector), 1);
        StarRocksAdbcClient client = new StarRocksAdbcClient(new StarRocksCdcSourceConfig(props),
                (allocator, options) -> new AdbcDatabase() {
                    @Override
                    public AdbcConnection connect() {
                        AdbcStatement statement = (AdbcStatement) Proxy.newProxyInstance(
                                getClass().getClassLoader(), new Class<?>[] {AdbcStatement.class},
                                (proxy, method, args) -> "executeQuery".equals(method.getName())
                                        ? new AdbcStatement.QueryResult(1, new ArrowReader(allocator) {
                                            private boolean loaded;

                                            @Override
                                            public boolean loadNextBatch() {
                                                if (loaded) {
                                                    return false;
                                                }
                                                loaded = true;
                                                return true;
                                            }

                                            @Override
                                            public VectorSchemaRoot getVectorSchemaRoot() {
                                                return root;
                                            }

                                            @Override
                                            public long bytesRead() {
                                                return 0;
                                            }

                                            @Override
                                            public void close() {
                                                if (release.getCount() != 0) {
                                                    closedDuringDecode.set(true);
                                                }
                                                readerClosed.countDown();
                                            }

                                            @Override
                                            protected void closeReadSource() {
                                            }

                                            @Override
                                            protected Schema readSchema() {
                                                return new Schema(Collections.singletonList(field));
                                            }
                                        }) : null);
                        return (AdbcConnection) Proxy.newProxyInstance(getClass().getClassLoader(),
                                new Class<?>[] {AdbcConnection.class}, (proxy, method, args) ->
                                        "createStatement".equals(method.getName()) ? statement : null);
                    }

                    @Override
                    public void close() {
                    }
                });
        AtomicReference<Object[]> decoded = new AtomicReference<>();
        AtomicReference<Throwable> readFailure = new AtomicReference<>();
        Thread reader = null;
        try {
            CdcClient.SnapshotCursor cursor = client.openSnapshot("db", "t",
                    Collections.singletonList(new ColumnMeta("id", "int", "int", 0, false)), 1);
            reader = new Thread(() -> {
                try {
                    decoded.set(cursor.next());
                } catch (Throwable e) {
                    readFailure.set(e);
                }
            });
            reader.start();
            assertTrue(decoding.await(2, TimeUnit.SECONDS));
            cursor.close();
            assertFalse("snapshot reader closed while decoding a row", readerClosed.await(50, TimeUnit.MILLISECONDS));
        } finally {
            release.countDown();
            if (reader != null) {
                reader.join(2000);
            }
            client.close();
        }
        assertFalse("snapshot read thread did not exit", reader.isAlive());
        assertTrue("snapshot read failed: " + readFailure.get(), readFailure.get() == null);
        assertTrue(Arrays.equals(new Object[] {42}, decoded.get()));
        assertTrue(readerClosed.await(2, TimeUnit.SECONDS));
        assertFalse(closedDuringDecode.get());
    }

    @Test
    public void stopWaitsForSnapshotResourcesBeforeClosingDatabase() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:mysql://localhost:9030");
        props.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db");
        props.put(StarRocksCdcSourceConfig.USERNAME, "root");
        props.put(StarRocksCdcSourceConfig.PASSWORD, "");
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t");
        props.put(StarRocksCdcSourceConfig.CONNECTOR_NAME, "test");
        props.put(StarRocksCdcSourceConfig.ADBC_URI, "grpc+tcp://localhost:9408");
        CountDownLatch readerClosing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch databaseClosed = new CountDownLatch(1);
        StarRocksAdbcClient client = new StarRocksAdbcClient(new StarRocksCdcSourceConfig(props),
                (allocator, options) -> new AdbcDatabase() {
                    @Override
                    public AdbcConnection connect() {
                        AdbcStatement statement = (AdbcStatement) Proxy.newProxyInstance(
                                getClass().getClassLoader(), new Class<?>[] {AdbcStatement.class},
                                (proxy, method, args) -> "executeQuery".equals(method.getName())
                                        ? new AdbcStatement.QueryResult(0, new ArrowReader(allocator) {
                                            @Override
                                            public boolean loadNextBatch() {
                                                return false;
                                            }

                                            @Override
                                            public long bytesRead() {
                                                return 0;
                                            }

                                            @Override
                                            public void close() {
                                                readerClosing.countDown();
                                                awaitIgnoringInterrupts(release);
                                            }

                                            @Override
                                            protected void closeReadSource() {
                                            }

                                            @Override
                                            protected Schema readSchema() {
                                                return new Schema(Collections.emptyList());
                                            }
                                        }) : null);
                        return (AdbcConnection) Proxy.newProxyInstance(getClass().getClassLoader(),
                                new Class<?>[] {AdbcConnection.class}, (proxy, method, args) ->
                                        "createStatement".equals(method.getName()) ? statement : null);
                    }

                    @Override
                    public void close() {
                        databaseClosed.countDown();
                    }
                });
        Thread cursorCloser = null;
        try {
            CdcClient.SnapshotCursor cursor = client.openSnapshot("db", "t",
                    Collections.singletonList(new ColumnMeta("id", "int", "int", 0, false)), 1);
            cursorCloser = new Thread(cursor::close);
            cursorCloser.start();
            assertTrue(readerClosing.await(2, TimeUnit.SECONDS));
            cursorCloser.join(1000);
            assertFalse("snapshot close waited for a stuck Flight reader", cursorCloser.isAlive());
            client.close();
            assertFalse("database closed before snapshot reader", databaseClosed.await(50, TimeUnit.MILLISECONDS));
        } finally {
            release.countDown();
            if (cursorCloser != null) {
                cursorCloser.join(2000);
            }
            client.close();
        }
        assertFalse("snapshot closer did not exit", cursorCloser.isAlive());
        assertTrue(databaseClosed.await(2, TimeUnit.SECONDS));
    }

    @Test
    public void changesCleanupTimeoutDetachesConnection() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:mysql://localhost:9030");
        props.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db");
        props.put(StarRocksCdcSourceConfig.USERNAME, "root");
        props.put(StarRocksCdcSourceConfig.PASSWORD, "");
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t");
        props.put(StarRocksCdcSourceConfig.CONNECTOR_NAME, "test");
        props.put(StarRocksCdcSourceConfig.ADBC_URI, "grpc+tcp://localhost:9408");
        props.put(StarRocksCdcSourceConfig.READ_TIMEOUT_MS, "100");
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch connectionClosed = new CountDownLatch(1);
        StarRocksAdbcClient client = new StarRocksAdbcClient(new StarRocksCdcSourceConfig(props),
                (allocator, options) -> new AdbcDatabase() {
                    @Override
                    public AdbcConnection connect() {
                        AdbcStatement statement = (AdbcStatement) Proxy.newProxyInstance(
                                getClass().getClassLoader(), new Class<?>[] {AdbcStatement.class},
                                (proxy, method, args) -> "executeQuery".equals(method.getName())
                                        ? new AdbcStatement.QueryResult(0, new ArrowReader(allocator) {
                                            @Override
                                            public boolean loadNextBatch() {
                                                return false;
                                            }

                                            @Override
                                            public long bytesRead() {
                                                return 0;
                                            }

                                            @Override
                                            public void close() {
                                                closing.countDown();
                                                awaitIgnoringInterrupts(release);
                                            }

                                            @Override
                                            protected void closeReadSource() {
                                            }

                                            @Override
                                            protected Schema readSchema() {
                                                return new Schema(Collections.emptyList());
                                            }
                                        }) : null);
                        return (AdbcConnection) Proxy.newProxyInstance(getClass().getClassLoader(),
                                new Class<?>[] {AdbcConnection.class}, (proxy, method, args) -> {
                                    if ("createStatement".equals(method.getName())) {
                                        return statement;
                                    }
                                    if ("close".equals(method.getName())) {
                                        connectionClosed.countDown();
                                    }
                                    return null;
                                });
                    }

                    @Override
                    public void close() {
                    }
                });
        long start = System.nanoTime();
        try {
            client.streamChanges("db", "t",
                    Collections.singletonList(new ColumnMeta("id", "int", "int", 0, false)), 1, 2,
                    (row, type, version) -> { });
            fail("Expected a bounded cleanup timeout");
        } catch (SQLException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("cleanup timed out"));
            assertTrue(closing.await(2, TimeUnit.SECONDS));
            assertFalse("connection closed before its reader", connectionClosed.await(50, TimeUnit.MILLISECONDS));
            assertTrue("cleanup blocked the caller", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 2000);
        } finally {
            release.countDown();
            client.close();
        }
        assertTrue(connectionClosed.await(2, TimeUnit.SECONDS));
    }

    @Test
    public void stopInterruptsChangesRowDecodeWithinCurrentBatch() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:mysql://localhost:9030");
        props.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db");
        props.put(StarRocksCdcSourceConfig.USERNAME, "root");
        props.put(StarRocksCdcSourceConfig.PASSWORD, "");
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "t");
        props.put(StarRocksCdcSourceConfig.CONNECTOR_NAME, "test");
        props.put(StarRocksCdcSourceConfig.ADBC_URI, "grpc+tcp://localhost:9408");
        FieldVector vector = (FieldVector) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {FieldVector.class}, (proxy, method, args) -> {
                    if ("isNull".equals(method.getName())) {
                        return false;
                    }
                    return "getObject".equals(method.getName()) ? 1 : null;
                });
        List<Field> fields = Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("change_type", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("row_version", FieldType.nullable(new ArrowType.Int(64, true)), null));
        VectorSchemaRoot root = new VectorSchemaRoot(fields, Arrays.asList(vector, vector, vector), 1000);
        StarRocksAdbcClient client = new StarRocksAdbcClient(new StarRocksCdcSourceConfig(props),
                (allocator, options) -> new AdbcDatabase() {
                    @Override
                    public AdbcConnection connect() {
                        AdbcStatement statement = (AdbcStatement) Proxy.newProxyInstance(
                                getClass().getClassLoader(), new Class<?>[] {AdbcStatement.class},
                                (proxy, method, args) -> "executeQuery".equals(method.getName())
                                        ? new AdbcStatement.QueryResult(1000, new ArrowReader(allocator) {
                                            private boolean loaded;

                                            @Override
                                            public boolean loadNextBatch() {
                                                if (loaded) {
                                                    return false;
                                                }
                                                loaded = true;
                                                return true;
                                            }

                                            @Override
                                            public VectorSchemaRoot getVectorSchemaRoot() {
                                                return root;
                                            }

                                            @Override
                                            public long bytesRead() {
                                                return 0;
                                            }

                                            @Override
                                            protected void closeReadSource() {
                                            }

                                            @Override
                                            protected Schema readSchema() {
                                                return new Schema(fields);
                                            }
                                        }) : null);
                        return (AdbcConnection) Proxy.newProxyInstance(getClass().getClassLoader(),
                                new Class<?>[] {AdbcConnection.class}, (proxy, method, args) ->
                                        "createStatement".equals(method.getName()) ? statement : null);
                    }

                    @Override
                    public void close() {
                    }
                });
        AtomicInteger consumed = new AtomicInteger();
        try {
            client.streamChanges("db", "t",
                    Collections.singletonList(new ColumnMeta("id", "int", "int", 0, false)), 1, 2,
                    (row, type, version) -> {
                        if (consumed.incrementAndGet() == 1) {
                            client.close();
                        }
                    });
            fail("Expected the stopped client to abort the current batch");
        } catch (SQLException expected) {
            assertTrue(expected.getMessage().contains("closed"));
        } finally {
            client.close();
        }
        assertTrue("stop did not interrupt row decoding", consumed.get() > 0 && consumed.get() <= 256);
    }
}
