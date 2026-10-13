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

import org.apache.arrow.flight.CallStatus;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class StarRocksJdbcClientTest {
    private static final List<ColumnMeta> COLS = Collections.singletonList(
            new ColumnMeta("id", "int", "int(11)", 0, false, true));

    @Test
    public void snapshotRetriesSameBookmarkOnNextFeAfterQueryOrStreamFailure() throws Exception {
        for (String scheme : Arrays.asList("mysql", "arrow-flight-sql")) {
            List<Exception> failures = new ArrayList<>(Collections.singletonList(
                    new SQLRecoverableException("connection lost", "08006")));
            if (scheme.equals("arrow-flight-sql")) {
                failures.add(CallStatus.UNAVAILABLE.withDescription("stream disconnected").toRuntimeException());
                failures.add(new RuntimeException(CallStatus.TIMED_OUT.toRuntimeException()));
            }
            for (Exception failure : failures) {
                for (boolean duringRead : new boolean[]{false, true}) {
                    ScriptedConnections connections = new ScriptedConnections(failure, duringRead);
                    connections.failCleanup = failure instanceof RuntimeException;
                    try (StarRocksJdbcClient client = new StarRocksJdbcClient(config(scheme), connections)) {
                        assertSnapshotFailure(client, connections);
                        assertEquals(Collections.singletonList("fe1"), connections.closed);
                        try (CdcClient.SnapshotCursor retry = client.openSnapshot("db1", "orders", COLS, 100L)) {
                            assertArrayEquals(new Object[]{1}, retry.next());
                            assertNull(retry.next());
                        }
                        assertEquals(Arrays.asList("fe1", "fe2"), connections.opened);
                        assertEquals(Arrays.asList("fe1", "fe2"), connections.closed);
                        assertEquals(2, connections.queries.size());
                        assertEquals(connections.queries.get(0), connections.queries.get(1));
                    }
                }
            }
        }
    }

    @Test
    public void changesNormalizeFlightErrorsAndRotateEvenWhenCleanupAlsoFails() throws Exception {
        for (boolean duringRead : new boolean[]{false, true}) {
            Exception failure = CallStatus.UNAVAILABLE.withDescription("stream disconnected").toRuntimeException();
            ScriptedConnections connections = new ScriptedConnections(failure, duringRead);
            connections.failCleanup = true;
            List<Integer> rows = new ArrayList<>();
            try (StarRocksJdbcClient client = new StarRocksJdbcClient(config("arrow-flight-sql"), connections)) {
                try {
                    client.streamChanges("db1", "orders", COLS, 100L, 101L,
                            (row, changeType, version) -> rows.add((Integer) row[0]));
                    fail("expected read failure");
                } catch (SQLException e) {
                    assertSame(failure, e.getCause());
                }
                assertEquals(duringRead ? Collections.singletonList(1) : Collections.emptyList(), rows);
                rows.clear(); // The task discards the failed window before retrying it.
                client.streamChanges("db1", "orders", COLS, 100L, 101L,
                        (row, changeType, version) -> rows.add((Integer) row[0]));
                assertEquals(Collections.singletonList(1), rows);
                assertEquals(Arrays.asList("fe1", "fe2"), connections.opened);
                assertEquals(connections.queries.get(0), connections.queries.get(1));
            }
        }
    }

    @Test
    public void snapshotRetainsEndpointSelectedDuringFlightConnectFailover() throws Exception {
        ScriptedConnections connections = new ScriptedConnections(null, false);
        List<String> attempts = new ArrayList<>();
        FeConnection.ConnectionFactory factory = (url, props) -> {
            String host = url.contains("fe1:") ? "fe1" : "fe2";
            attempts.add(host);
            if (host.equals("fe1")) {
                throw CallStatus.UNAVAILABLE.withDescription("FE down").toRuntimeException();
            }
            return connections.open(url, props);
        };
        try (StarRocksJdbcClient client = new StarRocksJdbcClient(config("arrow-flight-sql"), factory)) {
            for (int i = 0; i < 2; i++) {
                try (CdcClient.SnapshotCursor cursor = client.openSnapshot("db1", "orders", COLS, 100L)) {
                    assertArrayEquals(new Object[]{1}, cursor.next());
                    assertNull(cursor.next());
                }
            }
            assertEquals(Arrays.asList("fe1", "fe2", "fe2"), attempts);
        }
    }

    @Test
    public void uncheckedFlightBookmarkAndMetadataQueryFailuresUseSqlRecovery() throws Exception {
        for (boolean bookmark : new boolean[]{false, true}) {
            Exception failure = CallStatus.UNAVAILABLE.withDescription("query disconnected").toRuntimeException();
            ScriptedConnections connections = new ScriptedConnections(failure, false);
            try (StarRocksJdbcClient client = new StarRocksJdbcClient(config("arrow-flight-sql"), connections)) {
                if (bookmark) {
                    assertEquals(100L, client.bookmarkCreate("db1", "orders", "holder", 1000L));
                } else {
                    try {
                        client.fetchColumns("db1", "orders");
                        fail("expected metadata query failure");
                    } catch (SQLException e) {
                        assertSame(failure, e.getCause());
                    }
                    assertEquals(1L, client.fetchTableConfig("db1", "orders").tableId);
                }
                assertEquals(Arrays.asList("fe1", "fe2"), connections.opened);
            }
        }
    }

    @Test
    public void snapshotSqlErrorsDoNotRotateAndOtherRuntimeErrorsRemainFatal() throws Exception {
        for (Exception failure : Arrays.asList(new SQLException("syntax error", "42000"),
                CallStatus.INVALID_ARGUMENT.withDescription("invalid query").toRuntimeException(),
                new DataException("bad decoded value"))) {
            ScriptedConnections connections = new ScriptedConnections(failure, true);
            try (StarRocksJdbcClient client = new StarRocksJdbcClient(config("arrow-flight-sql"), connections)) {
                if (failure instanceof DataException) {
                    try (CdcClient.SnapshotCursor cursor = client.openSnapshot("db1", "orders", COLS, 100L)) {
                        cursor.next();
                        cursor.next();
                        fail("expected fatal decode error");
                    } catch (DataException e) {
                        assertSame(failure, e);
                    }
                } else {
                    assertSnapshotFailure(client, connections);
                }
                connections.failure = null;
                try (CdcClient.SnapshotCursor retry = client.openSnapshot("db1", "orders", COLS, 100L)) {
                    assertArrayEquals(new Object[]{1}, retry.next());
                }
                assertEquals(Arrays.asList("fe1", "fe1"), connections.opened);
                assertEquals(Arrays.asList("fe1", "fe1"), connections.closed);
            }
        }
    }

    @Test
    public void snapshotRotationDoesNotCloseOtherCursorsOrMoveBookmarkConnection() throws Exception {
        ScriptedConnections connections = new ScriptedConnections(null, false);
        try (StarRocksJdbcClient client = new StarRocksJdbcClient(config("mysql"), connections);
             CdcClient.SnapshotCursor first = client.openSnapshot("db1", "other", COLS, 99L)) {
            connections.failure = new SQLRecoverableException("connection lost", "08006");
            assertSnapshotFailure(client, connections);
            connections.failure = null;
            assertEquals(100L, client.bookmarkCreate("db1", "orders", "holder", 1000L));
            try (CdcClient.SnapshotCursor retry = client.openSnapshot("db1", "orders", COLS, 100L)) {
                assertArrayEquals(new Object[]{1}, first.next());
                first.close();
                first.close();
                assertArrayEquals(new Object[]{1}, retry.next());
                assertNull(retry.next());
            }
            assertEquals(Arrays.asList("fe1", "fe1", "fe1", "fe2"), connections.opened);
        }
    }

    @Test
    public void changesConsumerFailureIsNotRetriedAsSql() throws Exception {
        ScriptedConnections connections = new ScriptedConnections(null, false);
        DataException failure = new DataException("mapper failed");
        try (StarRocksJdbcClient client = new StarRocksJdbcClient(config("arrow-flight-sql"), connections)) {
            try {
                client.streamChanges("db1", "orders", COLS, 100L, 101L,
                        (row, changeType, version) -> { throw failure; });
                fail("expected fatal consumer error");
            } catch (DataException e) {
                assertSame(failure, e);
            }
            client.streamChanges("db1", "orders", COLS, 100L, 101L, (row, changeType, version) -> { });
            assertEquals(Collections.singletonList("fe1"), connections.opened);
        }
    }

    private static void assertSnapshotFailure(StarRocksJdbcClient client, ScriptedConnections connections)
            throws Exception {
        try (CdcClient.SnapshotCursor cursor = client.openSnapshot("db1", "orders", COLS, 100L)) {
            assertArrayEquals(new Object[]{1}, cursor.next());
            cursor.next();
            fail("expected snapshot failure");
        } catch (SQLException e) {
            assertSame(connections.failure, connections.failure instanceof SQLException ? e : e.getCause());
        }
    }

    private static StarRocksCdcSourceConfig config(String scheme) {
        Map<String, String> props = new HashMap<>();
        props.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:" + scheme + "://fe1:9030,fe2:9030");
        props.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db1");
        props.put(StarRocksCdcSourceConfig.USERNAME, "root");
        props.put(StarRocksCdcSourceConfig.PASSWORD, "");
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "orders");
        props.put(StarRocksCdcSourceConfig.CONNECTOR_NAME, "c1");
        return new StarRocksCdcSourceConfig(props);
    }

    /** Only driver calls are simulated; client query, decode, failover and cursor ownership stay real. */
    private static final class ScriptedConnections implements FeConnection.ConnectionFactory {
        Exception failure;
        final boolean duringRead;
        boolean failCleanup;
        final List<String> opened = new ArrayList<>();
        final List<String> closed = new ArrayList<>();
        final List<String> queries = new ArrayList<>();

        ScriptedConnections(Exception failure, boolean duringRead) {
            this.failure = failure;
            this.duringRead = duringRead;
        }

        @Override
        public Connection open(String url, Properties props) {
            String host = url.contains("fe1:") ? "fe1" : "fe2";
            opened.add(host);
            boolean[] isClosed = {false};
            return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "isClosed": return isClosed[0];
                            case "close":
                                if (!isClosed[0]) {
                                    closed.add(host);
                                    isClosed[0] = true;
                                }
                                cleanup(host);
                                return null;
                            case "createStatement": return statement(host, isClosed);
                            default: throw new AssertionError(method);
                        }
                    });
        }

        private void cleanup(String host) {
            if (failCleanup && host.equals("fe1")) {
                throw new RuntimeException(CallStatus.UNAVAILABLE.withDescription("close failed").toRuntimeException());
            }
        }

        private Statement statement(String host, boolean[] connectionClosed) {
            return (Statement) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{Statement.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "close": cleanup(host); return null;
                            case "execute": return false; // MySQL session setup
                            case "setFetchSize": return null;
                            case "executeQuery":
                                queries.add((String) args[0]);
                                if (host.equals("fe1") && failure != null && !duringRead) {
                                    throw failure;
                                }
                                return result(host, connectionClosed);
                            default: throw new AssertionError(method);
                        }
                    });
        }

        private ResultSet result(String host, boolean[] connectionClosed) {
            int[] advances = {0};
            return (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{ResultSet.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "next":
                                if (connectionClosed[0]) {
                                    throw new AssertionError("another cursor closed this connection");
                                }
                                if (advances[0]++ == 0) {
                                    return true;
                                }
                                if (host.equals("fe1") && failure != null && duringRead) {
                                    throw failure;
                                }
                                return false;
                            case "getInt": return 1;
                            case "getLong": return 1L;
                            case "getString": return "100";
                            case "wasNull": return false;
                            case "close": cleanup(host); return null;
                            default: throw new AssertionError(method);
                        }
                    });
        }
    }
}
