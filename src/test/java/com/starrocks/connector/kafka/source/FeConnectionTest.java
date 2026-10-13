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

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * URL fan-out is the whole transport switch: {@code starrocks.jdbc.url} carries no separate
 * transport key, so whichever scheme it names decides which driver runs and whether the
 * MySQL-only streaming knobs apply.
 */
public class FeConnectionTest {

    @Test
    public void testInitialConnectionFailsOverForJdbcAndFlightConnectionErrors() throws Exception {
        for (String scheme : Arrays.asList("mysql", "arrow-flight-sql")) {
            List<SQLException> failures = new ArrayList<>(Arrays.asList(
                    new SQLNonTransientConnectionException("connection refused"),
                    new SQLTransientConnectionException("connection lost"),
                    new SQLRecoverableException("connection must be reopened"),
                    new SQLException("connection lost", "08006"),
                    new SQLException("transport failed", new IOException("connection reset"))));
            if (scheme.equals("arrow-flight-sql")) {
                failures.add(new SQLException("Failed to connect.",
                        org.apache.arrow.flight.CallStatus.UNAVAILABLE.toRuntimeException()));
                failures.add(new SQLException("Failed to connect.",
                        org.apache.arrow.flight.CallStatus.TIMED_OUT.toRuntimeException()));
            }
            for (SQLException failure : failures) {
                ScriptedConnections connections = new ScriptedConnections();
                connections.connectFailures.put("fe1", failure);
                try (FeConnection fe = new FeConnection(config(scheme, "fe1:9030,fe2:9030"), connections)) {
                    Connection healthy = fe.get();
                    assertSame(healthy, fe.get());
                    assertEquals(Arrays.asList("fe1", "fe2"), connections.opened);
                    assertEquals(scheme.equals("mysql") ? Collections.singletonList("fe2")
                            : Collections.emptyList(), connections.sessionPinned);
                }
            }
        }
    }

    @Test
    public void testBookmarkSkipsUnavailableHostThenFollowsLeaderRedirect() throws Exception {
        ScriptedConnections connections = new ScriptedConnections();
        connections.connectFailures.put("fe1", new SQLNonTransientConnectionException("connection refused"));
        connections.queryFailures.put("fe2", new SQLException("must run on the FE leader"));
        try (FeConnection fe = new FeConnection(config("mysql", "fe1:9030,fe2:9030,fe3:9030"), connections)) {
            assertEquals("100", fe.executeOnLeader("SELECT bookmark_create(...)"));
            assertEquals(Arrays.asList("fe1", "fe2", "fe3"), connections.opened);
            assertEquals(Collections.singletonList("fe2"), connections.closed);
        }
    }

    @Test
    public void testBrokenEstablishedConnectionMovesBookmarkAndMetadataReads() throws Exception {
        for (boolean bookmark : new boolean[]{false, true}) {
            ScriptedConnections connections = new ScriptedConnections();
            SQLException failure = new SQLException("connection lost", "08006");
            connections.queryFailures.put("fe1", failure);
            try (FeConnection fe = new FeConnection(config("mysql", "fe1:9030,fe2:9030"), connections)) {
                Connection old = fe.get();
                if (bookmark) {
                    assertEquals("100", fe.executeOnLeader("SELECT bookmark_create(...)"));
                } else {
                    try (Statement statement = old.createStatement()) {
                        statement.executeQuery("SELECT * FROM information_schema.tables_config");
                        fail("expected connection failure");
                    } catch (SQLException e) {
                        assertSame(failure, e);
                        fe.closeIfBroken(e);
                    }
                    fe.get();
                }
                assertTrue(old.isClosed());
                assertEquals(Arrays.asList("fe1", "fe2"), connections.opened);
                assertEquals(Collections.singletonList("fe1"), connections.closed);
            }
        }
    }

    @Test
    public void testAllHostsUnavailableStopsAfterOnePass() throws Exception {
        ScriptedConnections connections = new ScriptedConnections();
        SQLException failure = new SQLNonTransientConnectionException("connection refused");
        connections.connectFailures.put("fe1", failure);
        connections.connectFailures.put("fe2", failure);
        try (FeConnection fe = new FeConnection(config("mysql", "fe1:9030,fe2:9030"), connections)) {
            for (boolean bookmark : new boolean[]{false, true}) {
                connections.opened.clear();
                try {
                    if (bookmark) {
                        fe.executeOnLeader("SELECT bookmark_create(...)");
                    } else {
                        fe.get();
                    }
                    fail("expected connection failure");
                } catch (SQLException e) {
                    assertSame(failure, e);
                }
                assertEquals(Arrays.asList("fe1", "fe2"), connections.opened);
            }
        }
    }

    @Test
    public void testAuthenticationAndSqlErrorsDoNotRotate() throws Exception {
        for (boolean authentication : new boolean[]{false, true}) {
            ScriptedConnections connections = new ScriptedConnections();
            SQLException failure = new SQLException(authentication ? "access denied" : "syntax error",
                    authentication ? "28000" : "42000");
            (authentication ? connections.connectFailures : connections.queryFailures).put("fe1", failure);
            try (FeConnection fe = new FeConnection(config("mysql", "fe1:9030,fe2:9030"), connections)) {
                try {
                    if (authentication) {
                        fe.get();
                    } else {
                        fe.executeOnLeader("bad SQL");
                    }
                    fail("expected SQL error");
                } catch (SQLException e) {
                    assertSame(failure, e);
                    fe.closeIfBroken(e);
                }
                assertEquals(Collections.singletonList("fe1"), connections.opened);
                assertTrue(connections.closed.isEmpty());
            }
        }
    }

    private static StarRocksCdcSourceConfig config(String scheme, String hosts) {
        Map<String, String> props = new HashMap<>();
        props.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:" + scheme + "://" + hosts);
        props.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db1");
        props.put(StarRocksCdcSourceConfig.USERNAME, "root");
        props.put(StarRocksCdcSourceConfig.PASSWORD, "");
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "orders");
        props.put(StarRocksCdcSourceConfig.CONNECTOR_NAME, "c1");
        props.put(StarRocksCdcSourceConfig.MAX_RETRIES, "1");
        return new StarRocksCdcSourceConfig(props);
    }

    /** Inject only the dial-out; connection caching, session setup, SQL retries and cleanup stay real. */
    private static final class ScriptedConnections implements FeConnection.ConnectionFactory {
        final Map<String, SQLException> connectFailures = new HashMap<>();
        final Map<String, SQLException> queryFailures = new HashMap<>();
        final List<String> opened = new ArrayList<>();
        final List<String> closed = new ArrayList<>();
        final List<String> sessionPinned = new ArrayList<>();

        @Override
        public Connection open(String url, Properties props) throws SQLException {
            String host = url.substring(url.indexOf("://") + 3).split(":")[0];
            opened.add(host);
            if (connectFailures.containsKey(host)) {
                throw connectFailures.get(host);
            }
            boolean[] isClosed = {false};
            return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "isClosed": return isClosed[0];
                            case "close":
                                isClosed[0] = true;
                                closed.add(host);
                                return null;
                            case "createStatement": return statement(host);
                            default: throw new AssertionError(method);
                        }
                    });
        }

        private Statement statement(String host) {
            return (Statement) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{Statement.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "close": return null;
                            case "execute":
                                assertEquals(FeConnection.SESSION_SETUP_SQL, args[0]);
                                sessionPinned.add(host);
                                return false;
                            case "executeQuery":
                                if (queryFailures.containsKey(host)) {
                                    throw queryFailures.get(host);
                                }
                                return result();
                            default: throw new AssertionError(method);
                        }
                    });
        }

        private ResultSet result() {
            return (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{ResultSet.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "next": return true;
                            case "getString": return "100";
                            case "close": return null;
                            default: throw new AssertionError(method);
                        }
                    });
        }
    }

    /**
     * {@code stop()} is the one call the Connect runtime may make off the poll thread, and it lands
     * on {@code close()}. A later {@code get()} must refuse rather than open a connection that the
     * already-finished {@code stop()} can no longer close.
     */
    @Test
    public void testCloseIsTerminalAndDoesNotReopen() {
        Map<String, String> props = new HashMap<>();
        props.put(StarRocksCdcSourceConfig.JDBC_URL, "jdbc:mysql://127.0.0.1:1");
        props.put(StarRocksCdcSourceConfig.DATABASE_NAME, "db1");
        props.put(StarRocksCdcSourceConfig.USERNAME, "root");
        props.put(StarRocksCdcSourceConfig.PASSWORD, "");
        props.put(StarRocksCdcSourceConfig.TABLE_NAMES, "orders");
        props.put(StarRocksCdcSourceConfig.CONNECTOR_NAME, "c1");
        FeConnection fe = new FeConnection(new StarRocksCdcSourceConfig(props));
        fe.close();
        try {
            fe.get();
            fail("get() after close() must not hand back a connection");
        } catch (SQLException e) {
            assertTrue("expected a closed-state refusal, not a driver dial-out: " + e.getMessage(),
                    e.getMessage().contains("closed"));
        }
    }

    /** jdbc:mysql is rewritten to jdbc:mariadb, because that is the driver actually bundled. */
    @Test
    public void testMysqlHostsFanOutAndAreRewrittenToMariadb() {
        assertEquals(Arrays.asList("jdbc:mariadb://fe1:9030", "jdbc:mariadb://fe2:9030"),
                FeConnection.parseUrls("jdbc:mysql://fe1:9030,fe2:9030"));
    }

    /** The Arrow scheme must survive untouched -- rewriting it would hand it to the wrong driver. */
    @Test
    public void testArrowFlightSchemeIsNotRewritten() {
        assertEquals(Collections.singletonList("jdbc:arrow-flight-sql://fe1:9408"),
                FeConnection.parseUrls("jdbc:arrow-flight-sql://fe1:9408"));
    }

    /**
     * A plaintext Flight server needs {@code useEncryption=false}, so the query string has to be
     * replicated onto every host the list fans out to -- not left attached to the last one.
     */
    @Test
    public void testArrowFlightQueryStringIsCopiedToEveryHost() {
        assertEquals(
                Arrays.asList(
                        "jdbc:arrow-flight-sql://fe1:9408?useEncryption=false&useSSL=false",
                        "jdbc:arrow-flight-sql://fe2:9408?useEncryption=false&useSSL=false"),
                FeConnection.parseUrls("jdbc:arrow-flight-sql://fe1:9408,fe2:9408?useEncryption=false&useSSL=false"));
    }

    /** A database path applies to every host too, for the same reason the query string does. */
    @Test
    public void testDatabasePathIsCopiedToEveryHost() {
        assertEquals(
                Arrays.asList("jdbc:mariadb://fe1:9030/db1", "jdbc:mariadb://fe2:9030/db1"),
                FeConnection.parseUrls("jdbc:mysql://fe1:9030,fe2:9030/db1"));
    }

    @Test
    public void testSingleHostIsUnchangedApartFromTheScheme() {
        assertEquals(Collections.singletonList("jdbc:mariadb://fe1:9030"),
                FeConnection.parseUrls("jdbc:mysql://fe1:9030"));
    }

    @Test
    public void testBlankHostsBetweenCommasAreDropped() {
        assertEquals(Arrays.asList("jdbc:mariadb://fe1:9030", "jdbc:mariadb://fe2:9030"),
                FeConnection.parseUrls("jdbc:mysql://fe1:9030, ,fe2:9030"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUrlWithoutSchemeSeparatorIsRejected() {
        FeConnection.parseUrls("fe1:9030");
    }

    /** Only the mysql prefix is rewritten; anything else passes through as-is. */
    @Test
    public void testOnlyTheMysqlSchemeIsRewritten() {
        assertEquals("jdbc:mariadb://h:1", FeConnection.rewriteMariadbScheme("jdbc:mysql://h:1"));
        assertEquals("jdbc:mariadb://h:1", FeConnection.rewriteMariadbScheme("jdbc:mariadb://h:1"));
        assertEquals("jdbc:arrow-flight-sql://h:1", FeConnection.rewriteMariadbScheme("jdbc:arrow-flight-sql://h:1"));
    }

    /**
     * Each transport must pull only its own driver. Registering both eagerly killed the MySQL
     * transport on a Java 8 worker: the Arrow driver is compiled for Java 11, and the resulting
     * UnsupportedClassVersionError is a LinkageError, so the ClassNotFoundException handler never
     * saw it and the class stayed poisoned. The Java 8 behaviour itself needs a Java 8 runtime;
     * what is pinned here is that a mysql URL never names the Arrow driver at all.
     */
    @Test
    public void testEachTransportNamesOnlyItsOwnDriver() {
        assertEquals("org.mariadb.jdbc.Driver",
                FeConnection.driverClassFor("jdbc:mariadb://fe1:9030/db"));
        assertEquals("org.mariadb.jdbc.Driver",
                FeConnection.driverClassFor("jdbc:mysql://fe1:9030/db"));
        assertEquals("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver",
                FeConnection.driverClassFor("jdbc:arrow-flight-sql://fe1:9040?useEncryption=false"));
    }

    /** A driver that is simply absent must still name the artifact to add. */
    /**
     * The session pin and the text reader must agree on the VARBINARY encoding, and the pin must
     * stay off the Arrow transport, which carries binary as binary.
     */
    @Test
    public void testMysqlSessionIsPinnedToTheEncodingTheTextReaderScans() {
        String expected = MysqlValueReader.SESSION_BINARY_ENCODING.name().toLowerCase(java.util.Locale.ROOT);
        assertTrue(FeConnection.SESSION_SETUP_SQL,
                FeConnection.SESSION_SETUP_SQL.contains("binary_encoding_format = '" + expected + "'"));
        assertTrue(FeConnection.SESSION_SETUP_SQL,
                FeConnection.SESSION_SETUP_SQL.contains("binary_encoding_level = 'nested'"));
        assertTrue(FeConnection.pinsSession("jdbc:mysql://fe1:9030"));
        assertTrue(FeConnection.pinsSession("jdbc:mariadb://fe1:9030"));
        assertFalse(FeConnection.pinsSession("jdbc:arrow-flight-sql://fe1:9408?useEncryption=false"));
    }

    @Test
    public void testAMissingDriverNamesItsArtifact() {
        try {
            FeConnection.registerDriver("com.example.NoSuchDriver", "some-artifact");
            fail("expected an IllegalStateException");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("some-artifact"));
        }
    }
}
