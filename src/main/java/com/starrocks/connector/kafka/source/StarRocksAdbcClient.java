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
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** Uses JDBC for bookmarks and metadata, and Flight SQL batches for snapshot and CHANGES reads. */
final class StarRocksAdbcClient implements CdcClient {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksAdbcClient.class);
    private static final int MAX_PENDING_ADBC_CALLS = 8;
    interface DatabaseOpener {
        AdbcDatabase open(BufferAllocator allocator, Map<String, Object> options) throws AdbcException;
    }

    private final StarRocksJdbcClient jdbc;
    private final StarRocksCdcSourceConfig config;
    private final List<String> adbcUris;
    private final DatabaseOpener databaseOpener;
    private BufferAllocator allocator;
    private AdbcDatabase database;
    private final Map<String, AdbcDatabase> databases = new HashMap<>();
    private int adbcUriIndex;
    private final AtomicReference<AdbcConnection> connection = new AtomicReference<>();
    private final ArrowAdbcValueReader valueReader = new ArrowAdbcValueReader();
    private final boolean readTimings;
    private final Set<Snapshot> snapshots = new HashSet<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object lifecycle = new Object();
    private final Set<TimedCall<?>> timedCalls = new HashSet<>();
    private final Map<AdbcConnection, Integer> connectionLeases = new IdentityHashMap<>();
    private final Set<AdbcConnection> retiredConnections =
            Collections.newSetFromMap(new IdentityHashMap<AdbcConnection, Boolean>());
    private final AtomicInteger workerNumber = new AtomicInteger();
    private final ExecutorService readWorkers = Executors.newCachedThreadPool(work -> {
        Thread worker = new Thread(work, "starrocks-cdc-adbc-read-" + workerNumber.incrementAndGet());
        worker.setDaemon(true);
        return worker;
    });
    private int activeOperations;
    private int pendingCloses;
    private boolean closePrepared;
    private boolean resourcesReleased;

    StarRocksAdbcClient(StarRocksCdcSourceConfig config) {
        this(config, (allocator, options) -> new FlightSqlDriver(allocator).open(options));
    }

    StarRocksAdbcClient(StarRocksCdcSourceConfig config, DatabaseOpener databaseOpener) {
        this.config = config;
        this.jdbc = new StarRocksJdbcClient(config);
        this.adbcUris = config.adbcUris();
        this.databaseOpener = databaseOpener;
        this.readTimings = config.readTimingsEnabled() && LOG.isDebugEnabled();
    }

    @Override
    public long bookmarkCreate(String db, String table, String holder, long ttlMs) throws SQLException {
        return jdbc.bookmarkCreate(db, table, holder, ttlMs);
    }

    @Override
    public long bookmarkRenew(String db, String table, long bookmarkId, String holder, long ttlMs)
            throws SQLException {
        return jdbc.bookmarkRenew(db, table, bookmarkId, holder, ttlMs);
    }

    @Override
    public void bookmarkRelease(String db, String table, long bookmarkId, String holder) throws SQLException {
        jdbc.bookmarkRelease(db, table, bookmarkId, holder);
    }

    @Override
    public List<Long> fetchHeldBookmarks(String db, String table, String holder) throws SQLException {
        return jdbc.fetchHeldBookmarks(db, table, holder);
    }

    @Override
    public List<ColumnMeta> fetchColumns(String db, String table) throws SQLException {
        List<ColumnMeta> cols = jdbc.fetchColumns(db, table);
        ensureSupportedColumns(db, table, cols);
        return cols;
    }

    static void ensureSupportedColumns(String db, String table, List<ColumnMeta> cols) throws SQLException {
        for (ColumnMeta col : cols) {
            if (col.type.kind == ColumnType.Kind.OPAQUE) {
                throw new SQLException("Arrow ADBC cannot preserve the server-rendered text for column "
                        + db + "." + table + "." + col.name + " (" + col.srColumnType + "); "
                        + "use source.read.transport=jdbc for this table");
            }
        }
    }

    @Override
    public TableConfig fetchTableConfig(String db, String table) throws SQLException {
        return jdbc.fetchTableConfig(db, table);
    }

    synchronized void connectAdbc() throws SQLException {
        if (closed.get()) {
            throw new SQLException("ADBC client is closed");
        }
        if (connection.get() != null) {
            return;
        }
        try {
            if (allocator == null) {
                allocator = new RootAllocator();
            }
            if (database == null) {
                String uri = adbcUris.get(adbcUriIndex);
                database = databases.get(uri);
                if (database == null) {
                    Map<String, Object> options = new HashMap<>();
                    AdbcDriver.PARAM_URI.set(options, uri);
                    AdbcDriver.PARAM_USERNAME.set(options, config.username());
                    AdbcDriver.PARAM_PASSWORD.set(options, config.password());
                    database = databaseOpener.open(allocator, options);
                    databases.put(uri, database);
                }
            }
            publishConnection(connectWithTimeout(database));
        } catch (Exception e) {
            rotateAdbcEndpoint();
            throw readFailure(e);
        }
    }

    private synchronized void rotateAdbcEndpoint() {
        int previous = adbcUriIndex;
        database = null;
        adbcUriIndex = (adbcUriIndex + 1) % adbcUris.size();
        if (adbcUriIndex != previous) {
            LOG.warn("Rotating ADBC read endpoint from {} to {}",
                    adbcUris.get(previous), adbcUris.get(adbcUriIndex));
        }
    }

    private void publishConnection(AdbcConnection connected) throws SQLException {
        synchronized (lifecycle) {
            if (!closed.get()) {
                connection.set(connected);
                return;
            }
        }
        closeConnectionAsync(connected);
        throw new SQLException("ADBC client is closed");
    }

    AdbcConnection connectWithTimeout(AdbcDatabase source) throws SQLException {
        return new TimedCall<>(source::connect, config.connectTimeoutMs(), "ADBC connection",
                StarRocksAdbcClient::closeQuietly, null).execute();
    }

    private final class TimedCall<T> implements Runnable {
        private final Callable<T> work;
        private final long timeoutMs;
        private final String label;
        private final Consumer<T> closeLateResult;
        private final Runnable cleanupAfterAbandon;
        private final CountDownLatch finished = new CountDownLatch(1);
        private final CountDownLatch decision = new CountDownLatch(1);
        private volatile Thread thread;
        private T result;
        private Throwable failure;
        private boolean abandoned;
        private boolean resolved;

        private TimedCall(Callable<T> work, long timeoutMs, String label,
                          Consumer<T> closeLateResult, Runnable cleanupAfterAbandon) {
            this.work = work;
            this.timeoutMs = timeoutMs;
            this.label = label;
            this.closeLateResult = closeLateResult;
            this.cleanupAfterAbandon = cleanupAfterAbandon;
        }

        private T execute() throws SQLException {
            start();
            return awaitResult();
        }

        private T awaitResult() throws SQLException {
            try {
                if (!finished.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                    abandon();
                    throw new SQLException(label + " timed out after " + timeoutMs + " ms");
                }
                return take();
            } catch (InterruptedException e) {
                abandon();
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted while waiting for " + label, e);
            }
        }

        private void start() throws SQLException {
            synchronized (lifecycle) {
                if (closed.get()) {
                    throw new SQLException("ADBC client is closed");
                }
                long unfinished = timedCalls.stream().filter(TimedCall::countsAgainstLimit).count()
                        + pendingCloses;
                if (unfinished >= MAX_PENDING_ADBC_CALLS) {
                    throw new ConnectException("ADBC has " + MAX_PENDING_ADBC_CALLS
                            + " calls or resource closes still running; the task cannot safely retry");
                }
                timedCalls.add(this);
                try {
                    readWorkers.execute(this);
                } catch (RuntimeException e) {
                    timedCalls.remove(this);
                    if (e instanceof RejectedExecutionException && closed.get()) {
                        throw new SQLException("ADBC client is closed", e);
                    }
                    throw e;
                }
            }
        }

        @Override
        public void run() {
            thread = Thread.currentThread();
            try {
                if (!wasAbandoned()) {
                    T value = work.call();
                    synchronized (this) {
                        result = value;
                    }
                }
            } catch (Throwable e) {
                synchronized (this) {
                    failure = e;
                }
            } finally {
                finished.countDown();
                boolean interrupted = false;
                while (true) {
                    try {
                        decision.await();
                        break;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                T unused;
                boolean reject;
                synchronized (this) {
                    reject = abandoned && !resolved;
                    unused = reject ? result : null;
                    result = null;
                }
                try {
                    if (reject) {
                        try {
                            if (unused != null) {
                                closeLateResult.accept(unused);
                            }
                        } finally {
                            if (cleanupAfterAbandon != null) {
                                cleanupAfterAbandon.run();
                            }
                        }
                    }
                } finally {
                    synchronized (lifecycle) {
                        timedCalls.remove(this);
                    }
                    releaseResourcesIfIdle();
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        private T take() throws SQLException {
            T value = null;
            Throwable error;
            boolean cancelled;
            synchronized (this) {
                error = failure;
                cancelled = abandoned || closed.get();
                if (cancelled) {
                    abandoned = true;
                } else {
                    resolved = true;
                }
                if (!cancelled && error == null) {
                    value = result;
                    result = null;
                }
                decision.countDown();
            }
            if (cancelled) {
                throw new SQLException(label + " was cancelled");
            }
            if (error != null) {
                if (error instanceof Error) {
                    throw (Error) error;
                }
                throw readFailure((Exception) error);
            }
            return value;
        }

        private synchronized boolean abandon() {
            if (resolved) {
                return false;
            }
            abandoned = true;
            decision.countDown();
            Thread running = thread;
            if (running != null) {
                running.interrupt();
            }
            return true;
        }

        private synchronized boolean wasAbandoned() {
            return abandoned;
        }

        private synchronized boolean countsAgainstLimit() {
            return !resolved;
        }
    }

    private void enterOperation() throws SQLException {
        synchronized (lifecycle) {
            if (closed.get()) {
                throw new SQLException("ADBC client is closed");
            }
            activeOperations++;
        }
    }

    private void leaveOperation() {
        synchronized (lifecycle) {
            activeOperations--;
        }
        releaseResourcesIfIdle();
    }

    @Override
    public SnapshotCursor openSnapshot(String db, String table, List<ColumnMeta> cols, long bookmark)
            throws SQLException, NonTrackableException {
        String sql = SqlBuilder.snapshotSql(db, table, StarRocksJdbcClient.columnNames(cols), bookmark);
        CdcReadTimings timings = CdcReadTimings.forRead(readTimings);
        enterOperation();
        boolean connected = false;
        try {
            connectAdbc();
            connected = true;
            AdbcConnection readConnection = connectWithTimeout(database);
            AdbcStatement stmt = null;
            AdbcStatement.QueryResult result = null;
            TimedCall<AdbcStatement.QueryResult> queryCall = null;
            Snapshot snapshot = null;
            try {
                stmt = readConnection.createStatement();
                stmt.setSqlQuery(sql);
                AdbcStatement queryStatement = stmt;
                long start = timings == null ? 0 : System.nanoTime();
                queryCall = new TimedCall<>(queryStatement::executeQuery, config.readTimeoutMs(),
                        "ADBC snapshot query", StarRocksAdbcClient::closeQuietly,
                        () -> {
                            closeQuietly(queryStatement);
                            closeQuietly(readConnection);
                        });
                result = queryCall.execute();
                if (timings != null) {
                    timings.queryNanos += System.nanoTime() - start;
                }
                snapshot = new Snapshot(readConnection, stmt, result, cols, db, table, bookmark, timings);
                synchronized (snapshots) {
                    if (closed.get()) {
                        throw new SQLException("ADBC reader is closed");
                    }
                    snapshots.add(snapshot);
                }
                return snapshot;
            } catch (Exception e) {
                if (snapshot != null) {
                    snapshot.closeResources();
                } else if (queryCall == null || !queryCall.wasAbandoned()) {
                    AdbcStatement.QueryResult failedResult = result;
                    AdbcStatement failedStatement = stmt;
                    closeAsync(() -> {
                        closeQuietly(failedResult);
                        closeQuietly(failedStatement);
                        closeQuietly(readConnection);
                    });
                }
                throw e;
            }
        } catch (Exception e) {
            if (connected) {
                discardReadConnection();
            }
            SQLException failure = readFailure(e);
            NonTrackableException nonTrackable = NonTrackableException.classify(failure);
            if (nonTrackable != null) {
                throw nonTrackable;
            }
            throw failure;
        } finally {
            leaveOperation();
        }
    }

    private final class Snapshot implements CdcClient.SnapshotCursor {
        private final AdbcConnection readConnection;
        private final AdbcStatement stmt;
        private final AdbcStatement.QueryResult result;
        private final ArrowReader reader;
        private final List<ColumnMeta> cols;
        private final String db;
        private final String table;
        private final long bookmark;
        private final CdcReadTimings timings;
        private final AtomicBoolean done = new AtomicBoolean();
        private final AtomicBoolean resourcesClosed = new AtomicBoolean();
        private int activeReads;
        private boolean batchOwnsClose;
        private int rowIndex;
        private int batchCount;
        private List<FieldVector> vectors;
        private long rows;
        private volatile TimedCall<Boolean> activeBatch;

        private Snapshot(AdbcConnection readConnection, AdbcStatement stmt, AdbcStatement.QueryResult result,
                         List<ColumnMeta> cols, String db, String table, long bookmark, CdcReadTimings timings) {
            this.readConnection = readConnection;
            this.stmt = stmt;
            this.result = result;
            this.reader = result.getReader();
            this.cols = cols;
            this.db = db;
            this.table = table;
            this.bookmark = bookmark;
            this.timings = timings;
            synchronized (lifecycle) {
                activeOperations++;
            }
        }

        @Override
        public Object[] next() throws SQLException, NonTrackableException {
            enterOperation();
            boolean reading = false;
            try {
                synchronized (this) {
                    if (done.get()) {
                        throw new SQLException("Snapshot cursor is closed");
                    }
                    activeReads++;
                    reading = true;
                }
                return nextRow();
            } finally {
                try {
                    if (reading) {
                        boolean closeNow;
                        synchronized (this) {
                            activeReads--;
                            closeNow = done.get() && activeReads == 0 && !batchOwnsClose;
                        }
                        if (closeNow) {
                            closeResources();
                        }
                    }
                } finally {
                    leaveOperation();
                }
            }
        }

        private Object[] nextRow() throws SQLException, NonTrackableException {
            if (done.get()) {
                throw new SQLException("Snapshot cursor is closed");
            }
            try {
                while (rowIndex == batchCount) {
                    long start = timings == null ? 0 : System.nanoTime();
                    TimedCall<Boolean> batchCall = new TimedCall<>(reader::loadNextBatch,
                            config.readTimeoutMs(), "ADBC snapshot batch", value -> { }, this::closeResources);
                    boolean loaded;
                    try {
                        synchronized (this) {
                            if (done.get()) {
                                throw new SQLException("Snapshot cursor is closed");
                            }
                            batchCall.start();
                            activeBatch = batchCall;
                        }
                        loaded = batchCall.awaitResult();
                    } finally {
                        synchronized (this) {
                            if (!batchCall.wasAbandoned()) {
                                activeBatch = null;
                            }
                        }
                    }
                    if (timings != null) {
                        timings.advanceNanos += System.nanoTime() - start;
                    }
                    if (!loaded) {
                        close();
                        if (readTimings && LOG.isDebugEnabled()) {
                            LOG.debug("CDC read snapshot table={}.{} bookmark={} rows={} query_ms={} next_ms={} decode_ms={}",
                                    db, table, bookmark, rows, timings.queryNanos / 1e6,
                                    timings.advanceNanos / 1e6, timings.decodeNanos / 1e6);
                        }
                        return null;
                    }
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    validateColumns(root, cols.size());
                    rowIndex = 0;
                    batchCount = root.getRowCount();
                    vectors = root.getFieldVectors();
                }
                long start = timings == null ? 0 : System.nanoTime();
                Object[] row = valueReader.readRow(vectors, rowIndex++, cols);
                if (timings != null) {
                    timings.decodeNanos += System.nanoTime() - start;
                }
                rows++;
                return row;
            } catch (Exception e) {
                close();
                SQLException sql = readFailure(e);
                discardReadConnection();
                NonTrackableException nonTrackable = NonTrackableException.classify(sql);
                if (nonTrackable != null) {
                    throw nonTrackable;
                }
                throw sql;
            }
        }

        @Override
        public void close() {
            boolean closeNow;
            synchronized (this) {
                if (!done.compareAndSet(false, true)) {
                    return;
                }
                TimedCall<Boolean> pending = activeBatch;
                batchOwnsClose = pending != null && pending.abandon();
                closeNow = activeReads == 0 && !batchOwnsClose;
            }
            synchronized (snapshots) {
                snapshots.remove(this);
            }
            if (closeNow) {
                closeResources();
            }
        }

        private void closeResources() {
            if (!resourcesClosed.compareAndSet(false, true)) {
                return;
            }
            closeAsync(() -> {
                try {
                    closeQuietly(result);
                    closeQuietly(stmt);
                    closeQuietly(readConnection);
                } finally {
                    leaveOperation();
                }
            });
        }
    }

    private static void validateColumns(VectorSchemaRoot root, int expected) throws SQLException {
        if (root.getFieldVectors().size() != expected) {
            throw new SQLException("Unexpected ADBC column count: expected " + expected + ", got "
                    + root.getFieldVectors().size());
        }
    }

    @Override
    public void streamChanges(String db, String table, List<ColumnMeta> cols, long base, long head,
                              CdcClient.ChangeRowConsumer consumer) throws SQLException, NonTrackableException {
        String sql = SqlBuilder.changesSql(db, table, StarRocksJdbcClient.columnNames(cols), base, head);
        enterOperation();
        boolean connected = false;
        try {
            connectAdbc();
            connected = true;
            readChanges(sql, cols, consumer, db, table, base, head);
        } catch (SQLException e) {
            if (connected) {
                discardReadConnection();
            }
            NonTrackableException nonTrackable = NonTrackableException.classify(e);
            if (nonTrackable != null) {
                throw nonTrackable;
            }
            throw e;
        } finally {
            leaveOperation();
        }
    }

    private synchronized void discardReadConnection() {
        AdbcConnection failed;
        synchronized (lifecycle) {
            failed = detachReadConnectionLocked();
        }
        if (failed != null) {
            closeConnectionAsync(failed);
        }
        rotateAdbcEndpoint();
    }

    private AdbcConnection acquireReadConnection() throws SQLException {
        synchronized (lifecycle) {
            if (closed.get()) {
                throw new SQLException("ADBC reader is closed");
            }
            AdbcConnection active = connection.get();
            if (active == null) {
                throw new SQLException("ADBC read connection is closed");
            }
            connectionLeases.merge(active, 1, Integer::sum);
            return active;
        }
    }

    private void releaseReadConnection(AdbcConnection active) {
        boolean closeRetired = false;
        synchronized (lifecycle) {
            int leases = connectionLeases.get(active);
            if (leases == 1) {
                connectionLeases.remove(active);
                closeRetired = retiredConnections.remove(active);
            } else {
                connectionLeases.put(active, leases - 1);
            }
        }
        if (closeRetired) {
            closeConnectionAsync(active);
        }
    }

    // Called under lifecycle. The last reader closes a retired connection after its scope is closed.
    private AdbcConnection detachReadConnectionLocked() {
        AdbcConnection detached = connection.getAndSet(null);
        if (detached != null && connectionLeases.containsKey(detached)) {
            retiredConnections.add(detached);
            return null;
        }
        return detached;
    }

    private void closeConnectionAsync(AdbcConnection oldConnection) {
        closeAsync(() -> closeQuietly(oldConnection));
    }

    private CountDownLatch closeAsync(Runnable closeWork) {
        CountDownLatch finished = new CountDownLatch(1);
        synchronized (lifecycle) {
            activeOperations++;
            pendingCloses++;
        }
        Thread closer = new Thread(() -> {
            try {
                closeWork.run();
            } finally {
                closeFinished(finished);
            }
        }, "starrocks-cdc-adbc-close");
        closer.setDaemon(true);
        try {
            closer.start();
        } catch (RuntimeException e) {
            try {
                closeWork.run();
            } finally {
                closeFinished(finished);
            }
        }
        return finished;
    }

    private void closeFinished(CountDownLatch finished) {
        synchronized (lifecycle) {
            pendingCloses--;
            activeOperations--;
        }
        finished.countDown();
        releaseResourcesIfIdle();
    }

    private static final class ReadScope implements AutoCloseable {
        private AdbcStatement statement;
        private AdbcStatement.QueryResult result;

        @Override
        public void close() {
            closeQuietly(result);
            closeQuietly(statement);
        }
    }

    private CountDownLatch closeReadScopeAsync(ReadScope scope, AdbcConnection active) {
        return closeAsync(() -> {
            try {
                scope.close();
            } finally {
                releaseReadConnection(active);
            }
        });
    }

    private void readChanges(String sql, List<ColumnMeta> cols, CdcClient.ChangeRowConsumer consumer,
                             String db, String table, long base, long head)
            throws SQLException {
        CdcReadTimings timings = CdcReadTimings.forRead(readTimings);
        AdbcConnection active = acquireReadConnection();
        ReadScope scope = new ReadScope();
        Runnable cleanup = () -> closeReadScopeAsync(scope, active);
        TimedCall<?> pending = null;
        boolean complete = false;
        try {
            scope.statement = active.createStatement();
            scope.statement.setSqlQuery(sql);
            long queryStart = timings == null ? 0 : System.nanoTime();
            TimedCall<AdbcStatement.QueryResult> queryCall = new TimedCall<>(scope.statement::executeQuery,
                    config.readTimeoutMs(), "ADBC changes query", StarRocksAdbcClient::closeQuietly,
                    cleanup);
            pending = queryCall;
            scope.result = queryCall.execute();
            pending = null;
            if (timings != null) {
                timings.queryNanos += System.nanoTime() - queryStart;
            }
            ArrowReader reader = scope.result.getReader();
            long rows = 0;
            while (true) {
                long start = timings == null ? 0 : System.nanoTime();
                TimedCall<Boolean> batchCall = new TimedCall<>(reader::loadNextBatch,
                        config.readTimeoutMs(), "ADBC changes batch", value -> { }, cleanup);
                pending = batchCall;
                boolean loaded = batchCall.execute();
                pending = null;
                if (timings != null) {
                    timings.advanceNanos += System.nanoTime() - start;
                }
                if (!loaded) {
                    break;
                }
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                int count = root.getRowCount();
                List<FieldVector> vectors = root.getFieldVectors();
                validateColumns(root, cols.size() + 2);
                for (int rowIndex = 0; rowIndex < count; rowIndex++) {
                    if ((rowIndex & 255) == 0 && closed.get()) {
                        throw new SQLException("ADBC reader is closed");
                    }
                    start = timings == null ? 0 : System.nanoTime();
                    Object[] row = valueReader.readRow(vectors, rowIndex, cols);
                    if (timings != null) {
                        timings.decodeNanos += System.nanoTime() - start;
                    }
                    int changeType = ((Number) vectors.get(cols.size()).getObject(rowIndex)).intValue();
                    long rowVersion = ((Number) vectors.get(cols.size() + 1).getObject(rowIndex)).longValue();
                    consumer.accept(row, changeType, rowVersion);
                    rows++;
                }
            }
            if (readTimings) {
                LOG.debug("CDC read changes table={}.{} base={} head={} rows={} query_ms={} next_ms={} decode_ms={}",
                        db, table, base, head, rows, timings.queryNanos / 1e6,
                        timings.advanceNanos / 1e6, timings.decodeNanos / 1e6);
            }
            complete = true;
        } catch (Exception e) {
            throw readFailure(e);
        } finally {
            if (pending == null || !pending.wasAbandoned()) {
                CountDownLatch closedScope = closeReadScopeAsync(scope, active);
                try {
                    if (!closedScope.await(config.readTimeoutMs(), TimeUnit.MILLISECONDS)) {
                        if (complete) {
                            throw new SQLException("ADBC changes cleanup timed out after "
                                    + config.readTimeoutMs() + " ms");
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    if (complete) {
                        throw new SQLException("Interrupted while closing ADBC changes reader", e);
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            TimedCall<?>[] attempts;
            AdbcConnection shared;
            synchronized (lifecycle) {
                readWorkers.shutdownNow();
                attempts = timedCalls.toArray(new TimedCall<?>[0]);
                shared = detachReadConnectionLocked();
            }
            for (TimedCall<?> attempt : attempts) {
                attempt.abandon();
            }
            Snapshot[] active;
            synchronized (snapshots) {
                active = snapshots.toArray(new Snapshot[0]);
            }
            for (Snapshot snapshot : active) {
                snapshot.close();
            }
            if (shared != null) {
                closeConnectionAsync(shared);
            }
            synchronized (lifecycle) {
                closePrepared = true;
            }
            releaseResourcesIfIdle();
            Runnable closeJdbc = () -> {
                try {
                    jdbc.close();
                } catch (RuntimeException e) {
                    LOG.warn("Could not close ADBC metadata JDBC connection", e);
                }
            };
            Thread closer = new Thread(closeJdbc, "starrocks-cdc-adbc-jdbc-close");
            closer.setDaemon(true);
            try {
                closer.start();
            } catch (RuntimeException e) {
                closeJdbc.run();
            }
        }
    }

    private void releaseResourcesIfIdle() {
        List<AdbcDatabase> releasedDatabases;
        BufferAllocator releasedAllocator;
        synchronized (lifecycle) {
            if (!closePrepared || resourcesReleased || activeOperations != 0 || !timedCalls.isEmpty()) {
                return;
            }
            resourcesReleased = true;
            releasedDatabases = new ArrayList<>(databases.values());
            databases.clear();
            releasedAllocator = allocator;
            database = null;
            allocator = null;
        }
        Runnable cleanup = () -> {
            for (AdbcDatabase releasedDatabase : releasedDatabases) {
                closeQuietly(releasedDatabase);
            }
            closeQuietly(releasedAllocator);
        };
        Thread closer = new Thread(cleanup, "starrocks-cdc-adbc-database-close");
        closer.setDaemon(true);
        try {
            closer.start();
        } catch (RuntimeException e) {
            cleanup.run();
        }
    }

    private static void closeQuietly(AutoCloseable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception e) {
                LOG.debug("Could not close ADBC resource", e);
            }
        }
    }

    static SQLException readFailure(Exception e) {
        // FlightStream.next() can throw a runtime transport error after executeQuery() succeeds.
        // Keep value/schema conversion errors fatal, but route Flight failures through poll retries.
        if (e instanceof RuntimeException && !(e instanceof FlightRuntimeException)) {
            throw (RuntimeException) e;
        }
        return sqlException(e);
    }

    private static SQLException sqlException(Exception e) {
        if (e instanceof SQLException) {
            return (SQLException) e;
        }
        if (e instanceof AdbcException) {
            AdbcException adbc = (AdbcException) e;
            return new SQLException(adbc.getMessage(), adbc.getSqlState(), adbc.getVendorCode(), adbc);
        }
        return new SQLException("ADBC read failed: " + e.getMessage(), e);
    }

}
