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

/** Per-read counters shared by JDBC and ADBC; the benchmark scopes one instance per round. */
final class CdcReadTimings {
    private static final ThreadLocal<CdcReadTimings> BENCHMARK = new ThreadLocal<>();

    long queryNanos;
    long advanceNanos;
    long decodeNanos;

    static CdcReadTimings beginBenchmark() {
        CdcReadTimings timings = new CdcReadTimings();
        BENCHMARK.set(timings);
        return timings;
    }

    static void endBenchmark() {
        BENCHMARK.remove();
    }

    static CdcReadTimings forRead(boolean onlineEnabled) {
        CdcReadTimings timings = BENCHMARK.get();
        return timings != null ? timings : onlineEnabled ? new CdcReadTimings() : null;
    }
}
