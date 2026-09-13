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

import java.sql.SQLException;
import java.util.Locale;

/**
 * A CHANGES read failed because the version range is no longer replayable, as opposed to
 * transiently. Callers use this to decide whether to fall back to a fresh snapshot.
 */
public class NonTrackableException extends Exception {

    public NonTrackableException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Wraps the exception when it is a non-trackable failure, else returns null so the caller
     * propagates it unchanged.
     *
     * <p>Five spellings from five places: {@code "CDC-ERROR-"} from the BE at execution time,
     * {@code "bookmark" + "not found"} from a released or expired base, {@code "not trackable"} from
     * FE planning, and {@code "no longer queryable"} / {@code "bookmark" + "no longer exists"} from
     * bookmark resolution. Matching only the first left {@code policy=resnapshot} inert.
     */
    public static NonTrackableException classify(SQLException e) {
        String message = e.getMessage();
        if (message == null) {
            return null;
        }
        String lower = message.toLowerCase(Locale.ROOT);
        boolean isCdcError = lower.contains("cdc-error-");
        boolean isStaleBookmark = lower.contains("bookmark") && lower.contains("not found");
        boolean isPlanningTimeNotTrackable = lower.contains("not trackable");
        boolean isUnresolvableBookmark = lower.contains("no longer queryable")
                || (lower.contains("bookmark") && lower.contains("no longer exists"));
        if (isCdcError || isStaleBookmark || isPlanningTimeNotTrackable || isUnresolvableBookmark) {
            return new NonTrackableException(message, e);
        }
        return null;
    }
}
