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
 * Signals that a CHANGES read failed because the requested version range is no longer
 * trackable by the BE (the window has aged out, or the bookmark pinning it has expired/been
 * released), as opposed to a transient or unrelated failure. Callers use this to decide when to
 * fall back to a fresh snapshot instead of retrying the same range.
 */
public class NonTrackableException extends Exception {

    public NonTrackableException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Classifies a SQLException raised while executing a CHANGES read.
     *
     * <p>Returns a wrapping {@link NonTrackableException} when the message contains
     * {@code "CDC-ERROR-"}, or contains both {@code "bookmark"} and {@code "not found"}
     * (case-insensitive in both checks); returns {@code null} for every other message, including
     * a null message, telling the caller to propagate the original SQLException unchanged.
     */
    public static NonTrackableException classify(SQLException e) {
        String message = e.getMessage();
        if (message == null) {
            return null;
        }
        String lower = message.toLowerCase(Locale.ROOT);
        boolean isCdcError = lower.contains("cdc-error-");
        boolean isStaleBookmark = lower.contains("bookmark") && lower.contains("not found");
        if (isCdcError || isStaleBookmark) {
            return new NonTrackableException(message, e);
        }
        return null;
    }
}
