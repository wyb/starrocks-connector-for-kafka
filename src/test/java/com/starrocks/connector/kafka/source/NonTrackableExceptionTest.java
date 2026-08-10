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

import java.sql.SQLException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * A gap here does not surface as a wrong answer but as {@code source.nontrackable.policy} silently
 * never firing -- which is what happened when the matcher missed FE planning-time messages.
 */
public class NonTrackableExceptionTest {

    /** BE-side: the CHANGES scan itself reports the window is not replayable. */
    @Test
    public void testBackendCdcErrorIsClassified() {
        assertNotNull(NonTrackableException.classify(new SQLException(
                "CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CHANGES window on tablet 1 spans version 3 ...")));
    }

    /** A released or expired base bookmark. */
    @Test
    public void testMissingBookmarkIsClassified() {
        assertNotNull(NonTrackableException.classify(new SQLException("Bookmark 11952 not found")));
    }

    /** FE planning messages contain none of the other markers, only "not trackable". */
    @Test
    public void testFrontendPlanningMessagesAreClassified() {
        assertNotNull(NonTrackableException.classify(new SQLException(
                "CHANGES from bookmark 5 to 9 on table 't' not trackable: physical partition 100 dropped")));
        assertNotNull(NonTrackableException.classify(new SQLException(
                "CHANGES on table 't' not trackable: partition p1 not present in the changeset")));
    }

    /** Over-matching is as bad: under resnapshot a misclassified blip re-reads the whole table. */
    @Test
    public void testUnrelatedFailuresAreNotClassified() {
        assertNull(NonTrackableException.classify(new SQLException("Connection refused")));
        assertNull(NonTrackableException.classify(new SQLException("Unknown database 'nope'")));
        assertNull(NonTrackableException.classify(new SQLException((String) null)));
    }
}
