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

package com.starrocks.connector.kafka.common;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Guards the build wiring, not the logic: {@link Version} only reports the right thing if
 * {@code starrocks-connector.properties} is both on the classpath and resource-filtered by Maven.
 * Delete the {@code <resources><filtering>} block from the POM and these fail -- which is the
 * point, since the symptom otherwise is every connector quietly reporting "unknown" (or, worse, the
 * literal {@code ${project.version}}) over Connect's REST API.
 */
public class VersionTest {

    @Test
    public void testVersionResourceIsPresentAndFiltered() {
        String version = Version.get();
        assertFalse("version resource missing or unfiltered -- check <resources> in pom.xml",
                "unknown".equals(version));
        assertFalse("Maven property was not substituted: " + version, version.contains("${"));
    }

    @Test
    public void testVersionLooksLikeAVersionNumber() {
        assertTrue("not a version-shaped string: " + Version.get(),
                Version.get().matches("\\d+\\.\\d+(\\.\\d+)?(-.+)?"));
    }

    /** Loaded once into a static; repeated reads must not diverge. */
    @Test
    public void testValueIsStable() {
        assertTrue(Version.get() == Version.get());
    }
}
