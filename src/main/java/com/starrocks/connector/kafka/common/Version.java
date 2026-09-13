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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * The version every connector and task here reports, read from a Maven-filtered resource so the
 * POM is the only place it is written; hand-maintained constants had drifted to three answers.
 */
public final class Version {

    /** Reported when the resource is missing or was never filtered. */
    private static final String UNKNOWN = "unknown";

    private static final String RESOURCE = "/starrocks-connector.properties";

    private static final String VERSION = load();

    private Version() {
    }

    public static String get() {
        return VERSION;
    }

    private static String load() {
        try (InputStream in = Version.class.getResourceAsStream(RESOURCE)) {
            if (in == null) {
                return UNKNOWN;
            }
            Properties props = new Properties();
            props.load(in);
            String value = props.getProperty("version");
            if (value == null) {
                return UNKNOWN;
            }
            String trimmed = value.trim();
            // An unsubstituted "${project.version}" means filtering did not run; saying so beats
            // reporting the placeholder.
            if (trimmed.isEmpty() || trimmed.contains("${")) {
                return UNKNOWN;
            }
            return trimmed;
        } catch (IOException e) {
            return UNKNOWN;
        }
    }
}
