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
 * The single source of truth for the version every connector and task in this artifact reports.
 *
 * <p>The value is read from {@code /starrocks-connector.properties}, which Maven filters at build
 * time so it carries {@code ${project.version}} verbatim from the POM. Hand-maintained version
 * constants drift: this class replaces one that had been left at {@code 1.0.3} while the POM said
 * {@code 1.0.5}, and a separate hard-coded {@code "1.0"} on the CDC source side. Connect surfaces
 * {@code version()} over the REST API and in startup logs, so operators use it to tell which build
 * is actually deployed -- three disagreeing answers made that impossible.
 */
public final class Version {

    /** Reported when the properties file is missing or was never filtered (e.g. running from raw sources). */
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
            // An unsubstituted "${project.version}" means resource filtering did not run. Reporting
            // that string would be worse than admitting we do not know.
            if (trimmed.isEmpty() || trimmed.contains("${")) {
                return UNKNOWN;
            }
            return trimmed;
        } catch (IOException e) {
            return UNKNOWN;
        }
    }
}
