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

import org.apache.kafka.connect.errors.DataException;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;

/** Converts nested values returned by Arrow Flight JDBC; also supplies its timestamp correction. */
final class ArrowJdbcValueReader extends ArrowValueReader {

    @Override
    protected Object nested(ColumnType type, Object raw) {
        if (raw instanceof java.sql.Array) {
            try {
                Object elements = ((java.sql.Array) raw).getArray();
                raw = elements instanceof Object[] ? Arrays.asList((Object[]) elements) : elements;
            } catch (SQLException e) {
                throw new DataException("could not read the elements of " + type, e);
            }
        }
        return super.nested(type, raw);
    }

    /** The JDBC timestamp accessor shifts values into the JVM zone even with a UTC Calendar. */
    static Timestamp fromJvmWallClock(Timestamp shifted) {
        LocalDateTime digits = shifted.toLocalDateTime();
        return Timestamp.from(digits.toInstant(ZoneOffset.UTC));
    }
}
