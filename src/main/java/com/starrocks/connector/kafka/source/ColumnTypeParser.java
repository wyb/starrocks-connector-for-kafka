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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Parses {@code information_schema.columns.COLUMN_TYPE} into a {@link ColumnType}.
 *
 * <p>The string is FE's {@code Type.toSql()}, a display form rather than a contract, so this
 * parser accepts exactly the spellings FE produces and refuses everything else with
 * {@link Optional#empty()}; the caller then falls back to carrying the column as text. What FE
 * emits inside a nested type ({@code ScalarType.toSql}): {@code boolean}, {@code tinyint(4)},
 * {@code int(11)}, {@code bigint(20)}, {@code largeint(40)}, {@code decimal(18, 2)},
 * {@code varchar(10)}, {@code varbinary(16)}, {@code date}, {@code datetime}, {@code json} --
 * except that {@code ArrayType.toSql} prints a decimal item through {@code toString()} instead,
 * as {@code DECIMAL64(18,2)}. Struct fields are {@code `name` type} joined by {@code ", "}, with
 * embedded backticks doubled; a field comment is appended unescaped as {@code COMMENT '...'},
 * and nesting past 15 levels prints {@code ...}. Both of those end here as a refusal.
 */
final class ColumnTypeParser {

    private final String text;
    private int pos;

    private ColumnTypeParser(String text) {
        this.text = text;
    }

    static Optional<ColumnType> parse(String columnType) {
        if (columnType == null || columnType.trim().isEmpty()) {
            return Optional.empty();
        }
        ColumnTypeParser p = new ColumnTypeParser(columnType.trim());
        try {
            ColumnType t = p.type();
            p.skipSpaces();
            return p.pos == p.text.length() ? Optional.of(t) : Optional.empty();
        } catch (RuntimeException e) {
            // Any surprise in the string -- a COMMENT, a "...", a type this connector never met.
            return Optional.empty();
        }
    }

    private ColumnType type() {
        skipSpaces();
        String name = ident().toLowerCase(Locale.ROOT);
        switch (name) {
            case "array": {
                expect('<');
                ColumnType element = type();
                expect('>');
                return ColumnType.array(element);
            }
            case "map": {
                expect('<');
                ColumnType key = type();
                expect(',');
                ColumnType value = type();
                expect('>');
                return ColumnType.map(key, value);
            }
            case "struct": {
                expect('<');
                List<ColumnType.Field> fields = new ArrayList<>();
                do {
                    skipSpaces();
                    String fieldName = backquoted();
                    fields.add(new ColumnType.Field(fieldName, type()));
                    skipSpaces();
                } while (consume(','));
                expect('>');
                return ColumnType.struct(fields);
            }
            default:
                return scalar(name, params());
        }
    }

    private static ColumnType scalar(String name, int[] params) {
        switch (name) {
            case "boolean":
                return ColumnType.scalar(ColumnType.Kind.BOOLEAN);
            case "tinyint":
                return ColumnType.scalar(ColumnType.Kind.TINYINT);
            case "smallint":
                return ColumnType.scalar(ColumnType.Kind.SMALLINT);
            case "int":
                return ColumnType.scalar(ColumnType.Kind.INT);
            case "bigint":
                return ColumnType.scalar(ColumnType.Kind.BIGINT);
            case "largeint":
                return ColumnType.scalar(ColumnType.Kind.LARGEINT);
            case "float":
                return ColumnType.scalar(ColumnType.Kind.FLOAT);
            case "double":
                return ColumnType.scalar(ColumnType.Kind.DOUBLE);
            case "char":
            case "varchar":
                return ColumnType.scalar(ColumnType.Kind.STRING);
            case "varbinary":
                return ColumnType.scalar(ColumnType.Kind.BYTES);
            case "date":
                return ColumnType.scalar(ColumnType.Kind.DATE);
            case "datetime":
                return ColumnType.scalar(ColumnType.Kind.DATETIME);
            case "json":
                return ColumnType.scalar(ColumnType.Kind.JSON);
            default:
                break;
        }
        // decimal, decimalv2, decimal32/64/128/256: precision and scale are mandatory here, a
        // bare "decimal" is a wildcard no column can carry.
        if (name.startsWith("decimal")) {
            if (params.length != 2) {
                throw new IllegalArgumentException("decimal without (precision, scale): " + name);
            }
            return ColumnType.decimal(params[1]);
        }
        // hll, bitmap, percentile, variant, function, time, "...": not something a record can carry.
        throw new IllegalArgumentException("unsupported nested type: " + name);
    }

    private String ident() {
        int start = pos;
        while (pos < text.length() && (Character.isLetterOrDigit(text.charAt(pos)) || text.charAt(pos) == '_')) {
            pos++;
        }
        if (start == pos) {
            throw new IllegalArgumentException("type name expected at " + pos);
        }
        return text.substring(start, pos);
    }

    /** {@code (n)} or {@code (p, s)} if present; the numbers of the types that carry them. */
    private int[] params() {
        skipSpaces();
        if (!consume('(')) {
            return new int[0];
        }
        List<Integer> nums = new ArrayList<>();
        do {
            skipSpaces();
            int start = pos;
            while (pos < text.length() && Character.isDigit(text.charAt(pos))) {
                pos++;
            }
            nums.add(Integer.parseInt(text.substring(start, pos)));
            skipSpaces();
        } while (consume(','));
        expect(')');
        int[] out = new int[nums.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = nums.get(i);
        }
        return out;
    }

    /** A backquoted struct field name; a doubled backquote is one literal backquote. */
    private String backquoted() {
        expect('`');
        StringBuilder sb = new StringBuilder();
        while (true) {
            if (pos >= text.length()) {
                throw new IllegalArgumentException("unterminated field name");
            }
            char c = text.charAt(pos++);
            if (c == '`') {
                if (pos < text.length() && text.charAt(pos) == '`') {
                    sb.append('`');
                    pos++;
                    continue;
                }
                return sb.toString();
            }
            sb.append(c);
        }
    }

    private void expect(char c) {
        skipSpaces();
        if (!consume(c)) {
            throw new IllegalArgumentException("'" + c + "' expected at " + pos + " in " + text);
        }
    }

    private boolean consume(char c) {
        if (pos < text.length() && text.charAt(pos) == c) {
            pos++;
            return true;
        }
        return false;
    }

    private void skipSpaces() {
        while (pos < text.length() && text.charAt(pos) == ' ') {
            pos++;
        }
    }
}
