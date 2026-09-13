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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Turns the text the MySQL protocol carries for a nested column into the neutral value
 * {@link ColumnType#toConnectValue} assembles from.
 *
 * <p>The text is BE's {@code put_mysql_row_buffer} output, JSON-like but not JSON, so this is a
 * scanner driven by the declared {@link ColumnType} rather than a JSON parser: a map key of a
 * numeric type is unquoted ({@code {1:2}}), a nested JSON value is wrapped in <em>single</em>
 * quotes, a BOOLEAN is {@code 1} or {@code 0}, strings escape only {@code "} and {@code \} and may
 * contain raw control characters, and VARBINARY is hex or base64 per the session's
 * {@code binary_encoding_format}. Dates arrive as text already in the format {@link TemporalText}
 * produces and pass through.
 */
final class MysqlTextReader {

    enum BinaryEncoding { HEX, BASE64 }

    /** What the connector pins the session to ({@link FeConnection#SESSION_SETUP_SQL}) and reads with. */
    static final BinaryEncoding SESSION_BINARY_ENCODING = BinaryEncoding.HEX;

    private final String text;
    private final BinaryEncoding binary;
    private int pos;

    private MysqlTextReader(String text, BinaryEncoding binary) {
        this.text = text;
        this.binary = binary;
    }

    static Object read(ColumnType type, String text, BinaryEncoding binary) {
        if (text == null) {
            return null;
        }
        MysqlTextReader r = new MysqlTextReader(text, binary);
        Object v;
        try {
            v = r.value(type);
        } catch (NumberFormatException e) {
            throw r.error("not a number for " + type + " (" + e.getMessage() + ")");
        }
        r.skipSpaces();
        if (r.pos != text.length()) {
            throw r.error("trailing characters after the " + type + " value");
        }
        return v;
    }

    private Object value(ColumnType type) {
        skipSpaces();
        // A bare null is SQL NULL for every kind; a quoted "null" starts with the quote and is text.
        if (lookingAt("null")) {
            pos += 4;
            return null;
        }
        switch (type.kind) {
            case ARRAY: {
                expect('[');
                List<Object> out = new ArrayList<>();
                if (!consume(']')) {
                    do {
                        out.add(value(type.element));
                    } while (consume(','));
                    expect(']');
                }
                return out;
            }
            case MAP: {
                expect('{');
                Map<String, Object> out = new LinkedHashMap<>();
                if (!consume('}')) {
                    do {
                        Object key = value(type.key);
                        if (key == null) {
                            throw error("null map key");
                        }
                        expect(':');
                        out.put(String.valueOf(key), value(type.value));
                    } while (consume(','));
                    expect('}');
                }
                return out;
            }
            case STRUCT: {
                expect('{');
                Map<String, Object> byName = new LinkedHashMap<>();
                if (!consume('}')) {
                    do {
                        skipSpaces();
                        String name = quoted('"');
                        expect(':');
                        ColumnType fieldType = fieldType(type, name);
                        byName.put(name, value(fieldType));
                    } while (consume(','));
                    expect('}');
                }
                // Declared order, and a field the text omitted is null.
                Map<String, Object> out = new LinkedHashMap<>();
                for (ColumnType.Field f : type.fields) {
                    out.put(f.name, byName.get(f.name));
                }
                return out;
            }
            case STRING:
            case DATE:
            case DATETIME:
                return quoted('"');
            case JSON:
                return quoted('\'');
            case BYTES: {
                String encoded = quoted('"');
                return binary == BinaryEncoding.BASE64 ? Base64.getDecoder().decode(encoded) : hex(encoded);
            }
            case BOOLEAN: {
                String lit = literal();
                if ("1".equals(lit) || "true".equalsIgnoreCase(lit)) {
                    return Boolean.TRUE;
                }
                if ("0".equals(lit) || "false".equalsIgnoreCase(lit)) {
                    return Boolean.FALSE;
                }
                throw error("not a boolean: " + lit);
            }
            case TINYINT:
                return Byte.parseByte(literal());
            case SMALLINT:
                return Short.parseShort(literal());
            case INT:
                return Integer.parseInt(literal());
            case BIGINT:
                return Long.parseLong(literal());
            case LARGEINT:
                return new BigDecimal(literal()).setScale(0);
            case DECIMAL:
                return new BigDecimal(literal()).setScale(type.scale);
            case FLOAT:
                return (float) floating(literal());
            case DOUBLE:
                return floating(literal());
            default:
                throw error("no text reader for " + type);
        }
    }

    private static ColumnType fieldType(ColumnType struct, String name) {
        for (ColumnType.Field f : struct.fields) {
            if (f.name.equals(name)) {
                return f.type;
            }
        }
        throw new DataException("struct field '" + name + "' is not declared in " + struct);
    }

    /** fmt prints non-finite doubles as inf, -inf and nan; Java spells them differently. */
    private static double floating(String lit) {
        switch (lit.toLowerCase(Locale.ROOT)) {
            case "inf":
            case "infinity":
                return Double.POSITIVE_INFINITY;
            case "-inf":
            case "-infinity":
                return Double.NEGATIVE_INFINITY;
            case "nan":
            case "-nan":
                return Double.NaN;
            default:
                return Double.parseDouble(lit);
        }
    }

    private static byte[] hex(String s) {
        if (s.length() % 2 != 0) {
            throw new DataException("odd-length hex for VARBINARY: " + s);
        }
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(s.substring(2 * i, 2 * i + 2), 16);
        }
        return out;
    }

    /** A value between {@code quote}s; only the quote itself and a backslash are escaped. */
    private String quoted(char quote) {
        skipSpaces();
        if (!consume(quote)) {
            throw error("expected " + quote);
        }
        StringBuilder sb = new StringBuilder();
        while (pos < text.length()) {
            char c = text.charAt(pos++);
            if (c == '\\' && pos < text.length()) {
                sb.append(text.charAt(pos++));
            } else if (c == quote) {
                return sb.toString();
            } else {
                sb.append(c);
            }
        }
        throw error("unterminated " + quote + "-quoted value");
    }

    /** An unquoted token: everything up to the next delimiter. */
    private String literal() {
        skipSpaces();
        int start = pos;
        while (pos < text.length() && ",]}:".indexOf(text.charAt(pos)) < 0) {
            pos++;
        }
        String lit = text.substring(start, pos).trim();
        if (lit.isEmpty()) {
            throw error("value expected");
        }
        return lit;
    }

    private boolean lookingAt(String word) {
        return text.startsWith(word, pos)
                && (pos + word.length() == text.length() || ",]}:".indexOf(text.charAt(pos + word.length())) >= 0);
    }

    private void expect(char c) {
        skipSpaces();
        if (!consume(c)) {
            throw error("expected '" + c + "'");
        }
    }

    private boolean consume(char c) {
        skipSpaces();
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

    private DataException error(String what) {
        return new DataException(what + " at offset " + pos + " in: " + text);
    }
}
