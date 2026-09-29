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
 * The MySQL protocol's one quirk: a nested column arrives as the text BE's
 * {@code put_mysql_row_buffer} renders, JSON-like but not JSON. A numeric map key is unquoted
 * ({@code {1:2}}), a nested JSON value is wrapped in <em>single</em> quotes, a BOOLEAN is {@code 1}
 * or {@code 0}, strings escape only {@code "} and {@code \} and may contain raw control characters,
 * VARBINARY is hex or base64 per the session's {@code binary_encoding_format}, and fmt prints
 * non-finite floats as {@code inf}/{@code nan}.
 *
 * <p>The text is first cut into a raw tree without knowing any types -- lists, maps keyed by
 * their key token, string tokens, bare {@code null} -- and the shared recursion then reads that
 * tree by the declared {@link ColumnType}, exactly as the Arrow reader reads the driver's objects.
 * Which token is a number, a boolean or a date is the type's call, not the text's.
 */
final class MysqlValueReader extends ValueReader {

    enum BinaryEncoding { HEX, BASE64 }

    /** What the connector pins the session to ({@link FeConnection#SESSION_SETUP_SQL}) and reads with. */
    static final BinaryEncoding SESSION_BINARY_ENCODING = BinaryEncoding.HEX;

    private final BinaryEncoding binary;

    MysqlValueReader() {
        this(SESSION_BINARY_ENCODING);
    }

    MysqlValueReader(BinaryEncoding binary) {
        this.binary = binary;
    }

    @Override
    protected Object nested(ColumnType type, Object raw) {
        String text = as(String.class, raw, type);
        Object tree = new Scanner(text).parse();
        try {
            return readNested(type, tree);
        } catch (NumberFormatException e) {
            throw new DataException("not a number for " + type + " (" + e.getMessage() + ") in: " + text, e);
        } catch (DataException e) {
            throw new DataException(e.getMessage() + " in: " + text, e);
        }
    }

    @Override
    protected Object leaf(ColumnType type, Object raw) {
        String token = as(String.class, raw, type);
        switch (type.kind) {
            case STRING:
            case JSON:
            case DATE:
            case DATETIME:
                // Dates arrive already in the text the base reader prints.
                return token;
            case BYTES:
                return binary == BinaryEncoding.BASE64 ? Base64.getDecoder().decode(token) : hex(token);
            case BOOLEAN:
                if ("1".equals(token) || "true".equalsIgnoreCase(token)) {
                    return Boolean.TRUE;
                }
                if ("0".equals(token) || "false".equalsIgnoreCase(token)) {
                    return Boolean.FALSE;
                }
                throw new DataException("not a boolean: " + token);
            case TINYINT:
                return Byte.parseByte(token);
            case SMALLINT:
                return Short.parseShort(token);
            case INT:
                return Integer.parseInt(token);
            case BIGINT:
                return Long.parseLong(token);
            case LARGEINT:
            case DECIMAL:
                return new BigDecimal(token).setScale(type.scale);
            case FLOAT:
                return (float) floating(token);
            case DOUBLE:
                return floating(token);
            default:
                throw new DataException("no text reader for " + type);
        }
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

    /**
     * BE's text to a raw tree: {@code [...]} to a List, {@code {k:v,...}} to a LinkedHashMap keyed by
     * the key token, a quoted value (either quote) to its unescaped String, a bare {@code null} to
     * null, any other bare token to its String. No types involved.
     */
    private static final class Scanner {
        private final String text;
        private int pos;

        Scanner(String text) {
            this.text = text;
        }

        Object parse() {
            Object v = value();
            skipSpaces();
            if (pos != text.length()) {
                throw error("trailing characters");
            }
            return v;
        }

        private Object value() {
            skipSpaces();
            if (pos >= text.length()) {
                throw error("value expected");
            }
            char c = text.charAt(pos);
            if (c == '[') {
                pos++;
                List<Object> out = new ArrayList<>();
                if (!consume(']')) {
                    do {
                        out.add(value());
                    } while (consume(','));
                    expect(']');
                }
                return out;
            }
            if (c == '{') {
                pos++;
                Map<Object, Object> out = new LinkedHashMap<>();
                if (!consume('}')) {
                    do {
                        Object key = value();
                        expect(':');
                        out.put(key, value());
                    } while (consume(','));
                    expect('}');
                }
                return out;
            }
            if (c == '"' || c == '\'') {
                return quoted(c);
            }
            String lit = literal();
            return "null".equals(lit) ? null : lit;
        }

        /** A value between {@code quote}s; only the quote itself and a backslash are escaped. */
        private String quoted(char quote) {
            pos++;
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

        private void expect(char c) {
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
}
