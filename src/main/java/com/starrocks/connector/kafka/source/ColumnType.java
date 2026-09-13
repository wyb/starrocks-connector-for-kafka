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

import io.debezium.data.Json;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * A StarRocks column type with its nesting, as declared. Built by {@link ColumnTypeParser} from
 * {@code information_schema.columns.COLUMN_TYPE}; the value readers walk it to know what a nested
 * element is, and {@link #toConnectSchema} turns it into the record schema.
 *
 * <p>Kept separate from the Connect schema because the two disagree on purpose: a map key is
 * declared {@code int} but travels as STRING (JsonConverter renders a non-string-keyed map as an
 * array of pairs), and a reader still needs the declared type to parse the transport's value.
 */
final class ColumnType {

    enum Kind {
        BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL,
        STRING, BYTES, DATE, DATETIME, JSON,
        ARRAY, MAP, STRUCT
    }

    static final class Field {
        final String name;
        final ColumnType type;

        Field(String name, ColumnType type) {
            this.name = name;
            this.type = type;
        }
    }

    final Kind kind;
    /** DECIMAL only; LARGEINT is a DECIMAL of scale 0 on the wire but keeps its own kind here. */
    final int scale;
    final ColumnType element;   // ARRAY
    final ColumnType key;       // MAP, as declared
    final ColumnType value;     // MAP
    final List<Field> fields;   // STRUCT, declared order

    private ColumnType(Kind kind, int scale, ColumnType element, ColumnType key, ColumnType value,
                       List<Field> fields) {
        this.kind = kind;
        this.scale = scale;
        this.element = element;
        this.key = key;
        this.value = value;
        this.fields = fields;
    }

    static ColumnType scalar(Kind kind) {
        return new ColumnType(kind, 0, null, null, null, Collections.emptyList());
    }

    static ColumnType decimal(int scale) {
        return new ColumnType(Kind.DECIMAL, scale, null, null, null, Collections.emptyList());
    }

    static ColumnType array(ColumnType element) {
        return new ColumnType(Kind.ARRAY, 0, element, null, null, Collections.emptyList());
    }

    static ColumnType map(ColumnType key, ColumnType value) {
        return new ColumnType(Kind.MAP, 0, null, key, value, Collections.emptyList());
    }

    static ColumnType struct(List<Field> fields) {
        return new ColumnType(Kind.STRUCT, 0, null, null, null,
                Collections.unmodifiableList(fields));
    }

    /**
     * The Connect schema for a value of this type. Nested elements, map values and struct fields
     * are always optional -- StarRocks lets any of them be NULL -- and only the column itself takes
     * the declared nullability. {@code name} seeds the names of nested structs, which the Avro
     * converter requires to be unique.
     */
    Schema toConnectSchema(String name, boolean optional) {
        SchemaBuilder b;
        switch (kind) {
            case BOOLEAN:
                b = SchemaBuilder.bool();
                break;
            case TINYINT:
                b = SchemaBuilder.int8();
                break;
            case SMALLINT:
                b = SchemaBuilder.int16();
                break;
            case INT:
                b = SchemaBuilder.int32();
                break;
            case BIGINT:
                b = SchemaBuilder.int64();
                break;
            case FLOAT:
                b = SchemaBuilder.float32();
                break;
            case DOUBLE:
                b = SchemaBuilder.float64();
                break;
            case LARGEINT:
                b = Decimal.builder(0);
                break;
            case DECIMAL:
                b = Decimal.builder(scale);
                break;
            case STRING:
                b = SchemaBuilder.string();
                break;
            case BYTES:
                b = SchemaBuilder.bytes();
                break;
            case DATE:
                b = SchemaBuilder.string().name(TemporalText.DATE_LOGICAL_NAME).version(1);
                break;
            case DATETIME:
                b = SchemaBuilder.string().name(TemporalText.DATETIME_LOGICAL_NAME).version(1);
                break;
            case JSON:
                b = SchemaBuilder.string().name(Json.LOGICAL_NAME).version(1);
                break;
            case ARRAY:
                b = SchemaBuilder.array(element.toConnectSchema(name + ".element", true));
                break;
            case MAP:
                b = SchemaBuilder.map(Schema.STRING_SCHEMA, value.toConnectSchema(name + ".value", true));
                break;
            case STRUCT:
                b = SchemaBuilder.struct().name(name);
                for (Field f : fields) {
                    b.field(f.name, f.type.toConnectSchema(name + "." + f.name, true));
                }
                break;
            default:
                throw new IllegalStateException("unmapped kind " + kind);
        }
        return optional ? b.optional().build() : b.build();
    }

    /**
     * Turns a reader's neutral value -- scalars already coerced, {@code List} for arrays,
     * {@code Map<String, Object>} for both maps and structs -- into what the Connect schema wants:
     * structs become {@link Struct}s, recursively. {@code schema} must be this type's own
     * {@link #toConnectSchema} output.
     */
    Object toConnectValue(Schema schema, Object neutral) {
        if (neutral == null) {
            return null;
        }
        switch (kind) {
            case ARRAY: {
                List<Object> out = new ArrayList<>();
                for (Object item : (List<?>) neutral) {
                    out.add(element.toConnectValue(schema.valueSchema(), item));
                }
                return out;
            }
            case MAP: {
                Map<String, Object> out = new LinkedHashMap<>();
                for (Map.Entry<?, ?> e : ((Map<?, ?>) neutral).entrySet()) {
                    out.put((String) e.getKey(), value.toConnectValue(schema.valueSchema(), e.getValue()));
                }
                return out;
            }
            case STRUCT: {
                Map<?, ?> in = (Map<?, ?>) neutral;
                Struct out = new Struct(schema);
                for (Field f : fields) {
                    Object v = f.type.toConnectValue(schema.field(f.name).schema(), in.get(f.name));
                    if (v != null) {
                        out.put(f.name, v);
                    }
                }
                return out;
            }
            default:
                return neutral;
        }
    }

    /** A normalized spelling, for messages and tests: {@code map<int,array<decimal(2)>>}. */
    @Override
    public String toString() {
        switch (kind) {
            case DECIMAL:
                return "decimal(" + scale + ")";
            case ARRAY:
                return "array<" + element + ">";
            case MAP:
                return "map<" + key + "," + value + ">";
            case STRUCT:
                StringJoiner j = new StringJoiner(",", "struct<", ">");
                for (Field f : fields) {
                    j.add(f.name + ":" + f.type);
                }
                return j.toString();
            default:
                return kind.name().toLowerCase();
        }
    }
}
