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

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.kafka.connect.errors.DataException;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Reads Arrow vectors directly, then uses the shared Arrow conversion for nested values. */
final class ArrowAdbcValueReader extends ArrowValueReader {

    Object[] readRow(List<FieldVector> vectors, int rowIndex, List<ColumnMeta> cols) {
        Object[] row = new Object[cols.size()];
        for (int i = 0; i < row.length; i++) {
            row[i] = readValue(vectors.get(i), rowIndex, cols.get(i).type);
        }
        return row;
    }

    Object readValue(FieldVector vector, int row, ColumnType type) {
        if (vector.isNull(row)) {
            return null;
        }
        switch (type.kind) {
            case BIGINT:
                return vector instanceof BigIntVector ? ((BigIntVector) vector).get(row)
                        : leaf(type, vector.getObject(row));
            case DATE:
                if (vector instanceof DateDayVector) {
                    return ArrowValueReader.dateOfEpochDays(((DateDayVector) vector).get(row));
                }
                throw unsupported(type, vector);
            case DATETIME:
                if (vector instanceof TimeStampMicroVector || vector instanceof TimeStampMicroTZVector) {
                    return ArrowValueReader.dateTimeOfEpochMicros(((TimeStampVector) vector).get(row));
                }
                throw unsupported(type, vector);
            case STRING:
            case JSON:
            case OPAQUE:
                if (vector instanceof VarCharVector) {
                    return new String(((VarCharVector) vector).get(row), StandardCharsets.UTF_8);
                }
                return textValue(type, vector.getObject(row));
            case MAP:
                if (vector instanceof MapVector) {
                    return readMap((MapVector) vector, row, type);
                }
                throw unsupported(type, vector);
            case ARRAY:
            case STRUCT:
                return readNested(type, vector.getObject(row));
            default:
                return leaf(type, vector.getObject(row));
        }
    }

    /** Avoids MapVector.getObject() creating one map per entry. */
    Map<String, Object> readMap(MapVector vector, int row, ColumnType type) {
        StructVector entries = (StructVector) vector.getDataVector();
        FieldVector keys = entries.getChild(MapVector.KEY_NAME);
        FieldVector mapValues = entries.getChild(MapVector.VALUE_NAME);
        Map<String, Object> out = new LinkedHashMap<>();
        for (int i = vector.getElementStartIndex(row); i < vector.getElementEndIndex(row); i++) {
            out.put(keyText(type.key, keys.getObject(i)), readNested(type.value, mapValues.getObject(i)));
        }
        return out;
    }

    private static DataException unsupported(ColumnType type, FieldVector vector) {
        return new DataException("unsupported Arrow vector for " + type + ": " + vector.getClass().getName());
    }
}
