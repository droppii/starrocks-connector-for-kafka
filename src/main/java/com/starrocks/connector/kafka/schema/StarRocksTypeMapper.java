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

package com.starrocks.connector.kafka.schema;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

import com.starrocks.connector.kafka.json.TemporalTypeFormats;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

// Maps a Kafka Connect field Schema to the StarRocks DDL type used when a
// missing column is added via ALTER TABLE ADD COLUMN. Columns added this way
// are always nullable, so no NOT NULL / key-column cases are handled here.
public final class StarRocksTypeMapper {

    private static final String DECIMAL_PRECISION_PARAM = "connect.decimal.precision";
    private static final int DEFAULT_DECIMAL_PRECISION = 38;

    private static final Set<String> DATE_LOGICAL_NAMES = new HashSet<>(Arrays.asList(
            Date.LOGICAL_NAME,
            TemporalTypeFormats.DEBEZIUM_DATE));

    private static final Set<String> DATETIME_LOGICAL_NAMES = new HashSet<>(Arrays.asList(
            Timestamp.LOGICAL_NAME,
            TemporalTypeFormats.DEBEZIUM_TIMESTAMP,
            TemporalTypeFormats.DEBEZIUM_MICRO_TIMESTAMP,
            TemporalTypeFormats.DEBEZIUM_NANO_TIMESTAMP,
            TemporalTypeFormats.DEBEZIUM_ZONED_TIMESTAMP));

    private static final Set<String> TIME_LOGICAL_NAMES = new HashSet<>(Arrays.asList(
            Time.LOGICAL_NAME,
            TemporalTypeFormats.DEBEZIUM_TIME,
            TemporalTypeFormats.DEBEZIUM_MICRO_TIME,
            TemporalTypeFormats.DEBEZIUM_NANO_TIME));

    private StarRocksTypeMapper() {
    }

    public static String mapType(Schema schema) {
        String logicalName = schema.name();
        if (DATE_LOGICAL_NAMES.contains(logicalName)) {
            return "DATE";
        }
        if (DATETIME_LOGICAL_NAMES.contains(logicalName)) {
            return "DATETIME";
        }
        if (TIME_LOGICAL_NAMES.contains(logicalName)) {
            return "STRING";
        }
        if (Decimal.LOGICAL_NAME.equals(logicalName)) {
            return mapDecimal(schema);
        }
        switch (schema.type()) {
            case INT8:
                return "TINYINT";
            case INT16:
                return "SMALLINT";
            case INT32:
                return "INT";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "FLOAT";
            case FLOAT64:
                return "DOUBLE";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
            case BYTES:
                return "STRING";
            case STRUCT:
            case ARRAY:
            case MAP:
                return "JSON";
            default:
                throw new DataException("Unsupported schema type for schema evolution: " + schema.type());
        }
    }

    private static String mapDecimal(Schema schema) {
        int scale = Integer.parseInt(schema.parameters().getOrDefault(Decimal.SCALE_FIELD, "0"));
        String precisionParam = schema.parameters().get(DECIMAL_PRECISION_PARAM);
        int precision = precisionParam != null ? Integer.parseInt(precisionParam) : DEFAULT_DECIMAL_PRECISION;
        return "DECIMAL(" + precision + "," + scale + ")";
    }
}
