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

package com.droppii.connector.kafka.schema;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Assert;
import org.junit.Test;

import com.droppii.connector.kafka.json.TemporalTypeFormats;

public class StarRocksTypeMapperTest {

    @Test
    public void mapsPrimitiveTypes() {
        Assert.assertEquals("TINYINT", StarRocksTypeMapper.mapType(Schema.INT8_SCHEMA));
        Assert.assertEquals("SMALLINT", StarRocksTypeMapper.mapType(Schema.INT16_SCHEMA));
        Assert.assertEquals("INT", StarRocksTypeMapper.mapType(Schema.INT32_SCHEMA));
        Assert.assertEquals("BIGINT", StarRocksTypeMapper.mapType(Schema.INT64_SCHEMA));
        Assert.assertEquals("FLOAT", StarRocksTypeMapper.mapType(Schema.FLOAT32_SCHEMA));
        Assert.assertEquals("DOUBLE", StarRocksTypeMapper.mapType(Schema.FLOAT64_SCHEMA));
        Assert.assertEquals("BOOLEAN", StarRocksTypeMapper.mapType(Schema.BOOLEAN_SCHEMA));
        Assert.assertEquals("STRING", StarRocksTypeMapper.mapType(Schema.STRING_SCHEMA));
        Assert.assertEquals("STRING", StarRocksTypeMapper.mapType(Schema.BYTES_SCHEMA));
    }

    @Test
    public void mapsDateLogicalType() {
        Assert.assertEquals("DATE", StarRocksTypeMapper.mapType(Date.SCHEMA));
    }

    @Test
    public void mapsTimestampLogicalType() {
        Assert.assertEquals("DATETIME", StarRocksTypeMapper.mapType(Timestamp.SCHEMA));
    }

    @Test
    public void mapsTimeLogicalTypeToString() {
        Assert.assertEquals("STRING", StarRocksTypeMapper.mapType(Time.SCHEMA));
    }

    @Test
    public void mapsDebeziumDateLogicalTypeToDate() {
        Schema schema = SchemaBuilder.int32().name(TemporalTypeFormats.DEBEZIUM_DATE).build();
        Assert.assertEquals("DATE", StarRocksTypeMapper.mapType(schema));
    }

    @Test
    public void mapsDebeziumTimestampVariantsToDatetime() {
        Assert.assertEquals("DATETIME", StarRocksTypeMapper.mapType(
                SchemaBuilder.int64().name(TemporalTypeFormats.DEBEZIUM_TIMESTAMP).build()));
        Assert.assertEquals("DATETIME", StarRocksTypeMapper.mapType(
                SchemaBuilder.int64().name(TemporalTypeFormats.DEBEZIUM_MICRO_TIMESTAMP).build()));
        Assert.assertEquals("DATETIME", StarRocksTypeMapper.mapType(
                SchemaBuilder.int64().name(TemporalTypeFormats.DEBEZIUM_NANO_TIMESTAMP).build()));
        Assert.assertEquals("DATETIME", StarRocksTypeMapper.mapType(
                SchemaBuilder.string().name(TemporalTypeFormats.DEBEZIUM_ZONED_TIMESTAMP).build()));
    }

    @Test
    public void mapsDebeziumTimeVariantsToString() {
        Assert.assertEquals("STRING", StarRocksTypeMapper.mapType(
                SchemaBuilder.int32().name(TemporalTypeFormats.DEBEZIUM_TIME).build()));
        Assert.assertEquals("STRING", StarRocksTypeMapper.mapType(
                SchemaBuilder.int64().name(TemporalTypeFormats.DEBEZIUM_MICRO_TIME).build()));
        Assert.assertEquals("STRING", StarRocksTypeMapper.mapType(
                SchemaBuilder.int64().name(TemporalTypeFormats.DEBEZIUM_NANO_TIME).build()));
    }

    @Test
    public void mapsDecimalLogicalTypeWithExplicitPrecision() {
        Schema schema = Decimal.builder(2).parameter("connect.decimal.precision", "10").build();
        Assert.assertEquals("DECIMAL(10,2)", StarRocksTypeMapper.mapType(schema));
    }

    @Test
    public void mapsDecimalLogicalTypeWithDefaultPrecision() {
        Schema schema = Decimal.schema(2);
        Assert.assertEquals("DECIMAL(38,2)", StarRocksTypeMapper.mapType(schema));
    }

    @Test
    public void mapsNestedTypesToJson() {
        Schema structSchema = SchemaBuilder.struct().field("a", Schema.STRING_SCHEMA).build();
        Schema arraySchema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
        Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();

        Assert.assertEquals("JSON", StarRocksTypeMapper.mapType(structSchema));
        Assert.assertEquals("JSON", StarRocksTypeMapper.mapType(arraySchema));
        Assert.assertEquals("JSON", StarRocksTypeMapper.mapType(mapSchema));
    }
}
