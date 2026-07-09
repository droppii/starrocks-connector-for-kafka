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

package com.starrocks.connector.kafka.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.connector.kafka.StarRocksSinkTask;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

public class JsonConverterTest {
    @Before
    public void setUp() {
        PropertyConfigurator.configure("src/test/conf/log4j.properties");
    }

    private SchemaAndValue getSchemaAndValueFromJsonStr(String jsonStr) throws JsonProcessingException {
        JsonConverter jsonConverter = new JsonConverter();
        Map<String, Object> props = new HashMap<>();
        props.put("schemas.enable", (Object) false);
        jsonConverter.configure(props, false);
        JsonSerializer jsonSerializer = jsonConverter.getSerializer();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNodeSource = objectMapper.readTree(jsonStr);
        byte[] jsonBytes = jsonSerializer.serialize("", jsonNodeSource);
        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("", jsonBytes);
        return schemaAndValue;
    }

    @Test
    public void testConvertToJson() throws JsonProcessingException {
        String jsonStr = "{\"elements\":[{\"elName\":\"zll\",\"age\":1},{\"elName\":\"zll1\",\"age\":2}],\"name\":\"haha\",\"id\":1}";
        SchemaAndValue schemaAndValue = getSchemaAndValueFromJsonStr(jsonStr);
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        JsonNode jsonNodeDest = jsonConverter.convertToJson(schemaAndValue.schema(), schemaAndValue.value());
        System.out.println(jsonNodeDest.toString());
        Assert.assertEquals(jsonStr, jsonNodeDest.toString());
    }

    @Test
    public void serializesKafkaConnectDateAsStarRocksString() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        java.util.Date date = Date.toLogical(Date.SCHEMA, 0);
        JsonNode node = jsonConverter.convertToJson(Date.SCHEMA, date);
        Assert.assertEquals("\"1970-01-01\"", node.toString());
    }

    @Test
    public void serializesKafkaConnectTimestampAsStarRocksString() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        java.util.Date timestamp = Timestamp.toLogical(Timestamp.SCHEMA, 90061000L);
        JsonNode node = jsonConverter.convertToJson(Timestamp.SCHEMA, timestamp);
        Assert.assertEquals("\"1970-01-02 01:01:01.000000\"", node.toString());
    }

    @Test
    public void serializesKafkaConnectTimeAsStarRocksString() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        java.util.Date time = Time.toLogical(Time.SCHEMA, 3723000);
        JsonNode node = jsonConverter.convertToJson(Time.SCHEMA, time);
        Assert.assertEquals("\"01:02:03.000000\"", node.toString());
    }

    @Test
    public void serializesDebeziumDateAsStarRocksString() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        Schema schema = SchemaBuilder.int32().name(TemporalTypeFormats.DEBEZIUM_DATE).build();
        JsonNode node = jsonConverter.convertToJson(schema, -1);
        Assert.assertEquals("\"1969-12-31\"", node.toString());
    }

    @Test
    public void serializesDebeziumMicroTimestampAsStarRocksString() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        Schema schema = SchemaBuilder.int64().name(TemporalTypeFormats.DEBEZIUM_MICRO_TIMESTAMP).build();
        JsonNode node = jsonConverter.convertToJson(schema, 1_500_000L);
        Assert.assertEquals("\"1970-01-01 00:00:01.500000\"", node.toString());
    }

    @Test
    public void serializesDebeziumNanoTimestampTruncatedToMicros() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        Schema schema = SchemaBuilder.int64().name(TemporalTypeFormats.DEBEZIUM_NANO_TIMESTAMP).build();
        JsonNode node = jsonConverter.convertToJson(schema, 1_500_000_500L);
        Assert.assertEquals("\"1970-01-01 00:00:01.500000\"", node.toString());
    }

    @Test
    public void serializesDebeziumZonedTimestampNormalizedToUtc() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        Schema schema = SchemaBuilder.string().name(TemporalTypeFormats.DEBEZIUM_ZONED_TIMESTAMP).build();
        JsonNode node = jsonConverter.convertToJson(schema, "2024-01-01T13:15:30.123456+03:00");
        Assert.assertEquals("\"2024-01-01 10:15:30.123456\"", node.toString());
    }

    @Test
    public void serializesDebeziumTimeAsStarRocksString() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        Schema schema = SchemaBuilder.int32().name(TemporalTypeFormats.DEBEZIUM_TIME).build();
        JsonNode node = jsonConverter.convertToJson(schema, 3723000);
        Assert.assertEquals("\"01:02:03.000000\"", node.toString());
    }

    @Test
    public void serializesDebeziumMicroTimeAsStarRocksString() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        Schema schema = SchemaBuilder.int64().name(TemporalTypeFormats.DEBEZIUM_MICRO_TIME).build();
        JsonNode node = jsonConverter.convertToJson(schema, 3723500000L);
        Assert.assertEquals("\"01:02:03.500000\"", node.toString());
    }

    @Test
    public void serializesDebeziumNanoTimeTruncatedToMicros() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        Schema schema = SchemaBuilder.int64().name(TemporalTypeFormats.DEBEZIUM_NANO_TIME).build();
        JsonNode node = jsonConverter.convertToJson(schema, 3723500000500L);
        Assert.assertEquals("\"01:02:03.500000\"", node.toString());
    }

    @Test
    public void serializesStructWithDebeziumTimestampField() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        Schema schema = SchemaBuilder.struct()
                .field("createdAt", SchemaBuilder.int64().name(TemporalTypeFormats.DEBEZIUM_TIMESTAMP).build())
                .build();
        Struct struct = new Struct(schema).put("createdAt", 90061000L);
        JsonNode node = jsonConverter.convertToJson(schema, struct);
        Assert.assertEquals("{\"createdAt\":\"1970-01-02 01:01:01.000000\"}", node.toString());
    }

    @Test
    public void parsesKafkaConnectDateFromStarRocksString() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        String envelope = "{\"schema\":{\"type\":\"int32\",\"optional\":false,\"name\":\"org.apache.kafka.connect.data.Date\"},"
                + "\"payload\":\"1970-01-01\"}";
        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("test-topic", envelope.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(Date.toLogical(Date.SCHEMA, 0), schemaAndValue.value());
    }

    @Test
    public void parsesKafkaConnectTimestampFromStarRocksString() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        String envelope = "{\"schema\":{\"type\":\"int64\",\"optional\":false,\"name\":\"org.apache.kafka.connect.data.Timestamp\"},"
                + "\"payload\":\"1970-01-02 01:01:01.000000\"}";
        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("test-topic", envelope.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(Timestamp.toLogical(Timestamp.SCHEMA, 90061000L), schemaAndValue.value());
    }

    @Test
    public void parsesKafkaConnectTimeFromStarRocksString() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        String envelope = "{\"schema\":{\"type\":\"int32\",\"optional\":false,\"name\":\"org.apache.kafka.connect.data.Time\"},"
                + "\"payload\":\"01:02:03.000000\"}";
        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("test-topic", envelope.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(Time.toLogical(Time.SCHEMA, 3723000), schemaAndValue.value());
    }

    @Test
    public void parsesDebeziumDateFromStarRocksString() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        String envelope = "{\"schema\":{\"type\":\"int32\",\"optional\":false,\"name\":\"" + TemporalTypeFormats.DEBEZIUM_DATE + "\"},"
                + "\"payload\":\"1969-12-31\"}";
        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("test-topic", envelope.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(-1, schemaAndValue.value());
    }

    @Test
    public void parsesDebeziumMicroTimestampFromStarRocksString() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        String envelope = "{\"schema\":{\"type\":\"int64\",\"optional\":false,\"name\":\"" + TemporalTypeFormats.DEBEZIUM_MICRO_TIMESTAMP + "\"},"
                + "\"payload\":\"1970-01-01 00:00:01.500000\"}";
        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("test-topic", envelope.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(1_500_000L, schemaAndValue.value());
    }

    @Test
    public void parsesDebeziumNanoTimestampFromStarRocksString() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        String envelope = "{\"schema\":{\"type\":\"int64\",\"optional\":false,\"name\":\"" + TemporalTypeFormats.DEBEZIUM_NANO_TIMESTAMP + "\"},"
                + "\"payload\":\"1970-01-01 00:00:01.500000\"}";
        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("test-topic", envelope.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(1_500_000_000L, schemaAndValue.value());
    }

    @Test
    public void parsesDebeziumZonedTimestampFromStarRocksString() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        String envelope = "{\"schema\":{\"type\":\"string\",\"optional\":false,\"name\":\"" + TemporalTypeFormats.DEBEZIUM_ZONED_TIMESTAMP + "\"},"
                + "\"payload\":\"2024-01-01 10:15:30.123456\"}";
        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("test-topic", envelope.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals("2024-01-01T10:15:30.123456Z", schemaAndValue.value());
    }

    @Test
    public void parsesDebeziumTimeFromStarRocksString() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        String envelope = "{\"schema\":{\"type\":\"int32\",\"optional\":false,\"name\":\"" + TemporalTypeFormats.DEBEZIUM_TIME + "\"},"
                + "\"payload\":\"01:02:03.000000\"}";
        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("test-topic", envelope.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(3723000, schemaAndValue.value());
    }

    @Test
    public void parsesDebeziumMicroTimeFromStarRocksString() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        String envelope = "{\"schema\":{\"type\":\"int64\",\"optional\":false,\"name\":\"" + TemporalTypeFormats.DEBEZIUM_MICRO_TIME + "\"},"
                + "\"payload\":\"01:02:03.500000\"}";
        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("test-topic", envelope.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(3723500000L, schemaAndValue.value());
    }

    @Test
    public void parsesDebeziumNanoTimeFromStarRocksString() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        String envelope = "{\"schema\":{\"type\":\"int64\",\"optional\":false,\"name\":\"" + TemporalTypeFormats.DEBEZIUM_NANO_TIME + "\"},"
                + "\"payload\":\"01:02:03.500000\"}";
        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("test-topic", envelope.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(3723500000000L, schemaAndValue.value());
    }
}
