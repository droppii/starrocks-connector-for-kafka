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

package com.droppii.connector.kafka.json;

import org.junit.Assert;
import org.junit.Test;

public class TemporalTypeFormatsTest {

    @Test
    public void formatsAndParsesEpochDate() {
        Assert.assertEquals("1970-01-01", TemporalTypeFormats.formatDate(0));
        Assert.assertEquals(0, TemporalTypeFormats.parseDate("1970-01-01"));
    }

    @Test
    public void formatsAndParsesPreEpochDate() {
        Assert.assertEquals("1969-12-31", TemporalTypeFormats.formatDate(-1));
        Assert.assertEquals(-1, TemporalTypeFormats.parseDate("1969-12-31"));
    }

    @Test
    public void formatsAndParsesDateTimeMillis() {
        Assert.assertEquals("1970-01-02 01:01:01.000000", TemporalTypeFormats.formatDateTimeMillis(90061000L));
        Assert.assertEquals(90061000L, TemporalTypeFormats.parseDateTimeMillis("1970-01-02 01:01:01.000000"));
    }

    @Test
    public void formatsAndParsesPreEpochDateTimeMillis() {
        Assert.assertEquals("1969-12-31 23:59:59.000000", TemporalTypeFormats.formatDateTimeMillis(-1000L));
        Assert.assertEquals(-1000L, TemporalTypeFormats.parseDateTimeMillis("1969-12-31 23:59:59.000000"));
    }

    @Test
    public void formatsAndParsesDateTimeMicros() {
        Assert.assertEquals("1970-01-01 00:00:01.500000", TemporalTypeFormats.formatDateTimeMicros(1_500_000L));
        Assert.assertEquals(1_500_000L, TemporalTypeFormats.parseDateTimeMicros("1970-01-01 00:00:01.500000"));
    }

    @Test
    public void formatsDateTimeNanosTruncatedToMicros() {
        Assert.assertEquals("1970-01-01 00:00:01.500000", TemporalTypeFormats.formatDateTimeNanos(1_500_000_500L));
        Assert.assertEquals(1_500_000_000L, TemporalTypeFormats.parseDateTimeNanos("1970-01-01 00:00:01.500000"));
    }

    @Test
    public void formatsZonedTimestampNormalizedToUtc() {
        Assert.assertEquals("2024-01-01 10:15:30.123456",
                TemporalTypeFormats.formatZonedDateTime("2024-01-01T10:15:30.123456Z"));
        Assert.assertEquals("2024-01-01 10:15:30.123456",
                TemporalTypeFormats.formatZonedDateTime("2024-01-01T13:15:30.123456+03:00"));
    }

    @Test
    public void formatsPostgresInfinityZonedTimestampsAsNull() {
        Assert.assertNull(TemporalTypeFormats.formatZonedDateTime("-infinity"));
        Assert.assertNull(TemporalTypeFormats.formatZonedDateTime("infinity"));
    }

    @Test
    public void parsesZonedTimestampBackToUtcIso8601() {
        Assert.assertEquals("2024-01-01T10:15:30.123456Z",
                TemporalTypeFormats.parseZonedDateTime("2024-01-01 10:15:30.123456"));
    }

    @Test
    public void formatsAndParsesTimeMillis() {
        Assert.assertEquals("01:02:03.000000", TemporalTypeFormats.formatTimeMillis(3723000));
        Assert.assertEquals(3723000, TemporalTypeFormats.parseTimeMillis("01:02:03.000000"));
    }

    @Test
    public void formatsAndParsesTimeMicros() {
        Assert.assertEquals("01:02:03.500000", TemporalTypeFormats.formatTimeMicros(3723500000L));
        Assert.assertEquals(3723500000L, TemporalTypeFormats.parseTimeMicros("01:02:03.500000"));
    }

    @Test
    public void formatsTimeNanosTruncatedToMicros() {
        Assert.assertEquals("01:02:03.500000", TemporalTypeFormats.formatTimeNanos(3723500000500L));
        Assert.assertEquals(3723500000000L, TemporalTypeFormats.parseTimeNanos("01:02:03.500000"));
    }

    @Test
    public void exposesDebeziumLogicalTypeNameConstants() {
        Assert.assertEquals("io.debezium.time.Date", TemporalTypeFormats.DEBEZIUM_DATE);
        Assert.assertEquals("io.debezium.time.Timestamp", TemporalTypeFormats.DEBEZIUM_TIMESTAMP);
        Assert.assertEquals("io.debezium.time.MicroTimestamp", TemporalTypeFormats.DEBEZIUM_MICRO_TIMESTAMP);
        Assert.assertEquals("io.debezium.time.NanoTimestamp", TemporalTypeFormats.DEBEZIUM_NANO_TIMESTAMP);
        Assert.assertEquals("io.debezium.time.ZonedTimestamp", TemporalTypeFormats.DEBEZIUM_ZONED_TIMESTAMP);
        Assert.assertEquals("io.debezium.time.Time", TemporalTypeFormats.DEBEZIUM_TIME);
        Assert.assertEquals("io.debezium.time.MicroTime", TemporalTypeFormats.DEBEZIUM_MICRO_TIME);
        Assert.assertEquals("io.debezium.time.NanoTime", TemporalTypeFormats.DEBEZIUM_NANO_TIME);
    }
}
