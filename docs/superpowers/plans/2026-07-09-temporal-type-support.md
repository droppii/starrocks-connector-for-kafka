# Native Temporal Logical Type Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serialize Kafka Connect and Debezium temporal logical types (Date/Time/Timestamp and their Debezium micro/nano/zoned variants) to StarRocks-native string formats, both in the JSON payload sent to Stream Load and in the schema-evolution DDL type mapping, so the external `TimestampConverter` SMT is no longer required.

**Architecture:** A new pure, I/O-free class `TemporalTypeFormats` holds the Debezium logical-type-name constants and all format/parse string conversions (`java.time`-based). `JsonConverter`'s existing `LOGICAL_CONVERTERS` map is extended to call it for `Date`/`Time`/`Timestamp` (replacing numeric output with string output) and for 8 new Debezium logical type names. `StarRocksTypeMapper.mapType` is extended to recognize the same logical names for DDL purposes.

**Tech Stack:** Java 8, `java.time`, Jackson (`JsonNode`), Kafka Connect API (`org.apache.kafka.connect.data`), JUnit 4.12.

## Global Constraints

- Compile target is Java 8 (`maven.compiler.source`/`target` = 8) — `java.time` is available, no new Maven dependency needed.
- Test style: JUnit 4 (`org.junit.Test`, `org.junit.Assert`), matching `StarRocksTypeMapperTest` and `JsonConverterTest`.
- No new connector configuration properties — output format and UTC normalization are fixed (per `docs/superpowers/specs/2026-07-09-temporal-type-support-design.md`).
- Fractional seconds in all DATETIME/TIME string output are always exactly 6 digits (zero-padded or truncated), never conditionally trimmed.
- Out of scope: `io.debezium.data.VariableScaleDecimal`, `io.debezium.time.Interval`, `io.debezium.data.Enum`/`EnumSet`/`Json`/`Bits`, any new config, and `TimeMillisToStringTransform` (left untouched).

---

### Task 1: `TemporalTypeFormats` — pure format/parse utility

**Files:**
- Create: `src/main/java/com/starrocks/connector/kafka/json/TemporalTypeFormats.java`
- Test: `src/test/java/com/starrocks/connector/kafka/json/TemporalTypeFormatsTest.java`

**Interfaces:**
- Produces (used by Task 2 and Task 3):
  - Constants: `TemporalTypeFormats.DEBEZIUM_DATE`, `DEBEZIUM_TIMESTAMP`, `DEBEZIUM_MICRO_TIMESTAMP`, `DEBEZIUM_NANO_TIMESTAMP`, `DEBEZIUM_ZONED_TIMESTAMP`, `DEBEZIUM_TIME`, `DEBEZIUM_MICRO_TIME`, `DEBEZIUM_NANO_TIME` (all `String`)
  - `static String formatDate(int epochDay)` / `static int parseDate(String text)`
  - `static String formatDateTimeMillis(long epochMillis)` / `static long parseDateTimeMillis(String text)`
  - `static String formatDateTimeMicros(long epochMicros)` / `static long parseDateTimeMicros(String text)`
  - `static String formatDateTimeNanos(long epochNanos)` / `static long parseDateTimeNanos(String text)`
  - `static String formatZonedDateTime(String iso8601)` / `static String parseZonedDateTime(String text)`
  - `static String formatTimeMillis(int millisOfDay)` / `static int parseTimeMillis(String text)`
  - `static String formatTimeMicros(long microsOfDay)` / `static long parseTimeMicros(String text)`
  - `static String formatTimeNanos(long nanosOfDay)` / `static long parseTimeNanos(String text)`

- [ ] **Step 1: Write the failing test file**

Create `src/test/java/com/starrocks/connector/kafka/json/TemporalTypeFormatsTest.java`:

```java
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
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `mvn test -Dtest=TemporalTypeFormatsTest -q`
Expected: BUILD FAILURE — `cannot find symbol: class TemporalTypeFormats`

- [ ] **Step 3: Write the implementation**

Create `src/main/java/com/starrocks/connector/kafka/json/TemporalTypeFormats.java`:

```java
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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

// Converts Kafka Connect and Debezium temporal logical-type values to/from
// the plain string formats StarRocks Stream Load expects for DATE/DATETIME
// columns (StarRocks has no timezone-aware DATETIME, so zoned inputs are
// normalized to UTC). Fractional seconds are always rendered as exactly 6
// digits; nanosecond-precision inputs are truncated (not rounded) to
// microseconds, matching StarRocks DATETIME's maximum precision.
public final class TemporalTypeFormats {

    public static final String DEBEZIUM_DATE = "io.debezium.time.Date";
    public static final String DEBEZIUM_TIMESTAMP = "io.debezium.time.Timestamp";
    public static final String DEBEZIUM_MICRO_TIMESTAMP = "io.debezium.time.MicroTimestamp";
    public static final String DEBEZIUM_NANO_TIMESTAMP = "io.debezium.time.NanoTimestamp";
    public static final String DEBEZIUM_ZONED_TIMESTAMP = "io.debezium.time.ZonedTimestamp";
    public static final String DEBEZIUM_TIME = "io.debezium.time.Time";
    public static final String DEBEZIUM_MICRO_TIME = "io.debezium.time.MicroTime";
    public static final String DEBEZIUM_NANO_TIME = "io.debezium.time.NanoTime";

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");

    private TemporalTypeFormats() {
    }

    public static String formatDate(int epochDay) {
        return LocalDate.ofEpochDay(epochDay).format(DATE_FORMATTER);
    }

    public static int parseDate(String text) {
        return (int) LocalDate.parse(text, DATE_FORMATTER).toEpochDay();
    }

    public static String formatDateTimeMillis(long epochMillis) {
        return formatDateTimeMicros(Math.multiplyExact(epochMillis, 1000L));
    }

    public static long parseDateTimeMillis(String text) {
        return Math.floorDiv(parseDateTimeMicros(text), 1000L);
    }

    public static String formatDateTimeMicros(long epochMicros) {
        long epochSeconds = Math.floorDiv(epochMicros, 1_000_000L);
        long microOfSecond = Math.floorMod(epochMicros, 1_000_000L);
        Instant instant = Instant.ofEpochSecond(epochSeconds, microOfSecond * 1000L);
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC).format(DATE_TIME_FORMATTER);
    }

    public static long parseDateTimeMicros(String text) {
        LocalDateTime dateTime = LocalDateTime.parse(text, DATE_TIME_FORMATTER);
        Instant instant = dateTime.toInstant(ZoneOffset.UTC);
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1000L);
    }

    public static String formatDateTimeNanos(long epochNanos) {
        return formatDateTimeMicros(Math.floorDiv(epochNanos, 1000L));
    }

    public static long parseDateTimeNanos(String text) {
        return Math.multiplyExact(parseDateTimeMicros(text), 1000L);
    }

    public static String formatZonedDateTime(String iso8601) {
        OffsetDateTime offsetDateTime = OffsetDateTime.parse(iso8601);
        Instant instant = offsetDateTime.toInstant();
        long epochMicros = Math.addExact(
                Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1000L);
        return formatDateTimeMicros(epochMicros);
    }

    public static String parseZonedDateTime(String text) {
        LocalDateTime dateTime = LocalDateTime.parse(text, DATE_TIME_FORMATTER);
        return dateTime.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    public static String formatTimeMillis(int millisOfDay) {
        return formatTimeMicros(millisOfDay * 1000L);
    }

    public static int parseTimeMillis(String text) {
        return (int) (parseTimeMicros(text) / 1000L);
    }

    public static String formatTimeMicros(long microsOfDay) {
        LocalTime time = LocalTime.ofNanoOfDay(Math.multiplyExact(microsOfDay, 1000L));
        return time.format(TIME_FORMATTER);
    }

    public static long parseTimeMicros(String text) {
        LocalTime time = LocalTime.parse(text, TIME_FORMATTER);
        return time.toNanoOfDay() / 1000L;
    }

    public static String formatTimeNanos(long nanosOfDay) {
        return formatTimeMicros(Math.floorDiv(nanosOfDay, 1000L));
    }

    public static long parseTimeNanos(String text) {
        return Math.multiplyExact(parseTimeMicros(text), 1000L);
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn test -Dtest=TemporalTypeFormatsTest -q`
Expected: `Tests run: 12, Failures: 0, Errors: 0`

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/starrocks/connector/kafka/json/TemporalTypeFormats.java src/test/java/com/starrocks/connector/kafka/json/TemporalTypeFormatsTest.java
git commit -m "$(cat <<'EOF'
feat: add TemporalTypeFormats for StarRocks-native date/time strings

Pure java.time-based format/parse utility covering Kafka Connect's
built-in Date/Time/Timestamp precision and Debezium's micro/nano/zoned
variants. Zoned inputs are normalized to UTC; nanosecond inputs are
truncated to microseconds to match StarRocks DATETIME's precision.
EOF
)"
```

---

### Task 2: Wire `TemporalTypeFormats` into `JsonConverter` serialization

**Files:**
- Modify: `src/main/java/com/starrocks/connector/kafka/json/JsonConverter.java:165-211`
- Test: `src/test/java/com/starrocks/connector/kafka/json/JsonConverterTest.java`

**Interfaces:**
- Consumes: `TemporalTypeFormats.{formatDate,parseDate,formatDateTimeMillis,parseDateTimeMillis,formatDateTimeMicros,parseDateTimeMicros,formatDateTimeNanos,parseDateTimeNanos,formatZonedDateTime,parseZonedDateTime,formatTimeMillis,parseTimeMillis,formatTimeMicros,parseTimeMicros,formatTimeNanos,parseTimeNanos}` and the 8 `DEBEZIUM_*` constants from Task 1.
- Produces: `JsonConverter.convertToJson(Schema, Object)` now emits StarRocks-formatted string `JsonNode`s (instead of numeric epoch `JsonNode`s) for `Date`/`Time`/`Timestamp`-logical-name fields and for the 8 Debezium logical-type-name fields. Used by `StarRocksSinkTask.getRecordFromSinkRecord` (unchanged call site).

- [ ] **Step 1: Write the failing tests**

Add to `src/test/java/com/starrocks/connector/kafka/json/JsonConverterTest.java`. First add these imports after the existing `import java.util.Map;` line (line 34):

```java
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
```

Then add these test methods inside the `JsonConverterTest` class, after `testConvertToJson()`:

```java
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
    public void roundTripsStructWithDebeziumTimestampField() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        Schema schema = SchemaBuilder.struct()
                .field("createdAt", SchemaBuilder.int64().name(TemporalTypeFormats.DEBEZIUM_TIMESTAMP).build())
                .build();
        Struct struct = new Struct(schema).put("createdAt", 90061000L);
        JsonNode node = jsonConverter.convertToJson(schema, struct);
        Assert.assertEquals("{\"createdAt\":\"1970-01-02 01:01:01.000000\"}", node.toString());
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn test -Dtest=JsonConverterTest -q`
Expected: FAILURE — the KC `Date`/`Time`/`Timestamp` assertions fail because the converter still emits numeric `JsonNode`s (e.g. expected `"1970-01-01"` but got `0`); the Debezium-named assertions fail with `DataException: Unknown schema type` since those logical names are not yet registered in `LOGICAL_CONVERTERS`.

- [ ] **Step 3: Replace the Date/Time/Timestamp converters and add the Debezium converters**

In `src/main/java/com/starrocks/connector/kafka/json/JsonConverter.java`, replace lines 165-211 (the `LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, ...)` through `LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, ...)` blocks, i.e. everything between the `Decimal` block's closing `});` and the final `});` before `}` that closes the static initializer) with:

```java
        LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof java.util.Date))
                    throw new DataException("Invalid type for Date, expected Date but was " + value.getClass());
                int epochDay = Date.fromLogical(schema, (java.util.Date) value);
                return JSON_NODE_FACTORY.textNode(TemporalTypeFormats.formatDate(epochDay));
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (!value.isTextual())
                    throw new DataException("Invalid type for Date, underlying representation should be a string but was " + value.getNodeType());
                return Date.toLogical(schema, TemporalTypeFormats.parseDate(value.textValue()));
            }
        });

        LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof java.util.Date))
                    throw new DataException("Invalid type for Time, expected Date but was " + value.getClass());
                int millisOfDay = Time.fromLogical(schema, (java.util.Date) value);
                return JSON_NODE_FACTORY.textNode(TemporalTypeFormats.formatTimeMillis(millisOfDay));
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (!value.isTextual())
                    throw new DataException("Invalid type for Time, underlying representation should be a string but was " + value.getNodeType());
                return Time.toLogical(schema, TemporalTypeFormats.parseTimeMillis(value.textValue()));
            }
        });

        LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof java.util.Date))
                    throw new DataException("Invalid type for Timestamp, expected Date but was " + value.getClass());
                long epochMillis = Timestamp.fromLogical(schema, (java.util.Date) value);
                return JSON_NODE_FACTORY.textNode(TemporalTypeFormats.formatDateTimeMillis(epochMillis));
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (!value.isTextual())
                    throw new DataException("Invalid type for Timestamp, underlying representation should be a string but was " + value.getNodeType());
                return Timestamp.toLogical(schema, TemporalTypeFormats.parseDateTimeMillis(value.textValue()));
            }
        });

        LOGICAL_CONVERTERS.put(TemporalTypeFormats.DEBEZIUM_DATE, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof Integer))
                    throw new DataException("Invalid type for " + TemporalTypeFormats.DEBEZIUM_DATE + ", expected Integer but was " + value.getClass());
                return JSON_NODE_FACTORY.textNode(TemporalTypeFormats.formatDate((Integer) value));
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (!value.isTextual())
                    throw new DataException("Invalid type for " + TemporalTypeFormats.DEBEZIUM_DATE + ", underlying representation should be a string but was " + value.getNodeType());
                return TemporalTypeFormats.parseDate(value.textValue());
            }
        });

        LOGICAL_CONVERTERS.put(TemporalTypeFormats.DEBEZIUM_TIMESTAMP, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof Long))
                    throw new DataException("Invalid type for " + TemporalTypeFormats.DEBEZIUM_TIMESTAMP + ", expected Long but was " + value.getClass());
                return JSON_NODE_FACTORY.textNode(TemporalTypeFormats.formatDateTimeMillis((Long) value));
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (!value.isTextual())
                    throw new DataException("Invalid type for " + TemporalTypeFormats.DEBEZIUM_TIMESTAMP + ", underlying representation should be a string but was " + value.getNodeType());
                return TemporalTypeFormats.parseDateTimeMillis(value.textValue());
            }
        });

        LOGICAL_CONVERTERS.put(TemporalTypeFormats.DEBEZIUM_MICRO_TIMESTAMP, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof Long))
                    throw new DataException("Invalid type for " + TemporalTypeFormats.DEBEZIUM_MICRO_TIMESTAMP + ", expected Long but was " + value.getClass());
                return JSON_NODE_FACTORY.textNode(TemporalTypeFormats.formatDateTimeMicros((Long) value));
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (!value.isTextual())
                    throw new DataException("Invalid type for " + TemporalTypeFormats.DEBEZIUM_MICRO_TIMESTAMP + ", underlying representation should be a string but was " + value.getNodeType());
                return TemporalTypeFormats.parseDateTimeMicros(value.textValue());
            }
        });

        LOGICAL_CONVERTERS.put(TemporalTypeFormats.DEBEZIUM_NANO_TIMESTAMP, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof Long))
                    throw new DataException("Invalid type for " + TemporalTypeFormats.DEBEZIUM_NANO_TIMESTAMP + ", expected Long but was " + value.getClass());
                return JSON_NODE_FACTORY.textNode(TemporalTypeFormats.formatDateTimeNanos((Long) value));
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (!value.isTextual())
                    throw new DataException("Invalid type for " + TemporalTypeFormats.DEBEZIUM_NANO_TIMESTAMP + ", underlying representation should be a string but was " + value.getNodeType());
                return TemporalTypeFormats.parseDateTimeNanos(value.textValue());
            }
        });

        LOGICAL_CONVERTERS.put(TemporalTypeFormats.DEBEZIUM_ZONED_TIMESTAMP, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof String))
                    throw new DataException("Invalid type for " + TemporalTypeFormats.DEBEZIUM_ZONED_TIMESTAMP + ", expected String but was " + value.getClass());
                return JSON_NODE_FACTORY.textNode(TemporalTypeFormats.formatZonedDateTime((String) value));
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (!value.isTextual())
                    throw new DataException("Invalid type for " + TemporalTypeFormats.DEBEZIUM_ZONED_TIMESTAMP + ", underlying representation should be a string but was " + value.getNodeType());
                return TemporalTypeFormats.parseZonedDateTime(value.textValue());
            }
        });

        LOGICAL_CONVERTERS.put(TemporalTypeFormats.DEBEZIUM_TIME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof Integer))
                    throw new DataException("Invalid type for " + TemporalTypeFormats.DEBEZIUM_TIME + ", expected Integer but was " + value.getClass());
                return JSON_NODE_FACTORY.textNode(TemporalTypeFormats.formatTimeMillis((Integer) value));
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (!value.isTextual())
                    throw new DataException("Invalid type for " + TemporalTypeFormats.DEBEZIUM_TIME + ", underlying representation should be a string but was " + value.getNodeType());
                return TemporalTypeFormats.parseTimeMillis(value.textValue());
            }
        });

        LOGICAL_CONVERTERS.put(TemporalTypeFormats.DEBEZIUM_MICRO_TIME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof Long))
                    throw new DataException("Invalid type for " + TemporalTypeFormats.DEBEZIUM_MICRO_TIME + ", expected Long but was " + value.getClass());
                return JSON_NODE_FACTORY.textNode(TemporalTypeFormats.formatTimeMicros((Long) value));
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (!value.isTextual())
                    throw new DataException("Invalid type for " + TemporalTypeFormats.DEBEZIUM_MICRO_TIME + ", underlying representation should be a string but was " + value.getNodeType());
                return TemporalTypeFormats.parseTimeMicros(value.textValue());
            }
        });

        LOGICAL_CONVERTERS.put(TemporalTypeFormats.DEBEZIUM_NANO_TIME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof Long))
                    throw new DataException("Invalid type for " + TemporalTypeFormats.DEBEZIUM_NANO_TIME + ", expected Long but was " + value.getClass());
                return JSON_NODE_FACTORY.textNode(TemporalTypeFormats.formatTimeNanos((Long) value));
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (!value.isTextual())
                    throw new DataException("Invalid type for " + TemporalTypeFormats.DEBEZIUM_NANO_TIME + ", underlying representation should be a string but was " + value.getNodeType());
                return TemporalTypeFormats.parseTimeNanos(value.textValue());
            }
        });
```

No new imports are needed in `JsonConverter.java` — `Date`, `Time`, `Timestamp` are already covered by the existing `import org.apache.kafka.connect.data.*;` wildcard, and `TemporalTypeFormats` is in the same package.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn test -Dtest=JsonConverterTest -q`
Expected: `Tests run: 12, Failures: 0, Errors: 0`

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/starrocks/connector/kafka/json/JsonConverter.java src/test/java/com/starrocks/connector/kafka/json/JsonConverterTest.java
git commit -m "$(cat <<'EOF'
feat: serialize temporal logical types as StarRocks-native strings

JsonConverter previously encoded Kafka Connect's built-in Date/Time/
Timestamp logical types as raw numeric epoch values (Kafka's own wire
format), which StarRocks Stream Load can't parse into DATE/DATETIME
columns. It also didn't recognize Debezium's own temporal logical
types at all. Both now serialize through TemporalTypeFormats into
StarRocks' native DATE/DATETIME/TIME string formats.
EOF
)"
```

---

### Task 3: Recognize temporal logical types in `StarRocksTypeMapper`

**Files:**
- Modify: `src/main/java/com/starrocks/connector/kafka/schema/StarRocksTypeMapper.java`
- Test: `src/test/java/com/starrocks/connector/kafka/schema/StarRocksTypeMapperTest.java`

**Interfaces:**
- Consumes: `TemporalTypeFormats.{DEBEZIUM_DATE,DEBEZIUM_TIMESTAMP,DEBEZIUM_MICRO_TIMESTAMP,DEBEZIUM_NANO_TIMESTAMP,DEBEZIUM_ZONED_TIMESTAMP,DEBEZIUM_TIME,DEBEZIUM_MICRO_TIME,DEBEZIUM_NANO_TIME}` from Task 1.
- Produces: `StarRocksTypeMapper.mapType(Schema)` now returns `"DATE"`/`"DATETIME"`/`"STRING"` for the same logical names `JsonConverter` (Task 2) now serializes as strings. Used by `SchemaEvolutionManager` (unchanged call site).

- [ ] **Step 1: Write the failing tests**

In `src/test/java/com/starrocks/connector/kafka/schema/StarRocksTypeMapperTest.java`, add these imports after the existing `import org.apache.kafka.connect.data.Timestamp;` line (line 27):

```java
import org.apache.kafka.connect.data.Time;
import com.starrocks.connector.kafka.json.TemporalTypeFormats;
```

Then add these test methods inside the `StarRocksTypeMapperTest` class, after `mapsTimestampLogicalType()`:

```java
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn test -Dtest=StarRocksTypeMapperTest -q`
Expected: FAILURE — `mapsTimeLogicalTypeToString` gets `"INT"` instead of `"STRING"`; the Debezium-named tests get `"INT"`/`"BIGINT"`/`"STRING"` (raw primitive fallback) instead of `"DATE"`/`"DATETIME"`/`"STRING"`.

- [ ] **Step 3: Update `mapType`**

In `src/main/java/com/starrocks/connector/kafka/schema/StarRocksTypeMapper.java`, replace the imports block (lines 23-27):

```java
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
```

with:

```java
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
```

Then replace lines 32-49 (from `public final class StarRocksTypeMapper {` through the closing brace of `mapType`'s `Timestamp` check, i.e. through):

```java
    public static String mapType(Schema schema) {
        String logicalName = schema.name();
        if (Date.LOGICAL_NAME.equals(logicalName)) {
            return "DATE";
        }
        if (Timestamp.LOGICAL_NAME.equals(logicalName)) {
            return "DATETIME";
        }
```

with:

```java
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
```

(This inserts the three `Set` fields between the class declaration/constructor area and `mapType`, and replaces the two `if` checks with `Set.contains` checks plus the new `TIME_LOGICAL_NAMES` check. The rest of `mapType` — the `Decimal` check, the `switch (schema.type())`, and `mapDecimal` — is unchanged.)

Note `HashSet.contains(null)` returns `false` (no `NullPointerException`), so schemas with no logical name (`schema.name() == null`) safely fall through to the `Decimal` check and the primitive-type `switch`, exactly as before.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn test -Dtest=StarRocksTypeMapperTest -q`
Expected: `Tests run: 10, Failures: 0, Errors: 0`

- [ ] **Step 5: Run the full test suite**

Run: `mvn test -q`
Expected: `BUILD SUCCESS`, no failures across the whole project.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/starrocks/connector/kafka/schema/StarRocksTypeMapper.java src/test/java/com/starrocks/connector/kafka/schema/StarRocksTypeMapperTest.java
git commit -m "$(cat <<'EOF'
feat: map temporal logical types to matching StarRocks DDL types

StarRocksTypeMapper previously had no case for Kafka Connect's Time
logical type (fell through to INT) and didn't recognize any Debezium
temporal logical type. Both now map to the DDL type that matches what
JsonConverter serializes: DATE, DATETIME, or STRING for the Time
family (mirroring TimeMillisToStringTransform's existing convention).
EOF
)"
```

---

### Task 4: Document the type mapping in the README

**Files:**
- Modify: `README.md`

**Interfaces:**
- None (documentation only).

- [ ] **Step 1: Add a Type Mapping section**

In `README.md`, insert a new section after the "Schema Evolution" section's config table and example block (after the line containing `starrocks.query.port=9030` and its closing ` ``` `, before `## LICENSE`):

```markdown
## Type Mapping

For schema evolution DDL (`ALTER TABLE ADD COLUMN`) and for the JSON values
sent to Stream Load, Kafka Connect and Debezium logical types map to
StarRocks types as follows:

| Kafka Connect / Debezium logical type | StarRocks DDL type | Serialized value format |
|---|---|---|
| `org.apache.kafka.connect.data.Date`, `io.debezium.time.Date` | `DATE` | `yyyy-MM-dd` |
| `org.apache.kafka.connect.data.Timestamp`, `io.debezium.time.Timestamp`, `io.debezium.time.MicroTimestamp`, `io.debezium.time.NanoTimestamp`, `io.debezium.time.ZonedTimestamp` | `DATETIME` | `yyyy-MM-dd HH:mm:ss.SSSSSS` (always 6 fractional digits; nanosecond-precision sources are truncated, not rounded, to microseconds; `ZonedTimestamp` is normalized to UTC before formatting) |
| `org.apache.kafka.connect.data.Time`, `io.debezium.time.Time`, `io.debezium.time.MicroTime`, `io.debezium.time.NanoTime` | `STRING` | `HH:mm:ss.SSSSSS` (same precision rules as above) |
| `org.apache.kafka.connect.data.Decimal` | `DECIMAL(p,s)` | numeric |

These conversions happen automatically — no SMT (e.g. a `TimestampConverter`
transform) is needed to reformat temporal fields before they reach the sink.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "$(cat <<'EOF'
docs: document temporal logical type mapping

Records the DATE/DATETIME/STRING/DECIMAL mapping and string formats
now produced natively, so users no longer need an external
TimestampConverter SMT for Kafka Connect or Debezium temporal fields.
EOF
)"
```
