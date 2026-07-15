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

    // PostgreSQL's timestamptz supports the sentinel values -infinity/infinity (an
    // unbounded range endpoint, not a real instant). Debezium passes them through as
    // literal strings since there's no ISO-8601 representation for them, and StarRocks
    // DATETIME has no infinity concept either, so they're dropped to null.
    private static final String NEGATIVE_INFINITY = "-infinity";
    private static final String POSITIVE_INFINITY = "infinity";

    public static String formatZonedDateTime(String iso8601) {
        if (NEGATIVE_INFINITY.equals(iso8601) || POSITIVE_INFINITY.equals(iso8601)) {
            return null;
        }
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
