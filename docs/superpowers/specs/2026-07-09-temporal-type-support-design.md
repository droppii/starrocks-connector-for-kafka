# Native Temporal Logical Type Support — Design

Date: 2026-07-09

## Background

The connector serializes Kafka Connect records to JSON for StarRocks Stream
Load via a vendored copy of Kafka's `JsonConverter`
(`com.starrocks.connector.kafka.json.JsonConverter`). That copy's
`LOGICAL_CONVERTERS` table only recognizes the four built-in
`org.apache.kafka.connect.data.*` logical types (`Decimal`, `Date`, `Time`,
`Timestamp`), and for `Date`/`Time`/`Timestamp` it serializes them using
Kafka's own wire encoding — raw epoch numbers (epoch day / millis-since-midnight
/ epoch millis) — rather than the string format StarRocks Stream Load expects
for `DATE`/`DATETIME` columns.

Debezium's own logical types (`io.debezium.time.Date`, `Timestamp`,
`MicroTimestamp`, `NanoTimestamp`, `ZonedTimestamp`, `Time`, `MicroTime`,
`NanoTime`) aren't recognized at all — they fall through to raw primitive
handling.

`StarRocksTypeMapper` (used for schema-evolution `ALTER TABLE ADD COLUMN`
DDL) has the same gap: it maps KC `Date`/`Timestamp` to `DATE`/`DATETIME`,
has no case for KC `Time` (falls through to `INT`), and doesn't recognize any
Debezium logical type name.

Today, users work around this with an external SMT
(`com.github.howareyouo.kafka.connect.transforms.TimestampConverter`) to
pre-format temporal fields into StarRocks-compatible strings before they
reach the sink. This connector already solved the identical problem for one
type — `TimeMillisToStringTransform` converts KC `Time` fields to
`HH:mm:ss[.SSS]` strings — but only for that one case, and only via an SMT
users must explicitly wire in.

This change adds native handling for all of the above directly in the
connector, closing the gap so the external `TimestampConverter` SMT is no
longer needed.

## Goals

- Serialize KC `Date`/`Time`/`Timestamp` and all 8 Debezium temporal logical
  types to StarRocks-native string formats during `JsonConverter.convertToJson`.
- Recognize the same logical types in `StarRocksTypeMapper.mapType` so
  schema-evolution DDL matches the serialized value shape.
- Normalize `ZonedTimestamp`'s offset/zone to UTC before formatting, since
  StarRocks `DATETIME` has no timezone concept.
- Keep `JsonConverter`'s `toConnect` (JSON → Connect) direction symmetric
  with `toJson`, so the converter still round-trips correctly.

## Non-goals

- `io.debezium.data.VariableScaleDecimal` — a nested `{scale, value}` struct,
  structurally different from every other type in scope. Left for a future,
  focused change.
- `io.debezium.time.Interval`, `io.debezium.data.Enum`/`EnumSet`/`Json`/`Bits`
  — already plain-`STRING`-backed and already handled correctly by the
  existing generic fallback in both `JsonConverter` and `StarRocksTypeMapper`.
  No change needed.
- New connector configuration. Output format and UTC normalization are fixed,
  not configurable (mirrors StarRocks' own DATETIME literal format; avoids
  reintroducing the kind of external-format configuration this change is
  removing the need for).
- Deprecating or removing `TimeMillisToStringTransform`. It remains harmless
  for existing users: by the time a field reaches `JsonConverter` after that
  SMT runs, it is already a plain optional `STRING` schema, so there is no
  double-conversion.

## Architecture

New class: `com.starrocks.connector.kafka.json.TemporalTypeFormats`

Pure, I/O-free, built on `java.time` (already available on Java 8, the
project's compile target — no new dependency). Holds:

1. **Logical type name constants** for the 8 Debezium types, e.g.
   `DEBEZIUM_DATE = "io.debezium.time.Date"`,
   `DEBEZIUM_TIMESTAMP = "io.debezium.time.Timestamp"`, etc. Single source of
   truth shared by `JsonConverter` and `StarRocksTypeMapper` so the two
   classes cannot drift on the literal strings.
2. **Format/parse method pairs**, one per representation:
   - `formatDate(int epochDay)` / `parseDate(String)`
   - `formatDateTimeMillis(long epochMillis)` / `parseDateTimeMillis(String)`
   - `formatDateTimeMicros(long epochMicros)` / `parseDateTimeMicros(String)`
   - `formatDateTimeNanos(long epochNanos)` / `parseDateTimeNanos(String)`
   - `formatZonedDateTime(String iso8601)` / `parseZonedDateTime(String)`
   - `formatTimeMillis(int millisOfDay)` / `parseTimeMillis(String)`
   - `formatTimeMicros(long microsOfDay)` / `parseTimeMicros(String)`
   - `formatTimeNanos(long nanosOfDay)` / `parseTimeNanos(String)`

**Key implementation nuance:** KC's built-in `Date`/`Time`/`Timestamp`
logical types wrap values as `java.util.Date` (per `Date.toLogical`/
`fromLogical` etc.). Debezium's logical types are not registered Kafka
Connect logical types — they're a plain `INT32`/`INT64`/`STRING` schema with
just a `name()` tag, and Debezium puts a raw `Integer`/`Long`/`String`
directly into the `Struct` field. The 8 new Debezium `LogicalTypeConverter`
entries in `JsonConverter` therefore read/write raw primitives, not
`java.util.Date` — distinct from the existing 3 KC entries, which continue
wrapping/unwrapping `java.util.Date` but now call into `TemporalTypeFormats`
for the string representation instead of emitting a `numberNode`.

`JsonConverter.LOGICAL_CONVERTERS` changes:
- `Date`/`Time`/`Timestamp` entries: `toJson` now returns `textNode(...)`
  instead of `numberNode(...)`; `toConnect` now parses text instead of
  reading an int/long.
- 8 new entries keyed by the Debezium logical names, each a thin
  `LogicalTypeConverter` delegating to the matching `TemporalTypeFormats`
  method.

`StarRocksTypeMapper.mapType` changes:
- Recognizes the same logical names (via `TemporalTypeFormats` constants)
  and maps them per the table below.

## Formatting & precision rules

| Logical type | Underlying value | Output string |
|---|---|---|
| `Date` (KC), Debezium `Date` | epoch day (int) | `yyyy-MM-dd` |
| `Timestamp` (KC), Debezium `Timestamp` | epoch millis (long) | `yyyy-MM-dd HH:mm:ss.SSS000` |
| Debezium `MicroTimestamp` | epoch micros (long) | `yyyy-MM-dd HH:mm:ss.SSSSSS` (exact) |
| Debezium `NanoTimestamp` | epoch nanos (long) | same, truncated to micros |
| Debezium `ZonedTimestamp` | ISO-8601 offset string | parsed, converted to UTC, formatted as above |
| `Time` (KC), Debezium `Time` | millis since midnight | `HH:mm:ss.SSS000` |
| Debezium `MicroTime` | micros since midnight | `HH:mm:ss.SSSSSS` (exact) |
| Debezium `NanoTime` | nanos since midnight | truncated to micros |

Fractional seconds are always present and always exactly 6 digits —
zero-padded or truncated as needed. No conditional trimming logic. StarRocks'
Stream Load JSON parser accepts this regardless of the target column's
configured scale.

Negative epoch values (dates/timestamps before 1970, e.g. a `date_of_birth`
field) work with no special-casing — `LocalDate.ofEpochDay` and
`Instant.ofEpochMilli` both handle negative inputs natively. Covered by an
explicit test case.

`NanoTimestamp`/`NanoTime` truncate (not round) to microsecond precision,
since StarRocks `DATETIME` supports at most 6 fractional digits.

## DDL mapping (`StarRocksTypeMapper`)

| Logical type(s) | DDL type |
|---|---|
| `Date` (KC), Debezium `Date` | `DATE` |
| `Timestamp` (KC), Debezium `Timestamp`/`MicroTimestamp`/`NanoTimestamp`/`ZonedTimestamp` | `DATETIME` |
| `Time` (KC), Debezium `Time`/`MicroTime`/`NanoTime` | `STRING` |

`Time` (KC) is a new case — today it falls through to `INT` (the raw
`INT32` type), which will now be replaced with `STRING`, consistent with the
`STRING` shape `TimeMillisToStringTransform` already produces for the same
logical type.

## Behavior change to flag

This vendored `JsonConverter`'s wire format for KC `Date`/`Time`/`Timestamp`
changes from numeric epoch to formatted string. That is the point of this
change, but it is technically a breaking change to that class's `Converter`
contract for anyone who (unusually) configured it directly as a Kafka
Connect worker's `value.converter`. `StarRocksSinkTask` only calls
`convertToJson` internally on already-deserialized records, so the normal
sink data path is unaffected.

## Testing

- New `TemporalTypeFormatsTest` — unit tests for every format/parse pair,
  including a pre-1970 date/timestamp case and nanosecond-truncation case.
- Extend `JsonConverterTest` — `Struct` round-trip through `convertToJson`/
  `toConnectData` for all 11 logical types (3 KC + 8 Debezium), asserting
  exact StarRocks-formatted JSON string output.
- Extend `StarRocksTypeMapperTest` — Debezium logical names map to
  `DATE`/`DATETIME`; `Time`-family (KC and Debezium) maps to `STRING`.

## Documentation

README gets a short "Type Mapping" section listing the table above and
noting that KC and Debezium temporal logical types no longer require an
external `TimestampConverter`/format SMT.
