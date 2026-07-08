# Schema Evolution for StarRocks Kafka Connector — Design

Date: 2026-07-08

## Background

This connector loads Kafka records into StarRocks tables via the Stream Load
SDK (HTTP, `StreamLoadManagerV2`). It currently has no way to detect or react
to changes in the shape of incoming records: if a new field appears in the
data, StarRocks Stream Load silently ignores any JSON key that has no
matching column, and the field is lost.

The user asked for schema evolution and auto-create-table, modeled on
`apache/doris-kafka-connector`. Research into that connector found:

- Its `debezium.schema.evolution` feature (`none` | `basic`) builds a
  `RecordDescriptor` from each record's Kafka Connect `Schema`, diffs field
  names against a JDBC-queried, cached column set per table, and issues
  `ALTER TABLE ADD COLUMN` for anything missing. It is **additive only** —
  no dropped columns, no type changes.
- Auto-create-table is **not implemented** there; the code has a `// TODO`
  and throws `"table does not exist, please create it manually"` today.

Per user decision, this connector implements schema evolution only.
Auto-create-table is explicitly out of scope.

## Goals

- Detect fields present in an incoming record's Kafka Connect `Schema` that
  are missing as columns in the target StarRocks table, and add them via
  `ALTER TABLE ... ADD COLUMN` before the record is (or a later record is)
  loaded.
- Opt-in via config, default off, with zero behavior change for existing
  deployments when disabled.
- Apply only to records with a reliable Kafka Connect `STRUCT` schema
  (Debezium CDC after the existing unwrap/`AddOpFieldForDebeziumRecord`
  chain, Avro/Protobuf via Schema Registry, or JSON with a schema envelope).
  CSV sink format and schemaless JSON (plain `Map`, `schemas.enable=false`)
  are left untouched — no type information exists to evolve reliably.

## Non-goals

- Auto-creating tables.
- Dropping columns, renaming columns, or changing column types.
- Evolving primary/sort/distribution keys (StarRocks does not support
  altering key columns via `ADD COLUMN` regardless).
- Schemaless JSON / CSV inference.

## Architecture

New package: `com.starrocks.connector.kafka.schema`

| Class | Responsibility |
|---|---|
| `StarRocksJdbcConnectionProvider` | Lazily opens and holds a JDBC (MySQL-protocol) connection to the StarRocks FE query port. Reconnects if the connection is found closed/broken. Only instantiated when schema evolution is enabled. |
| `StarRocksSystemService` | Introspects `information_schema.TABLES` / `information_schema.COLUMNS` via the JDBC connection: `tableExists(db, table)`, `getColumns(db, table)` returning the current column name set. |
| `StarRocksTypeMapper` | Pure function mapping a Kafka Connect field `Schema` to a StarRocks DDL type string. No I/O, fully unit-testable. |
| `SchemaEvolutionManager` | Owns a `Map<String, Set<String>>` cache of table → known column names. `evolve(String table, Schema valueSchema)`: loads/caches columns on first use (throws if the table does not exist — it must be pre-created), diffs the schema's field names against the cache, and for each missing field re-checks existence (race guard) then executes `ALTER TABLE db.table ADD COLUMN name type` via `StarRocksSystemService`, tolerating "column already exists" errors, and updates the cache. |

`StarRocksSystemService` and the type mapper are plain classes with no
external dependency beyond `java.sql`, so `SchemaEvolutionManager` can be
unit tested against a hand-written stub implementation of the introspection
calls (this repo has no mocking framework — tests are plain JUnit with
manually constructed objects, and this design follows that convention).

### Wiring into `StarRocksSinkTask`

- `start()`: if `starrocks.schema.evolution` is `basic`, construct the JDBC
  provider, system service, and `SchemaEvolutionManager`. If `none`
  (default), none of these are constructed — no new connection, no new
  behavior.
- `put()`: for each record, before calling `getRecordFromSinkRecord`, if
  evolution is enabled AND `sinkType == JSON` AND
  `record.valueSchema() != null` AND `record.valueSchema().type() == STRUCT`,
  call `schemaEvolutionManager.evolve(table, record.valueSchema())`. Any
  other record shape (CSV, schemaless JSON, non-struct schema) skips this
  call entirely, matching current behavior.
- `stop()`: closes the JDBC connection provider if it was created.

## Configuration

New keys in `StarRocksSinkConnectorConfig`:

| Key | Type | Default | Meaning |
|---|---|---|---|
| `starrocks.schema.evolution` | STRING | `none` | `none` or `basic`. `basic` enables additive `ALTER TABLE ADD COLUMN`. |
| `starrocks.query.port` | INT | `9030` | StarRocks FE MySQL query port, used with the host(s) from `starrocks.http.url` to build the JDBC URL when `starrocks.jdbc.url` is not set. |
| `starrocks.jdbc.url` | STRING | *(derived)* | Optional explicit JDBC URL override, for cases where the derived URL (http host + query port) isn't correct (e.g. different host, custom JDBC params). |

`starrocks.username` / `starrocks.password` (existing keys) are reused for
the JDBC connection.

## Type mapping

All evolution-added columns are nullable with no default — StarRocks
requires `ADD COLUMN` targets to be nullable (key/sort columns can't be
added this way, and none of this mapping applies to key columns since we
never alter key/distribution definitions).

| Kafka Connect `Schema.Type` / logical name | StarRocks DDL type |
|---|---|
| `INT8` | `TINYINT` |
| `INT16` | `SMALLINT` |
| `INT32` | `INT` |
| `INT32` + `Date` logical type | `DATE` |
| `INT64` | `BIGINT` |
| `INT64` + `Timestamp` logical type | `DATETIME` |
| `FLOAT32` | `FLOAT` |
| `FLOAT64` | `DOUBLE` |
| `BOOLEAN` | `BOOLEAN` |
| `STRING` | `STRING` |
| `BYTES` + `Decimal` logical type | `DECIMAL(p, s)` (precision from the `connect.decimal.precision` schema parameter if present, else 38; scale from the logical type's `scale` parameter) |
| `BYTES` (plain) | `STRING` |
| `STRUCT` / `ARRAY` / `MAP` (nested) | `JSON` |

## Error handling

- **Evolution disabled (default):** no change from current behavior —
  StarRocks Stream Load silently drops JSON keys with no matching column.
- **Target table does not exist:** fail fast with a clear `ConnectException`
  stating the table must be created manually (no auto-create).
- **Concurrent tasks add the same missing column simultaneously:** guarded
  by a re-check of column existence immediately before executing the
  `ALTER`; a "duplicate column" error from StarRocks on the `ALTER` itself
  is logged as a warning and treated as success (the column exists either
  way).
- **Other `ALTER` failures** (permissions, invalid generated DDL, connection
  loss): propagate immediately as a `ConnectException`. This is a separate
  failure path from the existing Stream Load `maxRetryTimes`/backoff
  counter, since DDL failures are not the kind of transient fault that
  counter exists to absorb.

## Testing plan

- `StarRocksTypeMapper`: direct unit tests covering every `Schema.Type` and
  each logical type (Date, Timestamp, Decimal with/without precision
  parameter), plus nested Struct/Array/Map → `JSON`.
- `SchemaEvolutionManager`: unit tests using a stub `StarRocksSystemService`
  (manually implemented, no mocking framework) to verify: first-use column
  loading, correct diffing of missing fields, correct `ALTER` DDL generated,
  cache updated after a successful add, "already exists" tolerated, and the
  "table does not exist" failure path.
- `StarRocksSinkTask`: tests confirming `evolve()` is invoked only for
  JSON + STRUCT-schema records when evolution is enabled, and never invoked
  for CSV, schemaless JSON, or when evolution is `none` (default).
- No new integration/e2e harness is added — this repo has none today, and
  real-cluster verification (actually connecting to a StarRocks FE and
  observing the `ALTER TABLE`) is manual, consistent with existing test
  depth.

## Documentation

`README.md` gets a new section documenting the three new config keys, the
additive-only/no-auto-create semantics, and an example config snippet
showing `starrocks.schema.evolution=basic` alongside the existing Debezium
unwrap SMT chain.
