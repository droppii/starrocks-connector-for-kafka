# StarRocks Connector for Apache Kafka
starrocks-connector-for-kafka is a plugin of Apache Kafka Connect - Used to Ingest data from Kafka Topic to StarRocks Table.

## Documentation
For the user manual of the released version of the Kafka connector, please visit the StarRocks official documentation.


* [Load data using Kafka connector](https://docs.starrocks.io/docs/loading/Kafka-connector-starrocks/)

## How to build
Executing the `mvn package` command will generate the JAR file for the connector along with the JAR files that the connector depends on. The path is located at `target/starrocks-connector-for-kafka-1.0-SNAPSHOT-package/share/java`.

## Schema Evolution
The connector can additively evolve the target StarRocks table when new fields
appear in incoming records: `ALTER TABLE ... ADD COLUMN` is issued for any
field present in a record's schema but missing from the table. This is
**opt-in, additive-only, and does not create tables** — the target table must
already exist, and no columns are ever dropped or changed in type.

It only applies to records with a Kafka Connect `STRUCT` schema (e.g.
Debezium CDC records after `ExtractNewRecordState`/`AddOpFieldForDebeziumRecord`,
or Avro/Protobuf via Schema Registry) and `sink.properties.format=json`. CSV
sink format and schemaless JSON records are unaffected.

| Config | Default | Description |
|---|---|---|
| `starrocks.schema.evolution` | `none` | `none` (disabled) or `basic` (enable additive `ALTER TABLE ADD COLUMN`). |
| `starrocks.query.port` | `9030` | StarRocks FE MySQL query port, combined with the host(s) from `starrocks.http.url` to build the JDBC URL used for schema checks/DDL. |
| `starrocks.jdbc.url` | *(derived)* | Optional explicit JDBC URL, overriding the URL derived from `starrocks.http.url` + `starrocks.query.port`. |

Example, combined with the existing Debezium unwrap chain:

```properties
transforms=addfield,unwrap
transforms.addfield.type=com.starrocks.connector.kafka.transforms.AddOpFieldForDebeziumRecord
transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
transforms.unwrap.drop.tombstones=true
transforms.unwrap.delete.handling.mode=rewrite

starrocks.schema.evolution=basic
starrocks.query.port=9030
```

## LICENSE
The connector is under the [Apache License 2.0](LICENSE.txt).