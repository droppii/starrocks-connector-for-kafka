# Schema Evolution for StarRocks Kafka Connector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the StarRocks Kafka sink connector detect new fields in incoming struct-schema records and additively `ALTER TABLE ... ADD COLUMN` the target StarRocks table, opt-in and off by default.

**Architecture:** A new `com.starrocks.connector.kafka.schema` package adds a pure Kafka-Connect-`Schema` → StarRocks-DDL-type mapper, a `StarRocksSystemService` introspection interface (JDBC-backed implementation + a test fake), a `SchemaEvolutionManager` that caches per-table column sets and issues additive `ALTER TABLE ADD COLUMN` DDL, and a lazy JDBC connection provider. `StarRocksSinkTask` wires this in behind a new config flag, calling it once per eligible record in `put()` before the existing Stream Load write path, which is otherwise untouched.

**Tech Stack:** Java 8, Kafka Connect API 3.6.0, JUnit 4 (no mocking framework — hand-written test fakes, matching existing repo convention), `com.mysql:mysql-connector-j` for JDBC.

## Global Constraints

- No auto-create-table. The target table must already exist; if it doesn't, fail with a clear error.
- Additive only: `ADD COLUMN` only. No dropped columns, no type changes, no key/distribution column changes.
- Applies only to records with `sinkType == JSON` and a non-null `STRUCT`-typed `record.valueSchema()`. CSV sink format and schemaless JSON are left completely untouched.
- Disabled by default (`starrocks.schema.evolution=none`) — zero behavior change for existing deployments when not configured.
- Follow existing code conventions: Apache 2.0 license header block (copy verbatim from `Util.java`) on every new file, plain JUnit 4 tests with manually constructed objects/fakes (no Mockito — this repo has none), SLF4J logging via `LoggerFactory.getLogger`.

---

### Task 1: `StarRocksTypeMapper` — Kafka Connect Schema → StarRocks DDL type

**Files:**
- Create: `src/main/java/com/starrocks/connector/kafka/schema/StarRocksTypeMapper.java`
- Test: `src/test/java/com/starrocks/connector/kafka/schema/StarRocksTypeMapperTest.java`

**Interfaces:**
- Produces: `public static String StarRocksTypeMapper.mapType(org.apache.kafka.connect.data.Schema fieldSchema)` — returns a StarRocks column DDL type string (e.g. `"INT"`, `"DECIMAL(38,2)"`). Used by `SchemaEvolutionManager` (Task 2) and nothing else.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/starrocks/connector/kafka/schema/StarRocksTypeMapperTest.java`:

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

package com.starrocks.connector.kafka.schema;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Assert;
import org.junit.Test;

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=StarRocksTypeMapperTest`
Expected: FAIL to compile — `StarRocksTypeMapper` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/java/com/starrocks/connector/kafka/schema/StarRocksTypeMapper.java`:

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

package com.starrocks.connector.kafka.schema;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

// Maps a Kafka Connect field Schema to the StarRocks DDL type used when a
// missing column is added via ALTER TABLE ADD COLUMN. Columns added this way
// are always nullable, so no NOT NULL / key-column cases are handled here.
public final class StarRocksTypeMapper {

    private static final String DECIMAL_PRECISION_PARAM = "connect.decimal.precision";
    private static final int DEFAULT_DECIMAL_PRECISION = 38;

    private StarRocksTypeMapper() {
    }

    public static String mapType(Schema schema) {
        String logicalName = schema.name();
        if (Date.LOGICAL_NAME.equals(logicalName)) {
            return "DATE";
        }
        if (Timestamp.LOGICAL_NAME.equals(logicalName)) {
            return "DATETIME";
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=StarRocksTypeMapperTest`
Expected: PASS, 6 tests run, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/starrocks/connector/kafka/schema/StarRocksTypeMapper.java src/test/java/com/starrocks/connector/kafka/schema/StarRocksTypeMapperTest.java
git commit -m "Add StarRocksTypeMapper for schema evolution column type mapping"
```

---

### Task 2: `StarRocksSystemService` interface + `SchemaEvolutionManager`

**Files:**
- Create: `src/main/java/com/starrocks/connector/kafka/schema/StarRocksSystemService.java`
- Create: `src/main/java/com/starrocks/connector/kafka/schema/SchemaEvolutionManager.java`
- Test: `src/test/java/com/starrocks/connector/kafka/schema/SchemaEvolutionManagerTest.java`

**Interfaces:**
- Consumes: `StarRocksTypeMapper.mapType(Schema)` from Task 1.
- Produces:
  - `public interface StarRocksSystemService` with methods `boolean tableExists(String database, String table)`, `java.util.Set<String> getColumns(String database, String table)`, `boolean columnExists(String database, String table, String column)`, `void executeAlter(String ddl)`. Implemented by `JdbcStarRocksSystemService` in Task 4 and by a test fake here.
  - `public class SchemaEvolutionManager` with constructor `SchemaEvolutionManager(StarRocksSystemService systemService, String database)` and method `void evolve(String table, org.apache.kafka.connect.data.Schema valueSchema)`. Used by `StarRocksSinkTask` in Task 6.

- [ ] **Step 1: Write the failing test**

Create `src/main/java/com/starrocks/connector/kafka/schema/StarRocksSystemService.java` first (the interface itself has no logic to test, but `SchemaEvolutionManager`'s test needs it to compile):

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

package com.starrocks.connector.kafka.schema;

import java.util.Set;

// Introspects and mutates StarRocks table structure. JdbcStarRocksSystemService
// is the production implementation; tests use a hand-written fake.
public interface StarRocksSystemService {
    boolean tableExists(String database, String table);

    Set<String> getColumns(String database, String table);

    boolean columnExists(String database, String table, String column);

    void executeAlter(String ddl);
}
```

Now create the failing test `src/test/java/com/starrocks/connector/kafka/schema/SchemaEvolutionManagerTest.java`:

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

package com.starrocks.connector.kafka.schema;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SchemaEvolutionManagerTest {

    private static class FakeStarRocksSystemService implements StarRocksSystemService {
        boolean tableExists = true;
        Set<String> columns = new HashSet<>();
        List<String> executedDdls = new ArrayList<>();
        RuntimeException nextExecuteError;

        @Override
        public boolean tableExists(String database, String table) {
            return tableExists;
        }

        @Override
        public Set<String> getColumns(String database, String table) {
            return new HashSet<>(columns);
        }

        @Override
        public boolean columnExists(String database, String table, String column) {
            return columns.contains(column);
        }

        @Override
        public void executeAlter(String ddl) {
            executedDdls.add(ddl);
            if (nextExecuteError != null) {
                RuntimeException toThrow = nextExecuteError;
                nextExecuteError = null;
                throw toThrow;
            }
            String marker = "ADD COLUMN `";
            int start = ddl.indexOf(marker) + marker.length();
            int end = ddl.indexOf('`', start);
            columns.add(ddl.substring(start, end));
        }
    }

    private FakeStarRocksSystemService fake;
    private SchemaEvolutionManager manager;

    @Before
    public void setUp() {
        fake = new FakeStarRocksSystemService();
        fake.columns.add("id");
        manager = new SchemaEvolutionManager(fake, "test_db");
    }

    @Test
    public void addsMissingColumn() {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();

        manager.evolve("test_table", schema);

        Assert.assertEquals(1, fake.executedDdls.size());
        Assert.assertEquals(
                "ALTER TABLE `test_db`.`test_table` ADD COLUMN `name` STRING NULL",
                fake.executedDdls.get(0));
        Assert.assertTrue(fake.columns.contains("name"));
    }

    @Test
    public void doesNothingWhenNoMissingFields() {
        fake.columns.add("name");
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();

        manager.evolve("test_table", schema);

        Assert.assertTrue(fake.executedDdls.isEmpty());
    }

    @Test
    public void secondEvolveCallUsesCachedColumns() {
        Schema schema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();

        manager.evolve("test_table", schema);
        // If evolve() re-queried columns for a cached table, this would throw.
        fake.tableExists = false;
        manager.evolve("test_table", schema);

        Assert.assertTrue(fake.executedDdls.isEmpty());
    }

    @Test
    public void throwsWhenTableDoesNotExist() {
        fake.tableExists = false;
        Schema schema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();

        try {
            manager.evolve("missing_table", schema);
            Assert.fail("Expected ConnectException");
        } catch (ConnectException e) {
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    public void toleratesDuplicateColumnErrorFromConcurrentAlter() {
        // The fake throws before it would otherwise record "name" as added, simulating
        // a concurrent ALTER from another task instance winning the race. The manager
        // must swallow the error, still mark the column as known, and not retry on the
        // next evolve() call for the same table.
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        fake.nextExecuteError = new RuntimeException("Duplicate column name 'name'");

        manager.evolve("test_table", schema);
        manager.evolve("test_table", schema);

        Assert.assertEquals(1, fake.executedDdls.size());
    }

    @Test
    public void propagatesOtherAlterFailures() {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        fake.nextExecuteError = new RuntimeException("Access denied for user");

        try {
            manager.evolve("test_table", schema);
            Assert.fail("Expected ConnectException");
        } catch (ConnectException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to add column"));
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=SchemaEvolutionManagerTest`
Expected: FAIL to compile — `SchemaEvolutionManager` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/java/com/starrocks/connector/kafka/schema/SchemaEvolutionManager.java`:

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

package com.starrocks.connector.kafka.schema;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Additive-only schema evolution: caches known columns per table and issues
// ALTER TABLE ADD COLUMN for fields present in an incoming record's schema
// but missing from the table. Never drops columns, changes types, or
// creates tables - the table must already exist.
public class SchemaEvolutionManager {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaEvolutionManager.class);

    private final StarRocksSystemService systemService;
    private final String database;
    private final Map<String, Set<String>> tableColumnsCache = new HashMap<>();

    public SchemaEvolutionManager(StarRocksSystemService systemService, String database) {
        this.systemService = systemService;
        this.database = database;
    }

    public void evolve(String table, Schema valueSchema) {
        Set<String> columns = tableColumnsCache.computeIfAbsent(table, this::loadColumns);
        for (Field field : valueSchema.fields()) {
            if (!columns.contains(field.name())) {
                addColumn(table, field, columns);
            }
        }
    }

    private Set<String> loadColumns(String table) {
        if (!systemService.tableExists(database, table)) {
            throw new ConnectException(
                    "StarRocks table '" + database + "." + table + "' does not exist. "
                    + "Schema evolution requires the table to already exist; auto-create is not supported.");
        }
        return new HashSet<>(systemService.getColumns(database, table));
    }

    private void addColumn(String table, Field field, Set<String> cachedColumns) {
        if (systemService.columnExists(database, table, field.name())) {
            cachedColumns.add(field.name());
            return;
        }
        String columnType = StarRocksTypeMapper.mapType(field.schema());
        String ddl = String.format(
                "ALTER TABLE `%s`.`%s` ADD COLUMN `%s` %s NULL",
                database, table, field.name(), columnType);
        try {
            systemService.executeAlter(ddl);
            LOG.info("Added column {} ({}) to StarRocks table {}.{}", field.name(), columnType, database, table);
        } catch (Exception e) {
            if (isDuplicateColumnError(e)) {
                LOG.warn("Column {} already exists on {}.{}, ignoring: {}", field.name(), database, table, e.getMessage());
            } else {
                throw new ConnectException(
                        "Failed to add column " + field.name() + " to StarRocks table " + database + "." + table, e);
            }
        }
        cachedColumns.add(field.name());
    }

    private boolean isDuplicateColumnError(Exception e) {
        String message = e.getMessage();
        return message != null && message.toLowerCase().contains("duplicate column");
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=SchemaEvolutionManagerTest`
Expected: PASS, 6 tests run, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/starrocks/connector/kafka/schema/StarRocksSystemService.java src/main/java/com/starrocks/connector/kafka/schema/SchemaEvolutionManager.java src/test/java/com/starrocks/connector/kafka/schema/SchemaEvolutionManagerTest.java
git commit -m "Add SchemaEvolutionManager with additive ALTER TABLE ADD COLUMN logic"
```

---

### Task 3: `StarRocksJdbcConnectionProvider`

**Files:**
- Create: `src/main/java/com/starrocks/connector/kafka/schema/StarRocksJdbcConnectionProvider.java`
- Test: `src/test/java/com/starrocks/connector/kafka/schema/StarRocksJdbcConnectionProviderTest.java`

**Interfaces:**
- Produces:
  - `public static String StarRocksJdbcConnectionProvider.buildJdbcUrl(String httpUrl, String queryPort, String explicitJdbcUrl, String database)` — pure function, used by `StarRocksSinkTask` (Task 6).
  - `public StarRocksJdbcConnectionProvider(String jdbcUrl, String username, String password)` constructor, `java.sql.Connection getOrEstablishConnection()`, `void close()` — used by `JdbcStarRocksSystemService` (Task 4) and `StarRocksSinkTask` (Task 6).

Only `buildJdbcUrl` is unit tested here — it's a pure function. `getOrEstablishConnection()`/`close()` require a real JDBC driver and StarRocks/MySQL-protocol server, so they're exercised only via manual/integration verification (see Task 4).

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/starrocks/connector/kafka/schema/StarRocksJdbcConnectionProviderTest.java`:

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

package com.starrocks.connector.kafka.schema;

import org.junit.Assert;
import org.junit.Test;

public class StarRocksJdbcConnectionProviderTest {

    @Test
    public void derivesJdbcUrlFromFirstHostInCommaSeparatedHttpUrl() {
        String jdbcUrl = StarRocksJdbcConnectionProvider.buildJdbcUrl(
                "192.168.1.1:8030,192.168.1.2:8030", "9030", null, "test_db");
        Assert.assertEquals("jdbc:mysql://192.168.1.1:9030/test_db", jdbcUrl);
    }

    @Test
    public void derivesJdbcUrlFromSingleHttpUrl() {
        String jdbcUrl = StarRocksJdbcConnectionProvider.buildJdbcUrl(
                "fe.example.com:8030", "9030", null, "test_db");
        Assert.assertEquals("jdbc:mysql://fe.example.com:9030/test_db", jdbcUrl);
    }

    @Test
    public void explicitJdbcUrlOverridesDerivedUrl() {
        String jdbcUrl = StarRocksJdbcConnectionProvider.buildJdbcUrl(
                "192.168.1.1:8030", "9030", "jdbc:mysql://custom-host:9999/test_db", "test_db");
        Assert.assertEquals("jdbc:mysql://custom-host:9999/test_db", jdbcUrl);
    }

    @Test
    public void blankExplicitJdbcUrlIsIgnored() {
        String jdbcUrl = StarRocksJdbcConnectionProvider.buildJdbcUrl(
                "192.168.1.1:8030", "9030", "   ", "test_db");
        Assert.assertEquals("jdbc:mysql://192.168.1.1:9030/test_db", jdbcUrl);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=StarRocksJdbcConnectionProviderTest`
Expected: FAIL to compile — `StarRocksJdbcConnectionProvider` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/java/com/starrocks/connector/kafka/schema/StarRocksJdbcConnectionProvider.java`:

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

package com.starrocks.connector.kafka.schema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Lazily opens and holds a single JDBC (MySQL-protocol) connection to the
// StarRocks FE query port, reconnecting if the connection was closed.
public class StarRocksJdbcConnectionProvider {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksJdbcConnectionProvider.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private Connection connection;

    public StarRocksJdbcConnectionProvider(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    // httpUrl may be a comma-separated list of host:port entries (matching
    // starrocks.http.url); only the host of the first entry is used, paired
    // with the FE query port, unless explicitJdbcUrl overrides both.
    public static String buildJdbcUrl(String httpUrl, String queryPort, String explicitJdbcUrl, String database) {
        if (explicitJdbcUrl != null && !explicitJdbcUrl.trim().isEmpty()) {
            return explicitJdbcUrl;
        }
        String firstHostPort = httpUrl.split(",")[0].trim();
        String host = firstHostPort.split(":")[0];
        return "jdbc:mysql://" + host + ":" + queryPort + "/" + database;
    }

    public synchronized Connection getOrEstablishConnection() {
        try {
            if (connection == null || connection.isClosed()) {
                connection = DriverManager.getConnection(jdbcUrl, username, password);
            }
            return connection;
        } catch (SQLException e) {
            throw new ConnectException("Failed to establish JDBC connection to StarRocks at " + jdbcUrl, e);
        }
    }

    public synchronized void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            LOG.warn("Failed to cleanly close StarRocks JDBC connection", e);
        } finally {
            connection = null;
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=StarRocksJdbcConnectionProviderTest`
Expected: PASS, 4 tests run, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/starrocks/connector/kafka/schema/StarRocksJdbcConnectionProvider.java src/test/java/com/starrocks/connector/kafka/schema/StarRocksJdbcConnectionProviderTest.java
git commit -m "Add StarRocksJdbcConnectionProvider for lazy JDBC connectivity"
```

---

### Task 4: `JdbcStarRocksSystemService` + JDBC driver dependency

**Files:**
- Modify: `pom.xml`
- Create: `src/main/java/com/starrocks/connector/kafka/schema/JdbcStarRocksSystemService.java`

**Interfaces:**
- Consumes: `StarRocksSystemService` interface (Task 2), `StarRocksJdbcConnectionProvider.getOrEstablishConnection()` (Task 3).
- Produces: `public class JdbcStarRocksSystemService implements StarRocksSystemService`, constructor `JdbcStarRocksSystemService(StarRocksJdbcConnectionProvider connectionProvider)`. Used by `StarRocksSinkTask` (Task 6).

This class is a thin JDBC wrapper around `information_schema` queries and an `ALTER` execution — there is no branching logic worth a unit test, and testing it for real requires a live StarRocks (or MySQL-protocol-compatible) server, which this repo's test suite does not have. Per the design doc, this is verified manually against a real cluster rather than with an automated test, consistent with the rest of this connector's test depth (no integration harness exists today).

- [ ] **Step 1: Add the JDBC driver dependency**

In `pom.xml`, inside the `<dependencies>` block, add (after the `starrocks-stream-load-sdk` dependency, before the `debezium-core` dependency):

```xml
        <dependency>
            <groupId>com.mysql</groupId>
            <artifactId>mysql-connector-j</artifactId>
            <version>8.0.33</version>
        </dependency>
```

- [ ] **Step 2: Verify the project still builds**

Run: `mvn -q compile`
Expected: `BUILD SUCCESS`, no output (new dependency resolves and downloads cleanly).

- [ ] **Step 3: Write the implementation**

Create `src/main/java/com/starrocks/connector/kafka/schema/JdbcStarRocksSystemService.java`:

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

package com.starrocks.connector.kafka.schema;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;

// JDBC-backed StarRocksSystemService, querying information_schema on the FE
// query port for table/column introspection and executing ALTER statements
// directly.
public class JdbcStarRocksSystemService implements StarRocksSystemService {
    private static final String TABLE_EXISTS_SQL =
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
    private static final String GET_COLUMNS_SQL =
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
    private static final String COLUMN_EXISTS_SQL =
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?";

    private final StarRocksJdbcConnectionProvider connectionProvider;

    public JdbcStarRocksSystemService(StarRocksJdbcConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    @Override
    public boolean tableExists(String database, String table) {
        try (PreparedStatement ps = connectionProvider.getOrEstablishConnection().prepareStatement(TABLE_EXISTS_SQL)) {
            ps.setString(1, database);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new ConnectException("Failed to check if StarRocks table " + database + "." + table + " exists", e);
        }
    }

    @Override
    public Set<String> getColumns(String database, String table) {
        Set<String> columns = new HashSet<>();
        try (PreparedStatement ps = connectionProvider.getOrEstablishConnection().prepareStatement(GET_COLUMNS_SQL)) {
            ps.setString(1, database);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    columns.add(rs.getString(1));
                }
            }
            return columns;
        } catch (SQLException e) {
            throw new ConnectException("Failed to fetch columns for StarRocks table " + database + "." + table, e);
        }
    }

    @Override
    public boolean columnExists(String database, String table, String column) {
        try (PreparedStatement ps = connectionProvider.getOrEstablishConnection().prepareStatement(COLUMN_EXISTS_SQL)) {
            ps.setString(1, database);
            ps.setString(2, table);
            ps.setString(3, column);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new ConnectException(
                    "Failed to check if column " + column + " exists on " + database + "." + table, e);
        }
    }

    @Override
    public void executeAlter(String ddl) {
        try (Statement statement = connectionProvider.getOrEstablishConnection().createStatement()) {
            statement.execute(ddl);
        } catch (SQLException e) {
            throw new ConnectException("Failed to execute DDL against StarRocks: " + ddl, e);
        }
    }
}
```

- [ ] **Step 4: Verify the project builds with the new class**

Run: `mvn -q compile`
Expected: `BUILD SUCCESS`, no output.

- [ ] **Step 5: Commit**

```bash
git add pom.xml src/main/java/com/starrocks/connector/kafka/schema/JdbcStarRocksSystemService.java
git commit -m "Add JdbcStarRocksSystemService and mysql-connector-j dependency"
```

---

### Task 5: New connector config keys

**Files:**
- Modify: `src/main/java/com/starrocks/connector/kafka/StarRocksSinkConnectorConfig.java`
- Modify: `src/main/java/com/starrocks/connector/kafka/StarRocksSinkConnector.java`
- Test: `src/test/java/com/starrocks/connector/kafka/StarRocksSinkConnectorConfigTest.java`

**Interfaces:**
- Produces: new public constants on `StarRocksSinkConnectorConfig`: `STARROCKS_SCHEMA_EVOLUTION`, `SCHEMA_EVOLUTION_NONE`, `SCHEMA_EVOLUTION_BASIC`, `STARROCKS_QUERY_PORT`, `STARROCKS_JDBC_URL`. Used by `StarRocksSinkTask` (Task 6).

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/starrocks/connector/kafka/StarRocksSinkConnectorConfigTest.java`:

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

package com.starrocks.connector.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.Assert;
import org.junit.Test;

public class StarRocksSinkConnectorConfigTest {

    @Test
    public void schemaEvolutionDefaultsToNone() {
        ConfigDef configDef = StarRocksSinkConnectorConfig.newConfigDef();
        ConfigDef.ConfigKey key = configDef.configKeys().get(StarRocksSinkConnectorConfig.STARROCKS_SCHEMA_EVOLUTION);
        Assert.assertEquals("none", key.defaultValue);
    }

    @Test
    public void schemaEvolutionRejectsUnknownValue() {
        ConfigDef configDef = StarRocksSinkConnectorConfig.newConfigDef();
        ConfigDef.ConfigKey key = configDef.configKeys().get(StarRocksSinkConnectorConfig.STARROCKS_SCHEMA_EVOLUTION);
        try {
            key.validator.ensureValid(StarRocksSinkConnectorConfig.STARROCKS_SCHEMA_EVOLUTION, "invalid-mode");
            Assert.fail("Expected a validation exception for an invalid schema evolution mode");
        } catch (Exception expected) {
            // ConfigDef.ValidString throws ConfigException; any exception here confirms validation ran.
        }
    }

    @Test
    public void queryPortDefaultsTo9030() {
        ConfigDef configDef = StarRocksSinkConnectorConfig.newConfigDef();
        ConfigDef.ConfigKey key = configDef.configKeys().get(StarRocksSinkConnectorConfig.STARROCKS_QUERY_PORT);
        Assert.assertEquals(9030, key.defaultValue);
    }

    @Test
    public void jdbcUrlHasNoDefault() {
        ConfigDef configDef = StarRocksSinkConnectorConfig.newConfigDef();
        ConfigDef.ConfigKey key = configDef.configKeys().get(StarRocksSinkConnectorConfig.STARROCKS_JDBC_URL);
        Assert.assertNull(key.defaultValue);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=StarRocksSinkConnectorConfigTest`
Expected: FAIL to compile — the new constants don't exist yet.

- [ ] **Step 3: Add the new config constants and ConfigDef entries**

In `src/main/java/com/starrocks/connector/kafka/StarRocksSinkConnectorConfig.java`, replace:

```java
    // Data writing to StarRocks may fail due to a network fault or a short time StarRcoks restart.
    // For precommit, the connector detects if an error has occurred and writes the failed data to the SR again.
    // This configuration controls the number of failed retries. The default value is 3. -1 indicates unlimited retry.
    public static final String SINK_MAXRETRIES = "sink.maxretries";

    public static final String[] mustRequiredConfigs = {
```

with:

```java
    // Data writing to StarRocks may fail due to a network fault or a short time StarRcoks restart.
    // For precommit, the connector detects if an error has occurred and writes the failed data to the SR again.
    // This configuration controls the number of failed retries. The default value is 3. -1 indicates unlimited retry.
    public static final String SINK_MAXRETRIES = "sink.maxretries";
    // Additive schema evolution mode for struct-schema (Debezium/Avro/JSON-with-schema) records:
    // "none" (default, disabled) or "basic" (ALTER TABLE ADD COLUMN for fields missing from the target table).
    // The target table must already exist; auto-create is not supported.
    public static final String STARROCKS_SCHEMA_EVOLUTION = "starrocks.schema.evolution";
    public static final String SCHEMA_EVOLUTION_NONE = "none";
    public static final String SCHEMA_EVOLUTION_BASIC = "basic";
    // StarRocks FE MySQL query port, used together with the host(s) from STARROCKS_LOAD_URL
    // to build the JDBC URL used for schema evolution, unless STARROCKS_JDBC_URL is set.
    public static final String STARROCKS_QUERY_PORT = "starrocks.query.port";
    // Optional explicit JDBC URL for schema evolution, overriding the URL derived from
    // STARROCKS_LOAD_URL and STARROCKS_QUERY_PORT.
    public static final String STARROCKS_JDBC_URL = "starrocks.jdbc.url";

    public static final String[] mustRequiredConfigs = {
```

Then replace the end of `newConfigDef()`:

```java
                ).define(
                        SINK_MAXRETRIES,
                        ConfigDef.Type.LONG,
                        3,
                        ConfigDef.Range.between(-1, Long.MAX_VALUE),
                        ConfigDef.Importance.LOW,
                        "number of Stream Load retries after a stream load failure",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        SINK_MAXRETRIES
                );
```

with:

```java
                ).define(
                        SINK_MAXRETRIES,
                        ConfigDef.Type.LONG,
                        3,
                        ConfigDef.Range.between(-1, Long.MAX_VALUE),
                        ConfigDef.Importance.LOW,
                        "number of Stream Load retries after a stream load failure",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        SINK_MAXRETRIES
                ).define(
                        STARROCKS_SCHEMA_EVOLUTION,
                        ConfigDef.Type.STRING,
                        SCHEMA_EVOLUTION_NONE,
                        ConfigDef.ValidString.in(SCHEMA_EVOLUTION_NONE, SCHEMA_EVOLUTION_BASIC),
                        ConfigDef.Importance.LOW,
                        "additive schema evolution mode: none (default, disabled) or basic (ALTER TABLE ADD COLUMN for missing fields)",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        STARROCKS_SCHEMA_EVOLUTION
                ).define(
                        STARROCKS_QUERY_PORT,
                        ConfigDef.Type.INT,
                        9030,
                        ConfigDef.Range.between(1, 65535),
                        ConfigDef.Importance.LOW,
                        "starrocks FE MySQL query port, used to build the JDBC URL for schema evolution",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        STARROCKS_QUERY_PORT
                ).define(
                        STARROCKS_JDBC_URL,
                        ConfigDef.Type.STRING,
                        null,
                        null,
                        ConfigDef.Importance.LOW,
                        "optional explicit JDBC URL for schema evolution; overrides the URL derived from starrocks.http.url and starrocks.query.port",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        STARROCKS_JDBC_URL
                );
```

- [ ] **Step 4: Add defaults in `StarRocksSinkConnector.validate()`**

In `src/main/java/com/starrocks/connector/kafka/StarRocksSinkConnector.java`, replace:

```java
        if (!connectorConfigs.containsKey(SINK_MAXRETRIES)) {
            connectorConfigs.put(SINK_MAXRETRIES, "3");
        }
        Config result = super.validate(connectorConfigs);
```

with:

```java
        if (!connectorConfigs.containsKey(SINK_MAXRETRIES)) {
            connectorConfigs.put(SINK_MAXRETRIES, "3");
        }
        if (!connectorConfigs.containsKey(STARROCKS_SCHEMA_EVOLUTION)) {
            connectorConfigs.put(STARROCKS_SCHEMA_EVOLUTION, StarRocksSinkConnectorConfig.SCHEMA_EVOLUTION_NONE);
        }
        if (!connectorConfigs.containsKey(STARROCKS_QUERY_PORT)) {
            connectorConfigs.put(STARROCKS_QUERY_PORT, "9030");
        }
        Config result = super.validate(connectorConfigs);
```

(`STARROCKS_SCHEMA_EVOLUTION` and `STARROCKS_QUERY_PORT` are already in scope via the existing `import static com.starrocks.connector.kafka.StarRocksSinkConnectorConfig.*;`.)

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn test -Dtest=StarRocksSinkConnectorConfigTest`
Expected: PASS, 4 tests run, 0 failures.

- [ ] **Step 6: Run the full existing test suite to check for regressions**

Run: `mvn test`
Expected: `BUILD SUCCESS`, all previously-passing tests still pass.

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/starrocks/connector/kafka/StarRocksSinkConnectorConfig.java src/main/java/com/starrocks/connector/kafka/StarRocksSinkConnector.java src/test/java/com/starrocks/connector/kafka/StarRocksSinkConnectorConfigTest.java
git commit -m "Add starrocks.schema.evolution, starrocks.query.port, starrocks.jdbc.url config"
```

---

### Task 6: Wire schema evolution into `StarRocksSinkTask`

**Files:**
- Modify: `src/main/java/com/starrocks/connector/kafka/StarRocksSinkTask.java`
- Modify: `src/test/java/com/starrocks/connector/kafka/StarRocksSinkTaskTest.java`

**Interfaces:**
- Consumes: `SchemaEvolutionManager` (Task 2), `StarRocksJdbcConnectionProvider` (Task 3), `JdbcStarRocksSystemService` (Task 4), `StarRocksSinkConnectorConfig.STARROCKS_SCHEMA_EVOLUTION` / `SCHEMA_EVOLUTION_BASIC` / `STARROCKS_QUERY_PORT` / `STARROCKS_JDBC_URL` (Task 5).
- Produces: `static boolean StarRocksSinkTask.isEligibleForSchemaEvolution(boolean schemaEvolutionEnabled, SinkType sinkType, SinkRecord record)` — package-visible pure function, directly unit tested (avoids needing a live `loadManager`/`StreamLoadManagerV2` to exercise `put()` end-to-end, matching how this test file already avoids calling `start()`/`put()`).

- [ ] **Step 1: Write the failing test**

In `src/test/java/com/starrocks/connector/kafka/StarRocksSinkTaskTest.java`, add this test method inside the `StarRocksSinkTaskTest` class (e.g. right after `testNullRecord`, before the closing brace):

```java
    @Test
    public void testIsEligibleForSchemaEvolution() {
        Schema structSchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
        Struct structValue = new Struct(structSchema).put("id", 1);
        SinkRecord structRecord = new SinkRecord("t", 0, null, null, structSchema, structValue, 0);
        SinkRecord schemalessRecord = new SinkRecord("t", 0, null, null, null, "{}", 0);

        Assert.assertTrue(StarRocksSinkTask.isEligibleForSchemaEvolution(true, StarRocksSinkTask.SinkType.JSON, structRecord));
        Assert.assertFalse(StarRocksSinkTask.isEligibleForSchemaEvolution(false, StarRocksSinkTask.SinkType.JSON, structRecord));
        Assert.assertFalse(StarRocksSinkTask.isEligibleForSchemaEvolution(true, StarRocksSinkTask.SinkType.CSV, structRecord));
        Assert.assertFalse(StarRocksSinkTask.isEligibleForSchemaEvolution(true, StarRocksSinkTask.SinkType.JSON, schemalessRecord));
        Assert.assertFalse(StarRocksSinkTask.isEligibleForSchemaEvolution(true, StarRocksSinkTask.SinkType.JSON, null));
    }
```

(`Schema`, `SchemaBuilder`, `Struct`, `SinkRecord`, `Assert` are already imported at the top of this file.)

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=StarRocksSinkTaskTest#testIsEligibleForSchemaEvolution`
Expected: FAIL to compile — `isEligibleForSchemaEvolution` does not exist yet.

- [ ] **Step 3: Add imports to `StarRocksSinkTask.java`**

Replace:

```java
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.starrocks.connector.kafka.json.DecimalFormat;
import com.starrocks.connector.kafka.json.JsonConverter;
import com.starrocks.connector.kafka.json.JsonConverterConfig;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
```

with:

```java
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.starrocks.connector.kafka.json.DecimalFormat;
import com.starrocks.connector.kafka.json.JsonConverter;
import com.starrocks.connector.kafka.json.JsonConverterConfig;
import com.starrocks.connector.kafka.schema.JdbcStarRocksSystemService;
import com.starrocks.connector.kafka.schema.SchemaEvolutionManager;
import com.starrocks.connector.kafka.schema.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.kafka.schema.StarRocksSystemService;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
```

- [ ] **Step 4: Add new fields**

Replace:

```java
    private long buffMaxbytes;
    private long bufferFlushInterval;
    private long currentBufferBytes = 0;
    private long lastFlushTime = 0;
```

with:

```java
    private long buffMaxbytes;
    private long bufferFlushInterval;
    private long currentBufferBytes = 0;
    private long lastFlushTime = 0;

    private boolean schemaEvolutionEnabled;
    private SchemaEvolutionManager schemaEvolutionManager;
    private StarRocksJdbcConnectionProvider jdbcConnectionProvider;
```

- [ ] **Step 5: Initialize schema evolution in `start()`**

Replace:

```java
    @Override
    public void start(Map<String, String> props) {
        LOG.info("Starrocks sink task starting. version is " + Util.VERSION);
        this.props = props;
        loadProperties = buildLoadProperties();
        loadManager = buildLoadManager(loadProperties);
        topic2Table = getTopicToTableMap(props);
        jsonConverter = createJsonConverter();
        maxRetryTimes = Long.parseLong(props.getOrDefault(StarRocksSinkConnectorConfig.SINK_MAXRETRIES, "3"));
        LOG.info("Starrocks sink task started. version is " + Util.VERSION);
    }
```

with:

```java
    @Override
    public void start(Map<String, String> props) {
        LOG.info("Starrocks sink task starting. version is " + Util.VERSION);
        this.props = props;
        loadProperties = buildLoadProperties();
        loadManager = buildLoadManager(loadProperties);
        topic2Table = getTopicToTableMap(props);
        jsonConverter = createJsonConverter();
        maxRetryTimes = Long.parseLong(props.getOrDefault(StarRocksSinkConnectorConfig.SINK_MAXRETRIES, "3"));
        initSchemaEvolution();
        LOG.info("Starrocks sink task started. version is " + Util.VERSION);
    }

    private void initSchemaEvolution() {
        String schemaEvolutionMode = props.getOrDefault(
                StarRocksSinkConnectorConfig.STARROCKS_SCHEMA_EVOLUTION,
                StarRocksSinkConnectorConfig.SCHEMA_EVOLUTION_NONE);
        schemaEvolutionEnabled = StarRocksSinkConnectorConfig.SCHEMA_EVOLUTION_BASIC.equalsIgnoreCase(schemaEvolutionMode);
        if (!schemaEvolutionEnabled) {
            return;
        }
        String queryPort = props.getOrDefault(StarRocksSinkConnectorConfig.STARROCKS_QUERY_PORT, "9030");
        String explicitJdbcUrl = props.get(StarRocksSinkConnectorConfig.STARROCKS_JDBC_URL);
        String jdbcUrl = StarRocksJdbcConnectionProvider.buildJdbcUrl(
                props.get(StarRocksSinkConnectorConfig.STARROCKS_LOAD_URL), queryPort, explicitJdbcUrl, database);
        String username = props.get(StarRocksSinkConnectorConfig.STARROCKS_USERNAME);
        String password = props.get(StarRocksSinkConnectorConfig.STARROCKS_PASSWORD);
        jdbcConnectionProvider = new StarRocksJdbcConnectionProvider(jdbcUrl, username, password);
        StarRocksSystemService systemService = new JdbcStarRocksSystemService(jdbcConnectionProvider);
        schemaEvolutionManager = new SchemaEvolutionManager(systemService, database);
        LOG.info("Starrocks schema evolution enabled, jdbcUrl={}", jdbcUrl);
    }
```

(`database` is already populated by `buildLoadProperties()`, which runs earlier in `start()`.)

- [ ] **Step 6: Add the eligibility helper and call it from `put()`**

Add this method near `getTableFromTopic` (e.g. directly after it):

```java
    static boolean isEligibleForSchemaEvolution(boolean schemaEvolutionEnabled, SinkType sinkType, SinkRecord record) {
        return schemaEvolutionEnabled
                && sinkType == SinkType.JSON
                && record != null
                && record.valueSchema() != null
                && record.valueSchema().type() == Schema.Type.STRUCT;
    }
```

Then in `put()`, replace:

```java
            LOG.debug("Received record: " + record.toString());

            String topic = record.topic();
            // The sdk does not provide the ability to clean up exceptions, that is to say, according to the current implementation of the SDK,
            // after an Exception occurs, the SDK must be re-initialized, which is based on flink:
            // 1. When an exception occurs, put will continue to fail, at which point we do nothing and let put move forward.
            // 2. Because the framework periodically calls the preCommit method, we can sense if an exception has occurred in
            //    this method. In the case of an exception, we initialize the new SDK and then throw an exception to the framework.
            //    In this case, the framework repulls the data from the commit point and then moves forward.
            String row = getRecordFromSinkRecord(record);
            LOG.debug("Parsed row: " + row);
            if (row == null) {
                continue;
            }
            try {
                loadManager.write(null, database, getTableFromTopic(topic), row);
                currentBufferBytes += row.getBytes().length;
```

with:

```java
            LOG.debug("Received record: " + record.toString());

            String topic = record.topic();
            String table = getTableFromTopic(topic);
            if (isEligibleForSchemaEvolution(schemaEvolutionEnabled, sinkType, record)) {
                schemaEvolutionManager.evolve(table, record.valueSchema());
            }
            // The sdk does not provide the ability to clean up exceptions, that is to say, according to the current implementation of the SDK,
            // after an Exception occurs, the SDK must be re-initialized, which is based on flink:
            // 1. When an exception occurs, put will continue to fail, at which point we do nothing and let put move forward.
            // 2. Because the framework periodically calls the preCommit method, we can sense if an exception has occurred in
            //    this method. In the case of an exception, we initialize the new SDK and then throw an exception to the framework.
            //    In this case, the framework repulls the data from the commit point and then moves forward.
            String row = getRecordFromSinkRecord(record);
            LOG.debug("Parsed row: " + row);
            if (row == null) {
                continue;
            }
            try {
                loadManager.write(null, database, table, row);
                currentBufferBytes += row.getBytes().length;
```

- [ ] **Step 7: Close the JDBC connection in `stop()`**

Replace:

```java
    @Override
    public void stop() {
        if (loadManager != null) {
            loadManager.close();
        }
        if (jsonConverter != null) {
            jsonConverter.close();
        }
        LOG.info("Starrocks sink task stopped. version is " + Util.VERSION);
    }
```

with:

```java
    @Override
    public void stop() {
        if (loadManager != null) {
            loadManager.close();
        }
        if (jsonConverter != null) {
            jsonConverter.close();
        }
        if (jdbcConnectionProvider != null) {
            jdbcConnectionProvider.close();
        }
        LOG.info("Starrocks sink task stopped. version is " + Util.VERSION);
    }
```

- [ ] **Step 8: Run test to verify it passes**

Run: `mvn test -Dtest=StarRocksSinkTaskTest`
Expected: PASS, all tests in the class pass (existing tests plus the new one).

- [ ] **Step 9: Run the full test suite**

Run: `mvn test`
Expected: `BUILD SUCCESS`, all tests across the project pass.

- [ ] **Step 10: Commit**

```bash
git add src/main/java/com/starrocks/connector/kafka/StarRocksSinkTask.java src/test/java/com/starrocks/connector/kafka/StarRocksSinkTaskTest.java
git commit -m "Wire schema evolution into StarRocksSinkTask.put()"
```

---

### Task 7: README documentation

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add a schema evolution section**

In `README.md`, after the "How to build" section and before "LICENSE", add:

```markdown
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
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "Document schema evolution configuration in README"
```
