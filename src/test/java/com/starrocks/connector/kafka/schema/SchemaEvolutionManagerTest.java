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

    @Test
    public void toleratesDuplicateColumnErrorWrappedAsCause() {
        // Reproduces how the real JdbcStarRocksSystemService.executeAlter wraps the
        // driver's SQLException: the outer ConnectException's own message is just the
        // DDL text, and the real "duplicate column" text only survives on the cause.
        // The fake-based test above throws the raw message directly with no wrapping,
        // so it would not have caught a regression here.
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        fake.nextExecuteError = new ConnectException(
                "Failed to execute DDL against StarRocks: ALTER TABLE ...",
                new RuntimeException("Duplicate column name 'name'"));

        manager.evolve("test_table", schema);
        manager.evolve("test_table", schema);

        Assert.assertEquals(1, fake.executedDdls.size());
    }

    @Test
    public void skipsFieldWithUnsafeIdentifierName() {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("bad`name", Schema.STRING_SCHEMA)
                .build();

        manager.evolve("test_table", schema);

        Assert.assertTrue(fake.executedDdls.isEmpty());
    }
}
