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

    private static final java.util.regex.Pattern SAFE_IDENTIFIER =
            java.util.regex.Pattern.compile("^[A-Za-z0-9_]+$");

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
        if (!SAFE_IDENTIFIER.matcher(field.name()).matches()) {
            LOG.warn("Skipping schema evolution for field '{}' on table {}.{}: "
                    + "field name is not a safe SQL identifier", field.name(), database, table);
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
            } else if (isReservedColumnNameError(e)) {
                // e.g. `__op`, injected by AddOpFieldForDebeziumRecord for StarRocks primary-key
                // upsert/delete semantics: StarRocks reserves the name for its own Stream Load
                // protocol and will never allow it as a real column. The field still gets sent
                // in every load payload; it's just never a stored column, so evolution for it
                // must be treated as permanently settled rather than retried every batch.
                LOG.warn("Column {} is a StarRocks-reserved name and cannot be added to {}.{}, ignoring: {}",
                        field.name(), database, table, e.getMessage());
            } else {
                throw new ConnectException(
                        "Failed to add column " + field.name() + " to StarRocks table " + database + "." + table, e);
            }
        }
        cachedColumns.add(field.name());
    }

    private boolean isDuplicateColumnError(Exception e) {
        return containsErrorText(e, "duplicate column");
    }

    private boolean isReservedColumnNameError(Exception e) {
        return containsErrorText(e, "system reserved name");
    }

    private boolean containsErrorText(Exception e, String needle) {
        if (containsText(e.getMessage(), needle)) {
            return true;
        }
        Throwable cause = e.getCause();
        return cause != null && containsText(cause.getMessage(), needle);
    }

    private boolean containsText(String message, String needle) {
        return message != null && message.toLowerCase().contains(needle);
    }
}
