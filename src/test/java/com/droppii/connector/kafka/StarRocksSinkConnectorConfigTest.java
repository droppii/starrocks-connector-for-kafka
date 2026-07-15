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

package com.droppii.connector.kafka;

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
