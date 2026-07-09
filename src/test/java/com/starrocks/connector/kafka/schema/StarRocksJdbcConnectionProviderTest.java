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
