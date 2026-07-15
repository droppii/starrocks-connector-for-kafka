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

package com.droppii.connector.kafka.schema;

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
