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
