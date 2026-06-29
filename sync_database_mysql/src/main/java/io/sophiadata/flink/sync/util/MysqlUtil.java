/*
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

package io.sophiadata.flink.sync.util;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.mysql.database.catalog.MySqlCatalog;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/** (@SophiaData) (@date 2023/5/31 19:02). */
public class MysqlUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlUtil.class);
    private static final String DRIVER_NAME = "com.mysql.cj.jdbc.Driver";

    /**
     * MySQL {@code NOT NULL TIMESTAMP} columns require an explicit default in {@code
     * STRICT_TRANS_TABLES} mode; CDC binlog emits {@code 1970-01-01 09:00:00} (CST epoch) when
     * upstream value is null, so we materialize that as the default for sink tables.
     */
    private static final String MYSQL_TIMESTAMP_DEFAULT = " default '1970-01-01 09:00:00'";

    public static Connection getConnection(String sinkUrl, String sinkUsername, String sinkPassword)
            throws ClassNotFoundException, SQLException {
        try {
            Class.forName(DRIVER_NAME);
            return DriverManager.getConnection(sinkUrl, sinkUsername, sinkPassword);
        } catch (ClassNotFoundException e) {
            LOG.error("驱动未加载，请检查: ", e);
            throw e;
        } catch (SQLException e) {
            LOG.error("sql 异常: ", e);
            throw e;
        }
    }

    public static void executeSql(Connection connection, String createTable) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(createTable)) {
            ps.execute();
        } catch (SQLException e) {
            LOG.error("建表异常: ", e);
            throw e;
        }
    }

    /**
     * Convenience for one-shot DDL execution: open a connection, run the statement, and close the
     * connection. Prefer {@link #executeSql(Connection, String)} when the caller already owns a
     * connection.
     */
    public static void executeSqlAndClose(
            String url, String username, String password, String createTable)
            throws SQLException, ClassNotFoundException {
        try (Connection connection = getConnection(url, username, password)) {
            executeSql(connection, createTable);
        }
    }

    public static String createTable(
            String sinkTableName,
            String[] fieldNames,
            DataType[] fieldDataTypes,
            List<String> primaryKeys) {
        if (!isValidIdentifier(sinkTableName)) {
            throw new IllegalArgumentException("Invalid table name: " + sinkTableName);
        }
        if (fieldNames.length != fieldDataTypes.length) {
            throw new IllegalArgumentException(
                    "fieldNames/fieldDataTypes length mismatch: "
                            + fieldNames.length
                            + " vs "
                            + fieldDataTypes.length);
        }

        StringBuilder stmt = new StringBuilder();
        stmt.append("create table if not exists `").append(sinkTableName).append("` (\n");
        for (int i = 0; i < fieldNames.length; i++) {
            String column = fieldNames[i];
            if (!isValidIdentifier(column)) {
                throw new IllegalArgumentException("Invalid column name: " + column);
            }
            DataType dataType = fieldDataTypes[i];
            stmt.append("  `").append(column).append("` ").append(dataType.toString());
            if (needsTimestampDefault(dataType)) {
                stmt.append(MYSQL_TIMESTAMP_DEFAULT);
            }
            stmt.append(",\n");
        }
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw new IllegalArgumentException(
                    "primaryKeys must be non-empty (CDC requires a primary key)");
        }
        for (String pk : primaryKeys) {
            if (!isValidIdentifier(pk)) {
                throw new IllegalArgumentException("Invalid primary key: " + pk);
            }
        }
        stmt.append("  PRIMARY KEY (").append(String.join(",", primaryKeys)).append(")\n)");

        String createSql = stmt.toString();
        LOG.debug("Generated SQL: {}", createSql);
        return createSql;
    }

    private static boolean needsTimestampDefault(DataType dataType) {
        // Only TIMESTAMP without time zone needs the 1970 default; binary / lob types never do.
        return dataType.getLogicalType() instanceof TimestampType
                && !(dataType.getLogicalType() instanceof VarBinaryType);
    }

    public static MySqlCatalog useMysqlCatalog(ParameterTool params) {
        String username = ParameterUtil.username(params);
        String password = ParameterUtil.password(params);
        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException(
                    "username is required (set --username or env MYSQL_USERNAME)");
        }
        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException(
                    "password is required (set --password or env MYSQL_PASSWORD)");
        }
        return new MySqlCatalog(
                Thread.currentThread().getContextClassLoader(),
                "mysql-catalog",
                ParameterUtil.databaseName(params),
                username,
                password,
                String.format(
                        "jdbc:mysql://%s:%d",
                        ParameterUtil.hostname(params), ParameterUtil.port(params)));
    }

    private static boolean isValidIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return false;
        }
        return identifier.matches("[a-zA-Z0-9_]+");
    }
}
