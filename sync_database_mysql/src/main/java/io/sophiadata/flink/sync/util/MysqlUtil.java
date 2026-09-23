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

import org.apache.flink.table.types.DataType;

import io.sophiadata.flink.compat.ParameterTool;
import org.apache.commons.lang3.StringUtils;
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

    /** Default applied to MySQL {@code TIMESTAMP} columns, which require an explicit value. */
    private static final String TIMESTAMP_DEFAULT = " default '1970-01-01 09:00:00'";

    public static Connection getConnection(String sinkUrl, String sinkUsername, String sinkPassword)
            throws ClassNotFoundException, SQLException {
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            LOG.error("MySQL driver not on the classpath: {}", DRIVER_NAME, e);
            throw e;
        }
        try {
            return DriverManager.getConnection(sinkUrl, sinkUsername, sinkPassword);
        } catch (SQLException e) {
            LOG.error("Failed to connect to {}", sinkUrl, e);
            throw e;
        }
    }

    /**
     * Executes a statement and closes it, leaving the connection open for the caller.
     *
     * @param connection an open connection
     * @param sql the statement to run
     * @throws SQLException if execution fails
     */
    public static void executeSql(Connection connection, String sql) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.execute();
        } catch (SQLException e) {
            LOG.error("Failed to execute: {}", sql, e);
            throw e;
        }
    }

    /**
     * Builds a MySQL {@code CREATE TABLE} statement for the sink.
     *
     * <p>Previously this method built the DDL by {@code String.split(",")}-ing it back apart to
     * inject timestamp defaults, which also split any type containing a comma (for example {@code
     * DECIMAL(10,2)}) and produced invalid SQL. The defaults are now appended per column while the
     * statement is being assembled.
     *
     * @param sinkTableName target table name
     * @param fieldNames column names
     * @param fieldDataTypes Flink column types
     * @param primaryKeys primary key columns
     * @return the CREATE TABLE statement
     */
    public static String createTable(
            String sinkTableName,
            String[] fieldNames,
            DataType[] fieldDataTypes,
            List<String> primaryKeys) {

        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot create sink table '"
                            + sinkTableName
                            + "': MySQL CDC requires a primary key on the source table");
        }

        StringBuilder stmt = new StringBuilder();
        stmt.append("create table if not exists `").append(sinkTableName).append("` (\n");
        for (int i = 0; i < fieldNames.length; i++) {
            String column = fieldNames[i];
            String mysqlType = MysqlTypeMapper.toMysqlType(fieldDataTypes[i]);
            stmt.append("\t`").append(column).append("` ").append(mysqlType);
            // MySQL TIMESTAMP columns need an explicit default to be created without a value.
            if (mysqlType.toUpperCase().contains("TIMESTAMP")) {
                stmt.append(TIMESTAMP_DEFAULT);
            }
            stmt.append(",\n");
        }
        stmt.append("PRIMARY KEY (")
                .append(
                        StringUtils.join(
                                primaryKeys.stream().map(k -> "`" + k + "`").toArray(), ","))
                .append(")\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

        String createSql = stmt.toString();
        LOG.info("Sink table DDL:\n{}", createSql);
        return createSql;
    }

    /**
     * @return the JDBC URL for the source database described by the parameters.
     */
    public static String sourceJdbcUrl(ParameterTool params) {
        return String.format(
                "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai"
                        + "&useUnicode=true&characterEncoding=UTF-8",
                ParameterUtil.hostname(params),
                ParameterUtil.port(params),
                ParameterUtil.databaseName(params));
    }
}
