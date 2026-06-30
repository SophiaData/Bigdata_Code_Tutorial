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
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** (@SophiaData) (@date 2023/5/31 19:02). */
public final class MysqlUtil {

    private MysqlUtil() {}

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

    /** SQL 标识符正则：字母或下划线开头，只含字母、数字、下划线。用于防 SQL 注入。 */
    private static final Pattern VALID_IDENTIFIER = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    public static String validateIdentifier(String identifier) {
        if (identifier == null || !VALID_IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException("Invalid SQL identifier: " + identifier);
        }
        return identifier;
    }

    /**
     * 根据 CDC 捕获的列信息在 sink 端建表（IF NOT EXISTS）。 列类型通过 {@link #mapType} 从 CDC 类型映射为 MySQL 类型。 表名格式为
     * {@code sink_<原始表名>}，带反引号防止关键字冲突。
     */
    public static void createSinkTableIfNotExists(
            String jdbcUrl,
            String user,
            String pass,
            String table,
            Map<String, String> cols,
            String pk) {
        StringBuilder cl = new StringBuilder();
        int i = 0;
        for (Map.Entry<String, String> e : cols.entrySet()) {
            if (i > 0) {
                cl.append(", ");
            }
            cl.append("`").append(e.getKey()).append("` ").append(mapType(e.getValue()));
            i++;
        }
        String sql =
                String.format(
                        "CREATE TABLE IF NOT EXISTS `%s` (%s, PRIMARY KEY(`%s`))", table, cl, pk);
        try (Connection c = DriverManager.getConnection(jdbcUrl, user, pass);
                Statement s = c.createStatement()) {
            s.executeUpdate(sql);
            LOG.info("Sink table '{}' ready", table);
        } catch (SQLException e) {
            LOG.warn("Sink table create error (may already exist): {}", e.getMessage());
        }
    }

    /**
     * 将 CDC / Debezium 报告的类型字符串映射为 MySQL 列类型。 例如 "TEXT" → "VARCHAR(1024)"，"BOOLEAN" → "TINYINT(1)"。
     * 带精度的类型（如 "DECIMAL(10,2)"）会保留原始精度。
     */
    public static String mapType(String cdcType) {
        String upper = cdcType.toUpperCase();
        if (upper.contains("BOOLEAN") || upper.contains("BOOL")) {
            return "TINYINT(1)";
        }
        if (upper.contains("BIGINT")) {
            return "BIGINT";
        }
        if (upper.contains("INT")) {
            return "INT";
        }
        if (upper.contains("VARCHAR") || upper.contains("CHAR")) {
            return cdcType;
        }
        if (upper.contains("TEXT")) {
            return "VARCHAR(1024)";
        }
        if (upper.contains("DECIMAL") || upper.contains("NUMERIC")) {
            return upper.matches(".*DECIMAL\\(\\d+,\\d+\\).*") ? cdcType : "DECIMAL(10,2)";
        }
        if (upper.contains("TIMESTAMP")) {
            return "TIMESTAMP(6)";
        }
        if (upper.contains("DATETIME")) {
            return "DATETIME(6)";
        }
        if (upper.contains("DATE")) {
            return "DATE";
        }
        if (upper.contains("TIME")) {
            return "TIME";
        }
        if (upper.contains("DOUBLE")) {
            return "DOUBLE";
        }
        if (upper.contains("FLOAT")) {
            return "FLOAT";
        }
        if (upper.contains("BLOB") || upper.contains("BINARY")) {
            return "BLOB";
        }
        LOG.warn("Unknown CDC type '{}', mapping to VARCHAR(1024)", cdcType);
        return "VARCHAR(1024)";
    }
}
