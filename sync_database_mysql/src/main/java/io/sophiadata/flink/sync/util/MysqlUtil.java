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

    public static Connection getConnection(
            final String sinkUrl, final String sinkUsername, final String sinkPassword)
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

    public static void executeSql(final Connection connection, final String createTable)
            throws SQLException {
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
            final String url,
            final String username,
            final String password,
            final String createTable)
            throws SQLException, ClassNotFoundException {
        try (Connection connection = getConnection(url, username, password)) {
            executeSql(connection, createTable);
        }
    }

    public static String createTable(
            final String sinkTableName,
            final String[] fieldNames,
            final DataType[] fieldDataTypes,
            final List<String> primaryKeys) {
        validateCreateTableArgs(sinkTableName, fieldNames, fieldDataTypes, primaryKeys);

        final StringBuilder stmt = new StringBuilder();
        stmt.append("create table if not exists `").append(sinkTableName).append("` (\n");
        for (int i = 0; i < fieldNames.length; i++) {
            stmt.append("  `")
                    .append(fieldNames[i])
                    .append("` ")
                    .append(fieldDataTypes[i].toString());
            if (needsTimestampDefault(fieldDataTypes[i])) {
                stmt.append(MYSQL_TIMESTAMP_DEFAULT);
            }
            stmt.append(",\n");
        }
        stmt.append("  PRIMARY KEY (").append(String.join(",", primaryKeys)).append(")\n)");

        final String createSql = stmt.toString();
        LOG.debug("Generated SQL: {}", createSql);
        return createSql;
    }

    private static void validateCreateTableArgs(
            final String sinkTableName,
            final String[] fieldNames,
            final DataType[] fieldDataTypes,
            final List<String> primaryKeys) {
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
        for (final String field : fieldNames) {
            if (!isValidIdentifier(field)) {
                throw new IllegalArgumentException("Invalid column name: " + field);
            }
        }
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw new IllegalArgumentException(
                    "primaryKeys must be non-empty (CDC requires a primary key)");
        }
        for (final String pk : primaryKeys) {
            if (!isValidIdentifier(pk)) {
                throw new IllegalArgumentException("Invalid primary key: " + pk);
            }
        }
    }

    private static boolean needsTimestampDefault(final DataType dataType) {
        // Only TIMESTAMP without time zone needs the 1970 default; binary / lob types never do.
        return dataType.getLogicalType() instanceof TimestampType
                && !(dataType.getLogicalType() instanceof VarBinaryType);
    }

    public static MySqlCatalog useMysqlCatalog(final ParameterTool params) {
        final String username = ParameterUtil.username(params);
        final String password = ParameterUtil.password(params);
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

    private static boolean isValidIdentifier(final String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return false;
        }
        return identifier.matches("[a-zA-Z0-9_]+");
    }

    /** SQL 标识符正则：字母或下划线开头，只含字母、数字、下划线。用于防 SQL 注入。 */
    private static final Pattern VALID_IDENTIFIER = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    public static String validateIdentifier(final String identifier) {
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
            final String jdbcUrl,
            final String user,
            final String pass,
            final String table,
            final Map<String, String> cols,
            final String pk) {
        final StringBuilder cl = new StringBuilder();
        int i = 0;
        for (final Map.Entry<String, String> e : cols.entrySet()) {
            if (e.getKey() == null || e.getKey().isEmpty()) {
                continue;
            }
            if (i > 0) {
                cl.append(", ");
            }
            cl.append("`").append(e.getKey()).append("` ").append(mapType(e.getValue()));
            i++;
        }
        final String sql =
                String.format(
                        "CREATE TABLE IF NOT EXISTS `%s` (%s, PRIMARY KEY(`%s`))", table, cl, pk);
        try (Connection c = DriverManager.getConnection(jdbcUrl, user, pass);
                Statement s = c.createStatement()) {
            s.executeUpdate(sql);
            LOG.info("Sink table '{}' ready", table);
        } catch (SQLException e) {
            LOG.warn(
                    "Sink table create error (may already exist): {} | SQL was: {}",
                    e.getMessage(),
                    sql);
        }
    }

    /** CDC 类型 → MySQL 列类型的映射表 */
    private static final java.util.Map<String, String> CDC_TO_MYSQL;

    static {
        final java.util.Map<String, String> m = new java.util.HashMap<>();
        m.put("BOOLEAN", "TINYINT(1)");
        m.put("BOOL", "TINYINT(1)");
        m.put("BIGINT", "BIGINT");
        m.put("INT", "INT");
        m.put("INTEGER", "INT");
        m.put("TINYINT", "INT");
        m.put("SMALLINT", "INT");
        m.put("TEXT", "VARCHAR(1024)");
        m.put("LONGTEXT", "VARCHAR(1024)");
        m.put("TIMESTAMP", "TIMESTAMP(6)");
        m.put("DATETIME", "DATETIME(6)");
        m.put("DATE", "DATE");
        m.put("TIME", "TIME");
        m.put("DOUBLE", "DOUBLE");
        m.put("FLOAT", "FLOAT");
        m.put("BLOB", "BLOB");
        m.put("BINARY", "BLOB");
        m.put("VARBINARY", "BLOB");
        CDC_TO_MYSQL = java.util.Collections.unmodifiableMap(m);
    }

    /**
     * 将 CDC / Debezium 报告的类型字符串映射为 MySQL 列类型。 例如 "TEXT" → "VARCHAR(1024)"，"BOOLEAN" → "TINYINT(1)"。
     * 带精度的类型（如 "DECIMAL(10,2)"）会保留原始精度。
     */
    public static String mapType(final String cdcType) {
        final String upper = cdcType.toUpperCase();
        if (containsWithPrecision(upper, "VARCHAR") || containsWithPrecision(upper, "CHAR")) {
            return withDefaultLength(cdcType, 255);
        }
        if (containsWithPrecision(upper, "DECIMAL") || containsWithPrecision(upper, "NUMERIC")) {
            return withDefaultLength(cdcType, "DECIMAL(10,2)");
        }
        final String mapped = CDC_TO_MYSQL.get(upper);
        if (mapped != null) {
            return mapped;
        }
        return mapByContains(upper);
    }

    private static boolean containsWithPrecision(final String upper, final String keyword) {
        return upper.contains(keyword);
    }

    private static String withDefaultLength(final String cdcType, final int defaultLen) {
        // information_schema.COLUMNS.DATA_TYPE returns the bare type name (e.g. "varchar")
        // without the length specifier. MySQL 8 rejects VARCHAR / CHAR without a length,
        // so add a sensible default when the precision is missing.
        return cdcType.contains("(") ? cdcType : cdcType + "(" + defaultLen + ")";
    }

    private static String withDefaultLength(final String cdcType, final String defaultType) {
        return cdcType.contains("(") ? cdcType : defaultType;
    }

    private static String mapByContains(final String upper) {
        if (upper.contains("DOUBLE")) {
            return "DOUBLE";
        }
        if (upper.contains("FLOAT")) {
            return "FLOAT";
        }
        if (upper.contains("TIMESTAMP")) {
            return "TIMESTAMP(6)";
        }
        if (upper.contains("DATETIME")) {
            return "DATETIME(6)";
        }
        if (upper.contains("BLOB") || upper.contains("BINARY")) {
            return "BLOB";
        }
        return "VARCHAR(1024)";
    }
}
