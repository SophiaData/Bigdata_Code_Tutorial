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

package io.sophiadata.flink.sync.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import io.sophiadata.flink.sync.util.MysqlUtil;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration test for {@link MysqlUtil#createTable} — runs the generated DDL against a real MySQL
 * 8 server and verifies the resulting schema.
 *
 * <p>Enable with {@code -DrunIntegrationTests=true}. Configure the connection in either of two
 * ways:
 *
 * <ul>
 *   <li>Provide a pre-started MySQL via {@code -Dmysql.test.url=jdbc:mysql://host:port/
 *       -Dmysql.test.user=root -Dmysql.test.password=...}. Recommended when running the suite
 *       outside of CI.
 *   <li>Otherwise a Testcontainers MySQL 8 container is started; the test is skipped if Docker is
 *       unavailable.
 * </ul>
 */
public class CreateMysqlLSinkTableIT {

    @Test
    public void testCreateTableAgainstRealMySQL() throws SQLException, ClassNotFoundException {
        Assumptions.assumeTrue(
                Boolean.getBoolean("runIntegrationTests"),
                "set -DrunIntegrationTests=true to enable the sink-DDL integration test");

        String externalUrl = System.getProperty("mysql.test.url");
        String user = System.getProperty("mysql.test.user", "root");
        String pw = System.getProperty("mysql.test.password", "root");

        String url;
        if (externalUrl != null && !externalUrl.isEmpty()) {
            url = externalUrl;
        } else {
            JdbcDatabaseContainer<?> container = newContainer();
            try {
                container.start();
            } catch (Throwable t) {
                Assumptions.assumeTrue(false, "Docker unavailable: " + t.getMessage());
                return;
            }
            try {
                runAgainst(
                        container.getJdbcUrl(), container.getUsername(), container.getPassword());
            } finally {
                container.stop();
            }
            return;
        }
        runAgainst(url, user, pw);
    }

    private void runAgainst(String url, String user, String pw)
            throws SQLException, ClassNotFoundException {
        String[] columns = {"id", "name", "age", "create_time"};
        DataType[] types = {
            DataTypes.BIGINT().notNull(),
            DataTypes.VARCHAR(255),
            DataTypes.TINYINT(),
            DataTypes.TIMESTAMP()
        };

        // Drop any leftover table from a previous run so the test is idempotent.
        MysqlUtil.executeSqlAndClose(url, user, pw, "DROP TABLE IF EXISTS sink_t_user");

        String sql =
                MysqlUtil.createTable(
                        "sink_t_user", columns, types, Collections.singletonList("id"));
        assertNotNull(sql);

        MysqlUtil.executeSqlAndClose(url, user, pw, sql);

        try (Connection c = MysqlUtil.getConnection(url, user, pw);
                Statement st = c.createStatement();
                ResultSet rs =
                        st.executeQuery(
                                "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_DEFAULT, IS_NULLABLE "
                                        + "FROM information_schema.COLUMNS "
                                        + "WHERE TABLE_SCHEMA = DATABASE() "
                                        + "AND TABLE_NAME = 'sink_t_user' "
                                        + "ORDER BY ORDINAL_POSITION")) {
            int count = 0;
            while (rs.next()) {
                count++;
                String col = rs.getString("COLUMN_NAME");
                if ("id".equals(col)) {
                    assertEquals("bigint", rs.getString("DATA_TYPE"));
                    assertEquals("NO", rs.getString("IS_NULLABLE"));
                } else if ("create_time".equals(col)) {
                    String def = rs.getString("COLUMN_DEFAULT");
                    assertNotNull(def);
                    assertTrue(
                            def.startsWith("1970-01-01 09:00:00"),
                            "default should be 1970-01-01 09:00:00, got: " + def);
                }
            }
            assertEquals(4, count, "expected exactly 4 columns");
        }

        try (Connection c = MysqlUtil.getConnection(url, user, pw);
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "INSERT INTO sink_t_user (id, name, age, create_time) VALUES "
                            + "(1, 'alice', 30, '2024-01-01 00:00:00'),"
                            + " (2, 'bob', 25, '2024-01-02 00:00:00')");
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM sink_t_user");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    private static JdbcDatabaseContainer<?> newContainer() {
        try {
            Class<?> cls = Class.forName("io.sophiadata.flink.utils.MySqlContainer");
            Class<?> verCls = Class.forName("io.sophiadata.flink.utils.MySqlVersion");
            Object v8 = verCls.getEnumConstants()[0];
            return (JdbcDatabaseContainer<?>) cls.getConstructor(verCls).newInstance(v8);
        } catch (Throwable t) {
            throw new RuntimeException("MySqlContainer class not found on classpath", t);
        }
    }

    @Test
    public void testRejectsInvalidIdentifier() {
        DataType[] types = {DataTypes.BIGINT()};
        try {
            MysqlUtil.createTable(
                    "sink_t; DROP TABLE x", new String[] {"id"}, types, Arrays.asList("id"));
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }
}
