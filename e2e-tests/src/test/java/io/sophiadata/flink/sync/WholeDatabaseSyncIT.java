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

package io.sophiadata.flink.sync;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import io.sophiadata.flink.utils.MySqlContainer;
import io.sophiadata.flink.utils.MySqlVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end integration test for whole-database CDC sync.
 *
 * <p>Spins up two MySQL 8.0 containers (source + sink), runs the FlinkSqlWDS pipeline, inserts data
 * into the source, and verifies that INSERT / UPDATE / DELETE operations are correctly synced to
 * the sink database with {@code sink_} prefix table names.
 *
 * <p>This test specifically covers the bug where SharedSchemaState serialization across Flink
 * operators caused schemas to be empty on the TaskManager side, resulting in all records being
 * silently skipped.
 */
public class WholeDatabaseSyncIT {

    private static final Logger LOG = LoggerFactory.getLogger(WholeDatabaseSyncIT.class);

    private static final String SOURCE_DB = "flink_source";
    private static final String SINK_DB = "flink_sink";

    @Rule
    public MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .setConfiguration(createMiniClusterConfig())
                            .build());

    private static Configuration createMiniClusterConfig() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.TASK_HEAP_MEMORY, MemorySize.ofMebiBytes(512));
        config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(128));
        config.set(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY, MemorySize.ofMebiBytes(256));
        config.set(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(64));
        return config;
    }

    private JdbcDatabaseContainer<?> sourceContainer;
    private JdbcDatabaseContainer<?> sinkContainer;

    private String sourceUrl;
    private String sinkUrl;
    private String sourceHost;
    private int sourcePort;

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;
    private Thread flinkJobThread;

    @Before
    public void setUp() throws Exception {
        // Source: MySQL 8.0 with ROW binlog for CDC
        sourceContainer =
                new MySqlContainer(MySqlVersion.V8_0)
                        .withDatabaseName(SOURCE_DB)
                        .withUsername("root")
                        .withPassword("root")
                        .withConfigurationOverride("docker.cnf");
        sourceContainer.start();

        // Sink: plain MySQL
        sinkContainer =
                new MySqlContainer(MySqlVersion.V8_0)
                        .withDatabaseName(SINK_DB)
                        .withUsername("root")
                        .withPassword("root");
        sinkContainer.start();

        sourceHost = sourceContainer.getHost();
        sourcePort = sourceContainer.getMappedPort(3306);
        sourceUrl =
                "jdbc:mysql://"
                        + sourceHost
                        + ":"
                        + sourcePort
                        + "/"
                        + SOURCE_DB
                        + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        sinkUrl =
                "jdbc:mysql://"
                        + sinkContainer.getHost()
                        + ":"
                        + sinkContainer.getMappedPort(3306)
                        + "/"
                        + SINK_DB
                        + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

        LOG.info("Source MySQL: {}", sourceUrl);
        LOG.info("Sink MySQL: {}", sinkUrl);

        // Create CDC user with replication privileges on source
        try (Connection c =
                        DriverManager.getConnection(
                                "jdbc:mysql://"
                                        + sourceHost
                                        + ":"
                                        + sourcePort
                                        + "/?useSSL=false&allowPublicKeyRetrieval=true",
                                "root",
                                "root");
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "CREATE USER IF NOT EXISTS 'cdc_user'@'%' IDENTIFIED BY 'cdc_password'");
            st.executeUpdate(
                    "GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, "
                            + "REPLICATION CLIENT, LOCK TABLES ON *.* TO 'cdc_user'@'%'");
            st.executeUpdate("FLUSH PRIVILEGES");
        }

        // Create source tables
        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate("DROP TABLE IF EXISTS orders");
            st.executeUpdate(
                    "CREATE TABLE orders ("
                            + "  id BIGINT NOT NULL AUTO_INCREMENT, "
                            + "  product VARCHAR(255), "
                            + "  amount DECIMAL(10,2), "
                            + "  status VARCHAR(50) DEFAULT 'pending', "
                            + "  PRIMARY KEY (id))");

            st.executeUpdate("DROP TABLE IF EXISTS users");
            st.executeUpdate(
                    "CREATE TABLE users ("
                            + "  id BIGINT NOT NULL AUTO_INCREMENT, "
                            + "  name VARCHAR(100), "
                            + "  email VARCHAR(255), "
                            + "  age INT, "
                            + "  PRIMARY KEY (id))");
        }
    }

    @After
    public void tearDown() {
        if (flinkJobThread != null) {
            try {
                flinkJobThread.join(15000);
            } catch (InterruptedException ignored) {
            }
            if (flinkJobThread.isAlive()) {
                flinkJobThread.interrupt();
                try {
                    flinkJobThread.join(5000);
                } catch (InterruptedException ignored) {
                }
            }
        }
        if (sourceContainer != null) sourceContainer.stop();
        if (sinkContainer != null) sinkContainer.stop();
    }

    // ---- Test cases ----

    @Test
    public void testInsertSync() throws Exception {
        startFlinkPipeline();
        waitForSinkTable("sink_users", 60);

        // Insert data into source
        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "INSERT INTO users (name, email, age) VALUES "
                            + "('Alice', 'alice@example.com', 25),"
                            + "('Bob', 'bob@example.com', 30)");
        }

        waitForSinkRowCount("sink_users", 2, 60);
        assertSinkRow("sink_users", "name", "Alice");
        assertSinkRow("sink_users", "name", "Bob");
        LOG.info("=== testInsertSync PASSED ===");
    }

    @Test
    public void testUpdateSync() throws Exception {
        startFlinkPipeline();
        waitForSinkTable("sink_users", 60);

        // Step 1: Insert, then wait for it to sync before updating
        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 25)");
        }
        waitForSinkRowCount("sink_users", 1, 60);
        TimeUnit.SECONDS.sleep(3);

        // Step 2: Update
        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate("UPDATE users SET age = 26 WHERE name = 'Alice'");
        }

        // Step 3: Wait for the sink to reflect the update
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        int age = 0;
        while (System.nanoTime() < deadline) {
            try (Connection c = getSinkConnection();
                    Statement st = c.createStatement();
                    ResultSet rs =
                            st.executeQuery(
                                    "SELECT age FROM "
                                            + SINK_DB
                                            + ".sink_users WHERE name = 'Alice'")) {
                if (rs.next()) {
                    age = rs.getInt("age");
                    if (age == 26) break;
                }
            }
            Thread.sleep(2000);
        }
        assertEquals("Age should be updated to 26", 26, age);
        LOG.info("=== testUpdateSync PASSED ===");
    }

    @Test
    public void testDeleteSync() throws Exception {
        startFlinkPipeline();
        waitForSinkTable("sink_users", 60);

        // Step 1: Insert two rows, wait for sync
        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "INSERT INTO users (name, email, age) VALUES "
                            + "('Alice', 'alice@example.com', 25),"
                            + "('Bob', 'bob@example.com', 30)");
        }
        waitForSinkRowCount("sink_users", 2, 60);
        TimeUnit.SECONDS.sleep(3);

        // Step 2: Delete Alice
        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate("DELETE FROM users WHERE name = 'Alice'");
        }

        // Step 3: Wait for sink to reflect the delete
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        int remaining = 2;
        while (System.nanoTime() < deadline) {
            try (Connection c = getSinkConnection();
                    Statement st = c.createStatement();
                    ResultSet rs =
                            st.executeQuery("SELECT COUNT(*) FROM " + SINK_DB + ".sink_users")) {
                if (rs.next()) {
                    remaining = rs.getInt(1);
                    if (remaining <= 1) break;
                }
            }
            Thread.sleep(2000);
        }
        assertFalse("Alice should be deleted from sink", remaining > 1);
        assertSinkRow("sink_users", "name", "Bob");
        LOG.info("=== testDeleteSync PASSED ===");
    }

    @Test
    public void testMultipleTableSync() throws Exception {
        startFlinkPipeline();
        waitForSinkTable("sink_users", 60);
        waitForSinkTable("sink_orders", 60);

        // Insert into both tables
        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 25)");
            TimeUnit.SECONDS.sleep(1);
            st.executeUpdate(
                    "INSERT INTO orders (product, amount, status) VALUES ('Laptop', 999.99, 'completed')");
        }

        waitForSinkRowCount("sink_users", 1, 60);
        waitForSinkRowCount("sink_orders", 1, 60);
        assertSinkRow("sink_users", "name", "Alice");
        assertSinkRow("sink_orders", "product", "Laptop");
        LOG.info("=== testMultipleTableSync PASSED ===");
    }

    @Test
    public void testPreExistingDataSync() throws Exception {
        // Insert data BEFORE starting the pipeline (tests bootstrap path)
        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "INSERT INTO users (name, email, age) VALUES "
                            + "('Alice', 'alice@example.com', 25),"
                            + "('Bob', 'bob@example.com', 30),"
                            + "('Charlie', 'charlie@example.com', 35)");
        }

        startFlinkPipeline();
        waitForSinkTable("sink_users", 60);

        // Pre-existing data should be synced via snapshot
        waitForSinkRowCount("sink_users", 3, 60);
        assertSinkRow("sink_users", "name", "Alice");
        assertSinkRow("sink_users", "name", "Bob");
        assertSinkRow("sink_users", "name", "Charlie");
        LOG.info("=== testPreExistingDataSync PASSED ===");
    }

    // ---- Helpers ----

    private void startFlinkPipeline() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(2000);
        tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig()
                .getConfiguration()
                .setString("pipeline.name", "whole-db-sync-it-" + System.currentTimeMillis());

        Map<String, String> args = new HashMap<>();
        args.put("hostname", sourceHost);
        args.put("port", String.valueOf(sourcePort));
        args.put("username", "root");
        args.put("password", "root");
        args.put("databaseName", SOURCE_DB);
        args.put("tableList", SOURCE_DB + ".*");
        args.put("sinkUrl", sinkUrl);
        args.put("sinkUsername", "root");
        args.put("sinkPassword", "root");
        args.put("serverTimeZone", "UTC");
        args.put("setParallelism", "1");
        args.put("cdcSourceName", "mysql-cdc-e2e");

        flinkJobThread =
                new Thread(
                        () -> {
                            try {
                                new FlinkSqlWDS().handle(toArgs(args), env, tEnv);
                            } catch (Exception e) {
                                LOG.error("Flink pipeline failed", e);
                            }
                        },
                        "flink-wds-e2e");
        flinkJobThread.setDaemon(true);
        flinkJobThread.start();
    }

    private Connection getSourceConnection() throws SQLException {
        return DriverManager.getConnection(sourceUrl, "root", "root");
    }

    private Connection getSinkConnection() throws SQLException {
        return DriverManager.getConnection(sinkUrl, "root", "root");
    }

    @SuppressWarnings("BusyWait")
    private void waitForSinkTable(String table, int timeoutSeconds) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            try (Connection c = getSinkConnection();
                    ResultSet rs =
                            c.createStatement()
                                    .executeQuery(
                                            "SELECT COUNT(*) FROM " + SINK_DB + "." + table)) {
                if (rs.next()) {
                    LOG.info("Sink table {} is ready", table);
                    TimeUnit.SECONDS.sleep(5);
                    return;
                }
            } catch (Exception ignored) {
                // table not yet created by bootstrap
            }
            Thread.sleep(1000);
        }
        assertTrue("Timed out waiting for sink table " + table, false);
    }

    @SuppressWarnings("BusyWait")
    private void waitForSinkRowCount(String table, int expected, int timeoutSeconds)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        int count = 0;
        while (System.nanoTime() < deadline) {
            try (Connection c = getSinkConnection();
                    Statement st = c.createStatement();
                    ResultSet rs =
                            st.executeQuery("SELECT COUNT(*) FROM " + SINK_DB + "." + table)) {
                if (rs.next()) {
                    count = rs.getInt(1);
                    if (count >= expected) return;
                }
            } catch (Exception ignored) {
                // table may not exist yet
            }
            Thread.sleep(2000);
        }
        assertTrue(
                "Expected at least " + expected + " rows in " + table + ", got " + count,
                count >= expected);
    }

    private void assertSinkRow(String table, String column, String value) throws SQLException {
        try (Connection c = getSinkConnection();
                Statement st = c.createStatement();
                ResultSet rs =
                        st.executeQuery(
                                "SELECT COUNT(*) FROM "
                                        + SINK_DB
                                        + "."
                                        + table
                                        + " WHERE "
                                        + column
                                        + " = '"
                                        + value
                                        + "'")) {
            assertTrue("row with " + column + "=" + value + " should exist", rs.next());
            assertTrue("expected at least 1 row with " + column + "=" + value, rs.getInt(1) >= 1);
        }
    }

    private static String[] toArgs(Map<String, String> m) {
        String[] out = new String[m.size() * 2];
        int i = 0;
        for (Map.Entry<String, String> e : m.entrySet()) {
            out[i++] = "--" + e.getKey();
            out[i++] = e.getValue();
        }
        return out;
    }
}
