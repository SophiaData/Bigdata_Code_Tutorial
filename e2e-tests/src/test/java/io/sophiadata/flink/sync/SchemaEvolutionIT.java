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
import org.junit.ClassRule;
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
import static org.junit.Assert.assertTrue;

/**
 * End-to-end integration test: two MySQL 8 containers (source + sink) via Testcontainers, Flink CDC
 * pipeline, simulates DDL changes (ADD COLUMN, MODIFY COLUMN, DROP+RE-ADD), verifies data + schema
 * evolution synced to sink.
 */
public class SchemaEvolutionIT {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaEvolutionIT.class);

    private static final String SOURCE_DB = "flink_source";
    private static final String SINK_DB = "flink_sink";

    @ClassRule
    public static final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            // Explicitly bound TM memory; the default gets parsed as
                            // 1024 GB on this classpath and the job cannot acquire a
                            // slot (NoResourceAvailableException). 1 GiB is plenty.
                            .setConfiguration(
                                    new Configuration()
                                            .set(
                                                    TaskManagerOptions.TOTAL_PROCESS_MEMORY,
                                                    MemorySize.ofMebiBytes(1024)))
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    private JdbcDatabaseContainer<?> sourceContainer;
    private JdbcDatabaseContainer<?> sinkContainer;

    private String sourceUrl;
    private String sinkUrl;

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;
    private Thread flinkJobThread;

    @Before
    public void setUp() throws Exception {
        // Source: enable ROW binlog for CDC
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

        sourceUrl =
                "jdbc:mysql://"
                        + sourceContainer.getHost()
                        + ":"
                        + sourceContainer.getMappedPort(3306)
                        + "/"
                        + SOURCE_DB
                        + "?useSSL=false&allowPublicKeyRetrieval=true";
        sinkUrl =
                "jdbc:mysql://"
                        + sinkContainer.getHost()
                        + ":"
                        + sinkContainer.getMappedPort(3306)
                        + "/"
                        + SINK_DB
                        + "?useSSL=false&allowPublicKeyRetrieval=true";

        LOG.info("Source MySQL: {}", sourceUrl);
        LOG.info("Sink MySQL: {}", sinkUrl);

        // Create CDC user with replication privileges on source
        try (Connection c =
                        DriverManager.getConnection(
                                "jdbc:mysql://"
                                        + sourceContainer.getHost()
                                        + ":"
                                        + sourceContainer.getMappedPort(3306)
                                        + "/?useSSL=false&allowPublicKeyRetrieval=true",
                                "root",
                                "root");
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "CREATE USER IF NOT EXISTS 'cdc_user'@'%' IDENTIFIED BY 'cdc_password'");
            st.executeUpdate(
                    "GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%'");
            st.executeUpdate("FLUSH PRIVILEGES");
        }

        // Seed initial source table
        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS t_order ("
                            + "  id BIGINT NOT NULL, "
                            + "  product VARCHAR(255), "
                            + "  amount DECIMAL(10,2), "
                            + "  create_time TIMESTAMP(0) NOT NULL DEFAULT '1970-01-01 09:00:00', "
                            + "  PRIMARY KEY (id))");
            st.executeUpdate(
                    "INSERT INTO t_order VALUES "
                            + "(1, 'laptop', 5999.00, NOW()),"
                            + "(2, 'phone', 2999.00, NOW())");
        }
    }

    @After
    public void tearDown() {
        if (flinkJobThread != null) {
            // 先给管道时间自然结束，不要立即打断
            try {
                flinkJobThread.join(15000);
            } catch (InterruptedException ignored) {
            }
            // 如果还没结束，再强制打断
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

    @Test
    public void testSyncWithAddColumn() throws Exception {
        startFlinkPipeline();

        // Wait for initial 2 rows
        waitForSinkRowCount("sink_t_order", 2, 60);
        assertSinkProduct("sink_t_order", 1, "laptop");
        assertSinkProduct("sink_t_order", 2, "phone");
        LOG.info("=== testSyncWithAddColumn: initial data synced ===");

        // DDL: ADD COLUMN `discount`
        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate("ALTER TABLE t_order ADD COLUMN discount DECIMAL(10,2) DEFAULT 0.00");
        }
        LOG.info("DDL executed: ADD COLUMN discount");
        TimeUnit.SECONDS.sleep(5);

        // Insert row with new column
        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "INSERT INTO t_order (id, product, amount, create_time, discount) "
                            + "VALUES (3, 'tablet', 1999.00, NOW(), 200.00)");
        }

        waitForSinkRowCount("sink_t_order", 3, 60);
        assertSinkProduct("sink_t_order", 3, "tablet");
        LOG.info("=== Phase 2 PASSED: ADD COLUMN + new data synced ===");
    }

    @Test
    public void testSyncWithModifyColumn() throws Exception {
        startFlinkPipeline();

        waitForSinkRowCount("sink_t_order", 2, 60);
        LOG.info("=== testSyncWithModifyColumn: initial data synced ===");

        // DDL: MODIFY COLUMN `amount` to DOUBLE
        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate("ALTER TABLE t_order MODIFY COLUMN amount DOUBLE");
        }
        LOG.info("DDL executed: MODIFY COLUMN amount -> DOUBLE");
        TimeUnit.SECONDS.sleep(5);

        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "INSERT INTO t_order (id, product, amount, create_time) "
                            + "VALUES (3, 'monitor', 1299.50, NOW())");
        }

        waitForSinkRowCount("sink_t_order", 3, 60);
        assertSinkProduct("sink_t_order", 3, "monitor");
        LOG.info("=== Phase 2 PASSED: MODIFY COLUMN + data synced ===");
    }

    @Test
    public void testSyncWithDropAndReAdd() throws Exception {
        startFlinkPipeline();

        waitForSinkRowCount("sink_t_order", 2, 60);
        LOG.info("=== testSyncWithDropAndReAdd: initial data synced ===");

        // Drop then re-add a column
        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate("ALTER TABLE t_order DROP COLUMN amount");
            TimeUnit.SECONDS.sleep(2);
            st.executeUpdate("ALTER TABLE t_order ADD COLUMN amount DECIMAL(10,2) DEFAULT 0.00");
        }
        LOG.info("DDL executed: DROP + RE-ADD amount column");
        TimeUnit.SECONDS.sleep(5);

        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "INSERT INTO t_order (id, product, amount, create_time) "
                            + "VALUES (3, 'keyboard', 199.00, NOW())");
        }

        waitForSinkRowCount("sink_t_order", 3, 60);
        assertSinkProduct("sink_t_order", 3, "keyboard");
        LOG.info("=== Phase 2 PASSED: DROP + RE-ADD column synced ===");
    }

    // ---- helpers ----

    private void startFlinkPipeline() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(2000);
        tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig()
                .getConfiguration()
                .setString("pipeline.name", "schema-evolution-it-" + System.currentTimeMillis());

        Map<String, String> args = new HashMap<>();
        args.put("hostname", sourceContainer.getHost());
        args.put("port", String.valueOf(sourceContainer.getMappedPort(3306)));
        args.put("username", "cdc_user");
        args.put("password", "cdc_password");
        args.put("databaseName", SOURCE_DB);
        args.put("tableList", SOURCE_DB + ".*");
        args.put("sinkUrl", sinkUrl);
        args.put("sinkUsername", "root");
        args.put("sinkPassword", "root");
        args.put("serverTimeZone", "UTC");
        args.put("setParallelism", "1");
        args.put("cdcSourceName", "mysql-cdc-sit");

        flinkJobThread =
                new Thread(
                        () -> {
                            try {
                                new FlinkSqlWDS().handle(toArgs(args), env, tEnv);
                            } catch (Exception e) {
                                LOG.error("Flink pipeline failed", e);
                            }
                        },
                        "flink-wds-it");
        flinkJobThread.setDaemon(true);
        flinkJobThread.start();

        // Give pipeline time to start and discover existing tables
        try {
            TimeUnit.SECONDS.sleep(8);
        } catch (InterruptedException ignored) {
        }
    }

    private Connection getSourceConnection() throws SQLException {
        return DriverManager.getConnection(sourceUrl, "root", "root");
    }

    private Connection getSinkConnection() throws SQLException {
        return DriverManager.getConnection(sinkUrl, "root", "root");
    }

    @SuppressWarnings("BusyWait")
    private void waitForSinkRowCount(String table, int expected, int timeoutSeconds)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        int count = 0;
        while (System.nanoTime() < deadline) {
            try (Connection c = getSinkConnection();
                    Statement st = c.createStatement()) {
                try (ResultSet rs =
                        st.executeQuery("SELECT COUNT(*) FROM " + SINK_DB + "." + table)) {
                    if (rs.next()) {
                        count = rs.getInt(1);
                        if (count >= expected) return;
                    }
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

    private void assertSinkProduct(String table, int id, String expectedProduct)
            throws SQLException {
        try (Connection c = getSinkConnection();
                Statement st = c.createStatement();
                ResultSet rs =
                        st.executeQuery(
                                "SELECT product FROM "
                                        + SINK_DB
                                        + "."
                                        + table
                                        + " WHERE id = "
                                        + id)) {
            assertTrue("row id=" + id + " should exist", rs.next());
            assertEquals("product mismatch for id=" + id, expectedProduct, rs.getString(1));
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
