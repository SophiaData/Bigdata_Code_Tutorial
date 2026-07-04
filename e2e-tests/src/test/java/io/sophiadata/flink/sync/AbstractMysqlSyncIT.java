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
import org.junit.Rule;
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
 * Base class for MySQL CDC end-to-end integration tests.
 *
 * <p>Provides common infrastructure: Testcontainers MySQL (source + sink), Flink MiniCluster, CDC
 * user setup, pipeline lifecycle, and helper methods for assertions.
 */
public abstract class AbstractMysqlSyncIT {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractMysqlSyncIT.class);

    protected static final String SOURCE_DB = "flink_source";
    protected static final String SINK_DB = "flink_sink";

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

    protected JdbcDatabaseContainer<?> sourceContainer;
    protected JdbcDatabaseContainer<?> sinkContainer;

    protected String sourceUrl;
    protected String sinkUrl;
    protected String sourceHost;
    protected int sourcePort;

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;
    protected Thread flinkJobThread;

    /**
     * Start MySQL containers and create CDC user. Subclasses call this from {@code @Before} and
     * then create their specific source tables.
     */
    protected void startMysqlContainers() throws Exception {
        sourceContainer =
                new MySqlContainer(MySqlVersion.V8_0)
                        .withDatabaseName(SOURCE_DB)
                        .withUsername("root")
                        .withPassword("root")
                        .withConfigurationOverride("docker.cnf");
        sourceContainer.start();

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
    }

    /**
     * Stop containers and clean up Flink pipeline thread. Subclasses call this from {@code @After}.
     */
    protected void stopMysqlContainers() {
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

    // ---- Flink pipeline helpers ----

    protected void startFlinkPipeline(String pipelineName) {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(2000);
        tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig()
                .getConfiguration()
                .setString("pipeline.name", pipelineName + "-" + System.currentTimeMillis());

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

    // ---- JDBC helpers ----

    protected Connection getSourceConnection() throws SQLException {
        return DriverManager.getConnection(sourceUrl, "root", "root");
    }

    protected Connection getSinkConnection() throws SQLException {
        return DriverManager.getConnection(sinkUrl, "root", "root");
    }

    /** Wait until the CDC pipeline's bootstrap has created the specified sink table. */
    @SuppressWarnings("BusyWait")
    protected void waitForSinkTable(String table, int timeoutSeconds) throws Exception {
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
            }
            Thread.sleep(1000);
        }
        assertTrue("Timed out waiting for sink table " + table, false);
    }

    @SuppressWarnings("BusyWait")
    protected void waitForSinkRowCount(String table, int expected, int timeoutSeconds)
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
            }
            Thread.sleep(2000);
        }
        assertTrue(
                "Expected at least " + expected + " rows in " + table + ", got " + count,
                count >= expected);
    }

    protected void assertSinkRow(String table, String column, String value) throws SQLException {
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

    protected void assertSinkValue(
            String table, String whereCol, String whereVal, String selectCol, Object expected)
            throws SQLException {
        try (Connection c = getSinkConnection();
                Statement st = c.createStatement();
                ResultSet rs =
                        st.executeQuery(
                                "SELECT " + selectCol + " FROM " + SINK_DB + "." + table + " WHERE "
                                        + whereCol + " = '" + whereVal + "'")) {
            assertTrue("row with " + whereCol + "=" + whereVal + " should exist", rs.next());
            if (expected instanceof Integer) {
                assertEquals("value mismatch", ((Integer) expected).intValue(), rs.getInt(1));
            } else {
                assertEquals("value mismatch", expected, rs.getObject(1));
            }
        }
    }

    protected static String[] toArgs(Map<String, String> m) {
        String[] out = new String[m.size() * 2];
        int i = 0;
        for (Map.Entry<String, String> e : m.entrySet()) {
            out[i++] = "--" + e.getKey();
            out[i++] = e.getValue();
        }
        return out;
    }
}
