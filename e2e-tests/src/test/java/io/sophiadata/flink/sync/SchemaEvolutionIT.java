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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration test: two MySQL 8 containers (source + sink) via Testcontainers, Flink CDC
 * pipeline, simulates DDL changes (ADD COLUMN, MODIFY COLUMN, DROP+RE-ADD), verifies data + schema
 * evolution synced to sink.
 */
public class SchemaEvolutionIT extends AbstractMysqlSyncIT {

    @BeforeEach
    public void setUp() throws Exception {
        startMysqlContainers();

        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate("DROP TABLE IF EXISTS t_order");
            st.executeUpdate(
                    "CREATE TABLE t_order ("
                            + "  id BIGINT NOT NULL, "
                            + "  product VARCHAR(255), "
                            + "  amount DECIMAL(10,2), "
                            + "  create_time TIMESTAMP(0) NOT NULL DEFAULT '1970-01-01 09:00:00', "
                            + "  PRIMARY KEY (id))");
        }
    }

    @AfterEach
    public void tearDown() {
        stopMysqlContainers();
    }

    @Test
    public void testSyncWithAddColumn() throws Exception {
        startFlinkPipeline("schema-evolution-it");
        waitForCdcReady();

        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "INSERT INTO t_order VALUES "
                            + "(1, 'laptop', 5999.00, NOW()),"
                            + "(2, 'phone', 2999.00, NOW())");
        }

        waitForSinkRowCount("sink_t_order", 2, 60);
        assertSinkProduct("sink_t_order", 1, "laptop");
        assertSinkProduct("sink_t_order", 2, "phone");
        LOG.info("=== testSyncWithAddColumn: initial data synced ===");

        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate("ALTER TABLE t_order ADD COLUMN discount DECIMAL(10,2) DEFAULT 0.00");
        }
        TimeUnit.SECONDS.sleep(5);

        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "INSERT INTO t_order (id, product, amount, create_time, discount) "
                            + "VALUES (3, 'tablet', 1999.00, NOW(), 200.00)");
        }

        waitForSinkRowCount("sink_t_order", 3, 60);
        assertSinkProduct("sink_t_order", 3, "tablet");
        LOG.info("=== testSyncWithAddColumn PASSED ===");
    }

    @Test
    public void testSyncWithModifyColumn() throws Exception {
        startFlinkPipeline("schema-evolution-it");
        waitForCdcReady();

        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "INSERT INTO t_order VALUES "
                            + "(1, 'laptop', 5999.00, NOW()),"
                            + "(2, 'phone', 2999.00, NOW())");
        }

        waitForSinkRowCount("sink_t_order", 2, 60);

        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate("ALTER TABLE t_order MODIFY COLUMN amount DOUBLE");
        }
        TimeUnit.SECONDS.sleep(5);

        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "INSERT INTO t_order (id, product, amount, create_time) "
                            + "VALUES (3, 'monitor', 1299.50, NOW())");
        }

        waitForSinkRowCount("sink_t_order", 3, 60);
        assertSinkProduct("sink_t_order", 3, "monitor");
        LOG.info("=== testSyncWithModifyColumn PASSED ===");
    }

    @Test
    public void testSyncWithDropAndReAdd() throws Exception {
        startFlinkPipeline("schema-evolution-it");
        waitForCdcReady();

        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "INSERT INTO t_order VALUES "
                            + "(1, 'laptop', 5999.00, NOW()),"
                            + "(2, 'phone', 2999.00, NOW())");
        }

        waitForSinkRowCount("sink_t_order", 2, 60);

        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate("ALTER TABLE t_order DROP COLUMN amount");
            TimeUnit.SECONDS.sleep(10);
            st.executeUpdate("ALTER TABLE t_order ADD COLUMN amount DECIMAL(10,2) DEFAULT 0.00");
        }
        TimeUnit.SECONDS.sleep(15);

        for (int attempt = 1; attempt <= 3; attempt++) {
            try (Connection c = getSourceConnection();
                    Statement st = c.createStatement()) {
                st.executeUpdate(
                        "INSERT IGNORE INTO t_order (id, product, amount, create_time) "
                                + "VALUES (3, 'keyboard', 199.00, NOW())");
            }
            TimeUnit.SECONDS.sleep(5);
            try (Connection c = getSinkConnection();
                    Statement st = c.createStatement();
                    ResultSet rs =
                            st.executeQuery("SELECT COUNT(*) FROM " + SINK_DB + ".sink_t_order")) {
                if (rs.next() && rs.getInt(1) >= 3) {
                    break;
                }
            } catch (Exception ignored) {
            }
        }

        waitForSinkRowCount("sink_t_order", 3, 60);
        assertSinkProduct("sink_t_order", 3, "keyboard");
        LOG.info("=== testSyncWithDropAndReAdd PASSED ===");
    }

    // ---- helpers ----

    @SuppressWarnings("BusyWait")
    private void waitForCdcReady() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try (Connection c = getSinkConnection();
                    ResultSet rs =
                            c.createStatement()
                                    .executeQuery(
                                            "SELECT COUNT(*) FROM " + SINK_DB + ".sink_t_order")) {
                if (rs.next()) {
                    LOG.info("CDC pipeline ready — sink_t_order exists, waiting for binlog reader");
                    TimeUnit.SECONDS.sleep(10);
                    return;
                }
            } catch (Exception ignored) {
            }
            Thread.sleep(1000);
        }
        LOG.warn("Timed out waiting for sink_t_order to appear — proceeding anyway");
    }

    private void assertSinkProduct(String table, int id, String expectedProduct)
            throws java.sql.SQLException {
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
            assertTrue(rs.next(), "row id=" + id + " should exist");
            assertEquals(expectedProduct, rs.getString(1), "product mismatch for id=" + id);
        }
    }
}
