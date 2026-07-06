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
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * End-to-end integration test for whole-database CDC sync.
 *
 * <p>Spins up two MySQL 8.0 containers (source + sink), runs the FlinkSqlWDS pipeline, inserts data
 * into the source, and verifies that INSERT / UPDATE / DELETE operations are correctly synced to
 * the sink database with {@code sink_} prefix table names.
 */
public class WholeDatabaseSyncIT extends AbstractMysqlSyncIT {

    @BeforeEach
    public void setUp() throws Exception {
        startMysqlContainers();

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

    @AfterEach
    public void tearDown() {
        stopMysqlContainers();
    }

    @Test
    public void testInsertSync() throws Exception {
        startFlinkPipeline("whole-db-sync-it");
        waitForSinkTable("sink_users", 60);

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
        startFlinkPipeline("whole-db-sync-it");
        waitForSinkTable("sink_users", 60);

        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 25)");
        }
        waitForSinkRowCount("sink_users", 1, 60);
        TimeUnit.SECONDS.sleep(3);

        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate("UPDATE users SET age = 26 WHERE name = 'Alice'");
        }

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        int age = 0;
        while (System.nanoTime() < deadline) {
            try (java.sql.Connection c = getSinkConnection();
                    Statement st = c.createStatement();
                    java.sql.ResultSet rs =
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
        startFlinkPipeline("whole-db-sync-it");
        waitForSinkTable("sink_users", 60);

        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "INSERT INTO users (name, email, age) VALUES "
                            + "('Alice', 'alice@example.com', 25),"
                            + "('Bob', 'bob@example.com', 30)");
        }
        waitForSinkRowCount("sink_users", 2, 60);
        TimeUnit.SECONDS.sleep(3);

        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate("DELETE FROM users WHERE name = 'Alice'");
        }

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        int remaining = 2;
        while (System.nanoTime() < deadline) {
            try (java.sql.Connection c = getSinkConnection();
                    Statement st = c.createStatement();
                    java.sql.ResultSet rs =
                            st.executeQuery("SELECT COUNT(*) FROM " + SINK_DB + ".sink_users")) {
                if (rs.next()) {
                    remaining = rs.getInt(1);
                    if (remaining <= 1) break;
                }
            }
            Thread.sleep(2000);
        }
        assertFalse(remaining > 1, "Alice should be deleted from sink");
        assertSinkRow("sink_users", "name", "Bob");
        LOG.info("=== testDeleteSync PASSED ===");
    }

    @Test
    public void testMultipleTableSync() throws Exception {
        startFlinkPipeline("whole-db-sync-it");
        waitForSinkTable("sink_users", 60);
        waitForSinkTable("sink_orders", 60);

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
        try (Connection c = getSourceConnection();
                Statement st = c.createStatement()) {
            st.executeUpdate(
                    "INSERT INTO users (name, email, age) VALUES "
                            + "('Alice', 'alice@example.com', 25),"
                            + "('Bob', 'bob@example.com', 30),"
                            + "('Charlie', 'charlie@example.com', 35)");
        }

        startFlinkPipeline("whole-db-sync-it");
        waitForSinkTable("sink_users", 60);

        waitForSinkRowCount("sink_users", 3, 60);
        assertSinkRow("sink_users", "name", "Alice");
        assertSinkRow("sink_users", "name", "Bob");
        assertSinkRow("sink_users", "name", "Charlie");
        LOG.info("=== testPreExistingDataSync PASSED ===");
    }

    private void assertEquals(String msg, int expected, int actual) {
        org.junit.jupiter.api.Assertions.assertEquals(expected, actual, msg);
    }
}
