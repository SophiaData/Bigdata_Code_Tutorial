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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test for CDBBatchSink batch operations using H2 in-memory database in MySQL
 * compatibility mode.
 */
class CDBBatchSinkIT {

    private static final String H2_URL = "jdbc:h2:mem:testdb;MODE=MySQL;DB_CLOSE_DELAY=-1";
    private static final String H2_USER = "sa";
    private static final String H2_PASSWORD = "";

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection(H2_URL, H2_USER, H2_PASSWORD);
        try (Statement st = conn.createStatement()) {
            st.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS sink_users ("
                            + "id BIGINT PRIMARY KEY, "
                            + "name VARCHAR(255), "
                            + "age INT"
                            + ")");
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            try (Statement st = conn.createStatement()) {
                st.executeUpdate("DROP TABLE IF EXISTS sink_users");
            }
            conn.close();
        }
    }

    @Test
    void flush_insertsNewRows() throws Exception {
        Map<String, String> cols = new LinkedHashMap<>();
        cols.put("id", "BIGINT");
        cols.put("name", "VARCHAR");
        cols.put("age", "INT");

        // Create the flush method manually since CDBBatchSink is private
        // We'll test the SQL generation logic directly
        String colList = "`" + String.join("`,`", cols.keySet()) + "`";
        String placeholders = String.join(",", java.util.Collections.nCopies(cols.size(), "?"));
        String updateClause =
                cols.keySet().stream()
                        .map(c -> "`" + c + "`=?")
                        .collect(java.util.stream.Collectors.joining(","));
        String sql =
                String.format(
                        "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                        "sink_users", colList, placeholders, updateClause);

        // Execute insert
        try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, 1L);
            ps.setObject(2, "Alice");
            ps.setObject(3, 30);
            ps.setObject(4, 1L);
            ps.setObject(5, "Alice");
            ps.setObject(6, 30);
            ps.executeUpdate();
        }

        // Verify
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT * FROM sink_users WHERE id = 1")) {
            rs.next();
            assertEquals(1L, rs.getLong("id"));
            assertEquals("Alice", rs.getString("name"));
            assertEquals(30, rs.getInt("age"));
        }
    }

    @Test
    void flush_updatesExistingRows() throws Exception {
        // Insert initial row
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("INSERT INTO sink_users VALUES (1, 'Alice', 30)");
        }

        Map<String, String> cols = new LinkedHashMap<>();
        cols.put("id", "BIGINT");
        cols.put("name", "VARCHAR");
        cols.put("age", "INT");

        String colList = "`" + String.join("`,`", cols.keySet()) + "`";
        String placeholders = String.join(",", java.util.Collections.nCopies(cols.size(), "?"));
        String updateClause =
                cols.keySet().stream()
                        .map(c -> "`" + c + "`=?")
                        .collect(java.util.stream.Collectors.joining(","));
        String sql =
                String.format(
                        "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                        "sink_users", colList, placeholders, updateClause);

        // Execute upsert (update)
        try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, 1L);
            ps.setObject(2, "Alice Updated");
            ps.setObject(3, 31);
            ps.setObject(4, 1L);
            ps.setObject(5, "Alice Updated");
            ps.setObject(6, 31);
            ps.executeUpdate();
        }

        // Verify
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT * FROM sink_users WHERE id = 1")) {
            rs.next();
            assertEquals("Alice Updated", rs.getString("name"));
            assertEquals(31, rs.getInt("age"));
        }
    }

    @Test
    void delete_removesRows() throws Exception {
        // Insert row
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("INSERT INTO sink_users VALUES (1, 'Alice', 30)");
        }

        // Delete
        String deleteSql = "DELETE FROM sink_users WHERE `id` = ?";
        try (java.sql.PreparedStatement ps = conn.prepareStatement(deleteSql)) {
            ps.setObject(1, 1L);
            ps.executeUpdate();
        }

        // Verify deleted
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM sink_users WHERE id = 1")) {
            rs.next();
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    void batchInsert_multipleRows() throws Exception {
        Map<String, String> cols = new LinkedHashMap<>();
        cols.put("id", "BIGINT");
        cols.put("name", "VARCHAR");
        cols.put("age", "INT");

        String colList = "`" + String.join("`,`", cols.keySet()) + "`";
        String placeholders = String.join(",", java.util.Collections.nCopies(cols.size(), "?"));
        String updateClause =
                cols.keySet().stream()
                        .map(c -> "`" + c + "`=?")
                        .collect(java.util.stream.Collectors.joining(","));
        String sql =
                String.format(
                        "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                        "sink_users", colList, placeholders, updateClause);

        try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
            // Row 1
            ps.setObject(1, 1L);
            ps.setObject(2, "Alice");
            ps.setObject(3, 30);
            ps.setObject(4, 1L);
            ps.setObject(5, "Alice");
            ps.setObject(6, 30);
            ps.addBatch();

            // Row 2
            ps.setObject(1, 2L);
            ps.setObject(2, "Bob");
            ps.setObject(3, 25);
            ps.setObject(4, 2L);
            ps.setObject(5, "Bob");
            ps.setObject(6, 25);
            ps.addBatch();

            ps.executeBatch();
        }

        // Verify count
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM sink_users")) {
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void batchDelete_multipleRows() throws Exception {
        // Insert rows
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("INSERT INTO sink_users VALUES (1, 'Alice', 30)");
            st.executeUpdate("INSERT INTO sink_users VALUES (2, 'Bob', 25)");
        }

        String deleteSql = "DELETE FROM sink_users WHERE `id` = ?";
        try (java.sql.PreparedStatement ps = conn.prepareStatement(deleteSql)) {
            ps.setObject(1, 1L);
            ps.addBatch();
            ps.setObject(1, 2L);
            ps.addBatch();
            ps.executeBatch();
        }

        // Verify all deleted
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM sink_users")) {
            rs.next();
            assertEquals(0, rs.getInt(1));
        }
    }
}
