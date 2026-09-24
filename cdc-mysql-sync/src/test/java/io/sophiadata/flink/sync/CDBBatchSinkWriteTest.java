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

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Write-path tests for {@link CDBBatchSink}.
 *
 * <p>The existing suite covers {@code Record}, {@code groupByTable} and {@code splitByOperation}
 * but never reaches the SQL the sink actually issues, which is where the risk is: an upsert that
 * omits a column, or a delete that targets the wrong key, silently writes wrong data.
 *
 * <p>{@code CDBBatchSink#open} hardcodes the MySQL driver, so it cannot be pointed at H2. These
 * tests instead inject an H2 connection into the private {@code conn} field and drive {@code
 * flush()} through reflection, validating the generated statement by executing it. H2 runs in MySQL
 * compatibility mode because the statements use backtick-quoted identifiers and {@code INSERT ...
 * ON DUPLICATE KEY UPDATE}.
 */
class CDBBatchSinkWriteTest {

    private static final String TABLE = "users";
    private static final String PREFIXED = "sink_" + TABLE;

    private Connection inspection;
    private CDBBatchSink sink;

    @BeforeEach
    void setUp() throws Exception {
        String url = "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DB_CLOSE_DELAY=-1";
        inspection = DriverManager.getConnection(url, "sa", "");
        sink = new CDBBatchSink(url, "sa", "", 100, 1000);

        try (Statement statement = inspection.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE " + PREFIXED + " (id BIGINT PRIMARY KEY, name VARCHAR(100))");
        }

        injectConnection(sink, DriverManager.getConnection(url, "sa", ""));
        registerSchema(TABLE, "id", "id", "name");

        // The sink's own connection must not autocommit, matching how open() configures it.
        Connection sinkConnection = (Connection) readField(sink, "conn");
        sinkConnection.setAutoCommit(false);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (inspection != null && !inspection.isClosed()) {
            inspection.close();
        }
    }

    @Test
    void upsertInsertsANewRow() throws Exception {
        queue(insertEvent(1L, "alice"));

        flush();

        assertEquals("alice", nameOf(1L), "the insert should have reached the sink table");
    }

    @Test
    void upsertUpdatesAnExistingRow() throws Exception {
        queue(insertEvent(1L, "alice"));
        flush();
        queue(insertEvent(1L, "alice-updated"));

        flush();

        assertEquals(
                "alice-updated",
                nameOf(1L),
                "a second row with the same key should update, not duplicate");
        assertEquals(1, rowCount(), "the upsert must not insert a duplicate row");
    }

    @Test
    void upsertBindsEveryColumn() throws Exception {
        // A statement missing a column would either fail to bind or write the wrong value; checking
        // both columns guards against an off-by-one in the placeholder list.
        queue(insertEvent(7L, "seven"));

        flush();

        try (Statement statement = inspection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                "SELECT id, name FROM " + PREFIXED + " WHERE id = 7")) {
            assertTrue(rs.next(), "the row should exist");
            assertEquals(7L, rs.getLong("id"));
            assertEquals("seven", rs.getString("name"));
        }
    }

    @Test
    void deleteRemovesTheRowByIdentifiedKey() throws Exception {
        queue(insertEvent(2L, "bob"));
        flush();

        queue(deleteEvent(2L, "bob"));
        flush();

        assertEquals(0, rowCount(), "the delete should have removed the row");
    }

    @Test
    void deleteForUnknownKeyLeavesTableUntouched() throws Exception {
        queue(insertEvent(3L, "carol"));
        flush();

        queue(deleteEvent(99L, "nobody"));
        flush();

        assertEquals(1, rowCount(), "deleting a missing key must not affect other rows");
    }

    @Test
    void deleteIsSkippedWhenTheTableHasNoPrimaryKey() throws Exception {
        // Without a primary key the sink cannot identify a row, so it logs and skips. The row must
        // survive rather than the flush failing.
        registerSchema(TABLE, null, "id", "name");
        queue(insertEvent(4L, "dave"));
        flush();

        queue(deleteEvent(4L, "dave"));
        flush();

        assertEquals(1, rowCount(), "the row should survive because the delete is skipped");
    }

    @Test
    void mixedBatchAppliesUpsertsAndDeletesTogether() throws Exception {
        queue(insertEvent(10L, "ten"));
        flush();

        queue(insertEvent(11L, "eleven"));
        queue(deleteEvent(10L, "ten"));
        flush();

        assertEquals(1, rowCount(), "one insert and one delete should net to a single row");
        assertEquals("eleven", nameOf(11L));
    }

    @Test
    void nullColumnValueIsWrittenAsNull() throws Exception {
        queue(insertEvent(12L, null));

        flush();

        try (Statement statement = inspection.createStatement();
                ResultSet rs =
                        statement.executeQuery("SELECT name FROM " + PREFIXED + " WHERE id = 12")) {
            assertTrue(rs.next(), "the row should exist");
            rs.getString("name");
            assertTrue(rs.wasNull(), "a null column should be stored as SQL NULL");
        }
    }

    // --- helpers -------------------------------------------------------------

    private DataChangeEvent insertEvent(final Long id, final String name) {
        return DataChangeEvent.insertEvent(
                TableId.tableId("testdb", TABLE),
                org.apache.flink.cdc.common.data.GenericRecordData.of(new Object[] {id, name}));
    }

    private DataChangeEvent deleteEvent(final Long id, final String name) {
        return DataChangeEvent.deleteEvent(
                TableId.tableId("testdb", TABLE),
                org.apache.flink.cdc.common.data.GenericRecordData.of(new Object[] {id, name}));
    }

    /** Puts one record in the sink's batch without triggering the automatic flush in invoke(). */
    private void queue(final DataChangeEvent event) throws Exception {
        @SuppressWarnings("unchecked")
        java.util.List<CDBBatchSink.Record> batch =
                (java.util.List<CDBBatchSink.Record>) readField(sink, "batch");
        batch.add(new CDBBatchSink.Record(event));
    }

    private void flush() throws Exception {
        Method flush = CDBBatchSink.class.getDeclaredMethod("flush");
        flush.setAccessible(true);
        flush.invoke(sink);
    }

    /**
     * The sink resolves schemas through {@link SharedSchemaState}, so the column list and primary
     * key have to be published there rather than on the instance.
     */
    private void registerSchema(
            final String table, final String primaryKey, final String... columns) {
        Map<String, String> columnTypes = new LinkedHashMap<>();
        for (String column : columns) {
            columnTypes.put(column, "BIGINT".equals(column) ? "BIGINT" : "VARCHAR(100)");
        }
        SharedSchemaState.schemas().put(table, columnTypes);
        if (primaryKey == null) {
            SharedSchemaState.pks().remove(table);
        } else {
            SharedSchemaState.pks().put(table, primaryKey);
        }
    }

    private void injectConnection(final CDBBatchSink target, final Connection connection)
            throws Exception {
        Field field = CDBBatchSink.class.getDeclaredField("conn");
        field.setAccessible(true);
        field.set(target, connection);
        Field batchField = CDBBatchSink.class.getDeclaredField("batch");
        batchField.setAccessible(true);
        batchField.set(target, new java.util.ArrayList<CDBBatchSink.Record>());
    }

    private Object readField(final CDBBatchSink target, final String name) throws Exception {
        Field field = CDBBatchSink.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private int rowCount() throws SQLException {
        try (Statement statement = inspection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + PREFIXED)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private String nameOf(final long id) throws SQLException {
        try (Statement statement = inspection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                "SELECT name FROM " + PREFIXED + " WHERE id=" + id)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }
}
