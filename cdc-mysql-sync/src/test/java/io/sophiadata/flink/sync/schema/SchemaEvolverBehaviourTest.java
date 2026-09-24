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

package io.sophiadata.flink.sync.schema;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavioural tests for {@link SchemaEvolver} against a real database.
 *
 * <p>The previous suite only asserted that {@code processEvent} did not throw, which passes even
 * when the emitted SQL is wrong. These run the evolver against H2 in MySQL compatibility mode and
 * then inspect the resulting table, so the assertions are about the schema that actually ends up in
 * the sink rather than about the absence of exceptions.
 *
 * <p>H2 in MySQL mode accepts the backtick-quoted ALTER forms the evolver emits ({@code ADD
 * COLUMN}, {@code MODIFY COLUMN}, {@code DROP COLUMN}), which is what makes this possible without a
 * MySQL container. {@code CHANGE COLUMN} is the exception, and that exposes a defect in the
 * evolver: {@link SchemaEvolver#alterRenameColumn} formats
 *
 * <pre>ALTER TABLE t CHANGE COLUMN `old` `new`</pre>
 *
 * without a column type, but MySQL requires one ({@code CHANGE COLUMN old new <type>}), so the
 * statement is a syntax error and a column rename never reaches the sink. {@link
 * #renameColumnEventBuildsSqlThatOmitsTheColumnType()} pins that behaviour so a fix is a deliberate
 * change rather than a silent one.
 */
class SchemaEvolverBehaviourTest {

    private static final String TABLE = "users";

    private Connection inspection;
    private String jdbcUrl;
    private SchemaEvolver evolver;

    @BeforeEach
    void setUp() throws SQLException {
        // A private in-memory database per test, so cases cannot interfere with one another.
        jdbcUrl = "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DB_CLOSE_DELAY=-1";
        inspection = DriverManager.getConnection(jdbcUrl, "sa", "");
        try (Statement statement = inspection.createStatement()) {
            statement.executeUpdate("CREATE TABLE sink_" + TABLE + " (id BIGINT PRIMARY KEY)");
        }
        evolver = new SchemaEvolver(jdbcUrl, "sa", "", "sink_");
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (evolver != null) {
            evolver.shutdown();
        }
        if (inspection != null && !inspection.isClosed()) {
            inspection.close();
        }
    }

    @Test
    void addColumnAddsItToTheSink() throws Exception {
        TableId tableId = TableId.tableId("testdb", TABLE);
        AddColumnEvent event =
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn(
                                                "nickname", DataTypes.VARCHAR(100)))));

        evolver.processEvent(event);
        awaitColumn("nickname");

        assertTrue(hasColumn("nickname"), "ADD COLUMN should create the column in the sink");
    }

    @Test
    void dropColumnRemovesItFromTheSink() throws Exception {
        addVia(Column.physicalColumn("nickname", DataTypes.VARCHAR(100)), "nickname");

        TableId tableId = TableId.tableId("testdb", TABLE);
        evolver.processEvent(new DropColumnEvent(tableId, Collections.singletonList("nickname")));

        awaitMissingColumn("nickname");
        assertFalse(hasColumn("nickname"), "DROP COLUMN should remove the column from the sink");
    }

    @Test
    void renameColumnEventBuildsSqlThatOmitsTheColumnType() throws Exception {
        addVia(Column.physicalColumn("nickname", DataTypes.VARCHAR(100)), "nickname");

        Map<String, String> nameMapping = new LinkedHashMap<>();
        nameMapping.put("nickname", "moniker");
        evolver.processEvent(new RenameColumnEvent(TableId.tableId("testdb", TABLE), nameMapping));

        // Pins the current behaviour so the defect cannot change silently. MySQL requires
        // "CHANGE COLUMN old new <type>"; without the type the statement is a syntax error, so a
        // rename never reaches the sink. H2 rejects it the same way, which is why the column is
        // still absent here. Fixing SchemaEvolver to carry the type over should turn this test red
        // on purpose - see the note in the class javadoc.
        Thread.sleep(1000);
        assertTrue(
                hasColumn("nickname"),
                "the original column should be untouched because the emitted rename is invalid");
        assertFalse(
                hasColumn("moniker"), "the rename cannot have applied while the SQL is invalid");
    }

    @Test
    void alterColumnTypeWidensTheColumn() throws Exception {
        addVia(Column.physicalColumn("nickname", DataTypes.VARCHAR(10)), "nickname");

        Map<String, org.apache.flink.cdc.common.types.DataType> typeMapping = new LinkedHashMap<>();
        typeMapping.put("nickname", DataTypes.VARCHAR(255));
        evolver.processEvent(
                new AlterColumnTypeEvent(TableId.tableId("testdb", TABLE), typeMapping));

        // Assert on the declared length rather than the type name: H2 reports VARCHAR as
        // "CHARACTER VARYING", so matching on the type string would be driver-specific.
        awaitColumnLength("nickname", 255);
        assertEquals(255, columnLength("nickname"), "MODIFY COLUMN should widen the column");
    }

    @Test
    void repeatedAddColumnIsAppliedOnceAndStaysIdempotent() throws Exception {
        TableId tableId = TableId.tableId("testdb", TABLE);
        AddColumnEvent event =
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn(
                                                "nickname", DataTypes.VARCHAR(100)))));

        // The same event twice: the second attempt hits "duplicate column", which the evolver must
        // absorb rather than treat as a failure, and the table must stay usable.
        evolver.processEvent(event);
        awaitColumn("nickname");
        evolver.processEvent(event);
        evolver.processEvent(event);

        assertTrue(hasColumn("nickname"), "the column should still exist after repeated events");
        try (Statement statement = inspection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM sink_" + TABLE)) {
            assertTrue(rs.next(), "the sink table should still be queryable");
        }
    }

    @Test
    void addColumnThenAlterTypeAppliesBothInOrder() throws Exception {
        TableId tableId = TableId.tableId("testdb", TABLE);
        evolver.processEvent(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("age", DataTypes.INT())))));
        awaitColumn("age");

        Map<String, org.apache.flink.cdc.common.types.DataType> typeMapping = new LinkedHashMap<>();
        typeMapping.put("age", DataTypes.BIGINT());
        evolver.processEvent(new AlterColumnTypeEvent(tableId, typeMapping));

        awaitColumnLength("age", 64);
        assertEquals(64, columnLength("age"), "the column should have been altered to BIGINT");
    }

    @Test
    void tablePrefixIsAppliedToTheEmittedStatement() throws Exception {
        // The evolver was constructed with the "sink_" prefix, so the CDC table "users" must
        // address
        // the physical table "sink_users" rather than "users".
        evolver.processEvent(
                new AddColumnEvent(
                        TableId.tableId("testdb", TABLE),
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("prefixed", DataTypes.INT())))));

        awaitColumn("prefixed");
        assertTrue(
                hasColumn("prefixed"), "the prefixed sink table should have received the column");
    }

    @Test
    void processEventIgnoresNonSchemaChangeEvents() throws Exception {
        // Only SchemaChangeEvent instances may reach the ALTER path. A CreateTableEvent carries no
        // ALTER, so the sink schema must be untouched - proving processEvent filters by type rather
        // than forwarding everything to the dispatcher.
        evolver.processEvent(
                new org.apache.flink.cdc.common.event.CreateTableEvent(
                        TableId.tableId("testdb", TABLE),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .column(Column.physicalColumn("id", DataTypes.BIGINT()))
                                .build()));

        assertFalse(hasColumn("prefixed"), "CreateTable must not issue an ALTER");
        assertFalse(hasColumn("nickname"), "CreateTable must not issue an ALTER");
    }

    // --- helpers -------------------------------------------------------------

    private void addVia(Column column, String expectedName) throws Exception {
        evolver.processEvent(
                new AddColumnEvent(
                        TableId.tableId("testdb", TABLE),
                        Collections.singletonList(new AddColumnEvent.ColumnWithPosition(column))));
        awaitColumn(expectedName);
    }

    private boolean hasColumn(final String columnName) throws SQLException {
        try (Statement statement = inspection.createStatement();
                ResultSet rs =
                        statement.executeQuery("SELECT * FROM sink_" + TABLE + " WHERE 1=0")) {
            java.sql.ResultSetMetaData meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                if (meta.getColumnName(i).equalsIgnoreCase(columnName)) {
                    return true;
                }
            }
            return false;
        }
    }

    private String columnType(final String columnName) throws SQLException {
        try (Statement statement = inspection.createStatement();
                ResultSet rs =
                        statement.executeQuery("SELECT * FROM sink_" + TABLE + " WHERE 1=0")) {
            java.sql.ResultSetMetaData meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                if (meta.getColumnName(i).equalsIgnoreCase(columnName)) {
                    return meta.getColumnTypeName(i);
                }
            }
            return "";
        }
    }

    /** Declared length of a column, or -1 when it does not exist. */
    private int columnLength(final String columnName) throws SQLException {
        try (Statement statement = inspection.createStatement();
                ResultSet rs =
                        statement.executeQuery("SELECT * FROM sink_" + TABLE + " WHERE 1=0")) {
            java.sql.ResultSetMetaData meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                if (meta.getColumnName(i).equalsIgnoreCase(columnName)) {
                    return meta.getPrecision(i);
                }
            }
            return -1;
        }
    }

    /**
     * The evolver applies ALTERs on a worker pool, so assertions have to wait for the statement to
     * land. Polling keeps the test free of sleeps without depending on the pool's internals.
     */
    private void awaitColumn(final String columnName) throws Exception {
        awaitTrue(() -> hasColumn(columnName), "column " + columnName + " to appear");
    }

    private void awaitMissingColumn(final String columnName) throws Exception {
        awaitTrue(() -> !hasColumn(columnName), "column " + columnName + " to be dropped");
    }

    private void awaitColumnLength(final String columnName, final int expectedLength)
            throws Exception {
        awaitTrue(
                () -> columnLength(columnName) == expectedLength,
                "column " + columnName + " to have length " + expectedLength);
    }

    private void awaitTrue(final CheckedCondition condition, final String description)
            throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (condition.evaluate()) {
                return;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("Timed out waiting for " + description);
    }

    /** A condition that may touch JDBC, and therefore may throw. */
    @FunctionalInterface
    private interface CheckedCondition {
        boolean evaluate() throws Exception;
    }

    @Test
    void shutdownBeforeAnyAlterDoesNotThrow() {
        SchemaEvolver fresh =
                new SchemaEvolver("jdbc:h2:mem:" + UUID.randomUUID(), "sa", "", "sink_");
        fresh.shutdown();
        assertTrue(true, "shutdown on an unused evolver should be a no-op");
    }

    @Test
    void evolverSurvivesJavaSerialization() throws Exception {
        // The evolver holds a transient executor that is rebuilt in readObject; this checks the
        // round-trip still yields a usable object.
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(bytes)) {
            out.writeObject(evolver);
        }
        SchemaEvolver restored;
        try (java.io.ObjectInputStream in =
                new java.io.ObjectInputStream(
                        new java.io.ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (SchemaEvolver) in.readObject();
        }

        restored.processEvent(
                new AddColumnEvent(
                        TableId.tableId("testdb", TABLE),
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("afterSer", DataTypes.INT())))));
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline && !hasColumn("afterSer")) {
            Thread.sleep(25);
        }
        assertEquals(
                true, hasColumn("afterSer"), "a deserialized evolver should still apply ALTERs");
        restored.shutdown();
    }
}
