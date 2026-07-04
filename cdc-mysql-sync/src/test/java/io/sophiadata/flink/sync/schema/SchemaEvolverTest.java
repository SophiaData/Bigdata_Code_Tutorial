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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SchemaEvolverTest {

    // --- mapToMysqlType (delegates to MysqlUtil.mapType) ---

    @Test
    void mapToMysqlType_text_returnsVarchar() {
        SchemaEvolver evolver = createEvolver();
        assertEquals("VARCHAR(1024)", invokeMapToMysqlType(evolver, "TEXT"));
    }

    @Test
    void mapToMysqlType_bigint_returnsBigint() {
        SchemaEvolver evolver = createEvolver();
        assertEquals("BIGINT", invokeMapToMysqlType(evolver, "BIGINT"));
    }

    @Test
    void mapToMysqlType_int_returnsInt() {
        SchemaEvolver evolver = createEvolver();
        assertEquals("INT", invokeMapToMysqlType(evolver, "INT"));
    }

    @Test
    void mapToMysqlType_boolean_returnsTinyInt() {
        SchemaEvolver evolver = createEvolver();
        assertEquals("TINYINT(1)", invokeMapToMysqlType(evolver, "BOOLEAN"));
    }

    @Test
    void mapToMysqlType_timestamp_returnsTimestamp6() {
        SchemaEvolver evolver = createEvolver();
        assertEquals("TIMESTAMP(6)", invokeMapToMysqlType(evolver, "TIMESTAMP"));
    }

    @Test
    void mapToMysqlType_datetime_returnsDatetime6() {
        SchemaEvolver evolver = createEvolver();
        assertEquals("DATETIME(6)", invokeMapToMysqlType(evolver, "DATETIME"));
    }

    @Test
    void mapToMysqlType_varcharWithPrecision_preservesPrecision() {
        SchemaEvolver evolver = createEvolver();
        assertEquals("VARCHAR(100)", invokeMapToMysqlType(evolver, "VARCHAR(100)"));
    }

    @Test
    void mapToMysqlType_decimalWithPrecision_preservesPrecision() {
        SchemaEvolver evolver = createEvolver();
        assertEquals("DECIMAL(10,2)", invokeMapToMysqlType(evolver, "DECIMAL(10,2)"));
    }

    @Test
    void mapToMysqlType_unknownType_returnsVarchar1024() {
        SchemaEvolver evolver = createEvolver();
        assertEquals("VARCHAR(1024)", invokeMapToMysqlType(evolver, "UNKNOWN_TYPE"));
    }

    @Test
    void mapToMysqlType_caseInsensitive() {
        SchemaEvolver evolver = createEvolver();
        assertEquals("BIGINT", invokeMapToMysqlType(evolver, "bigint"));
        assertEquals("INT", invokeMapToMysqlType(evolver, "int"));
    }

    // --- Constructor and basic properties ---

    @Test
    void constructor_withTablePrefix() {
        SchemaEvolver evolver =
                new SchemaEvolver("jdbc:mysql://localhost:3306/test", "root", "pass", "sink_");
        assertNotNull(evolver);
    }

    @Test
    void constructor_withoutTablePrefix() {
        SchemaEvolver evolver =
                new SchemaEvolver("jdbc:mysql://localhost:3306/test", "root", "pass");
        assertNotNull(evolver);
    }

    // --- processEvent dispatching ---

    @Test
    void processEvent_createTable_doesNotThrow() {
        SchemaEvolver evolver = createEvolver();
        CreateTableEvent event =
                new CreateTableEvent(
                        TableId.tableId("testdb", "test_table"),
                        Schema.newBuilder()
                                .column(Column.physicalColumn("id", DataTypes.BIGINT()))
                                .column(Column.physicalColumn("name", DataTypes.VARCHAR(100)))
                                .primaryKey(Collections.singletonList("id"))
                                .build());
        evolver.processEvent(event);
    }

    @Test
    void processEvent_addColumn_doesNotThrow() {
        SchemaEvolver evolver = createEvolver();
        AddColumnEvent event =
                new AddColumnEvent(
                        TableId.tableId("testdb", "test_table"),
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("new_col", DataTypes.INT()))));
        evolver.processEvent(event);
    }

    @Test
    void processEvent_dropColumn_doesNotThrow() {
        SchemaEvolver evolver = createEvolver();
        DropColumnEvent event =
                new DropColumnEvent(
                        TableId.tableId("testdb", "test_table"),
                        Collections.singletonList("old_col"));
        evolver.processEvent(event);
    }

    @Test
    void processEvent_renameColumn_doesNotThrow() {
        SchemaEvolver evolver = createEvolver();
        Map<String, String> nameMapping = new LinkedHashMap<>();
        nameMapping.put("old_name", "new_name");
        RenameColumnEvent event =
                new RenameColumnEvent(TableId.tableId("testdb", "test_table"), nameMapping);
        evolver.processEvent(event);
    }

    @Test
    void processEvent_dropTable_doesNotThrow() {
        SchemaEvolver evolver = createEvolver();
        DropTableEvent event = new DropTableEvent(TableId.tableId("testdb", "test_table"));
        evolver.processEvent(event);
    }

    @Test
    void processEvent_truncateTable_doesNotThrow() {
        SchemaEvolver evolver = createEvolver();
        TruncateTableEvent event = new TruncateTableEvent(TableId.tableId("testdb", "test_table"));
        evolver.processEvent(event);
    }

    // --- shutdown ---

    @Test
    void shutdown_doesNotThrow() {
        SchemaEvolver evolver = createEvolver();
        evolver.shutdown();
    }

    // --- thread safety ---

    @Test
    void processEvent_concurrentEvents_doNotThrow() throws InterruptedException {
        SchemaEvolver evolver = createEvolver();
        Thread t1 =
                new Thread(
                        () -> {
                            for (int i = 0; i < 10; i++) {
                                evolver.processEvent(
                                        new CreateTableEvent(
                                                TableId.tableId("testdb", "t1"),
                                                Schema.newBuilder()
                                                        .column(
                                                                Column.physicalColumn(
                                                                        "id", DataTypes.BIGINT()))
                                                        .primaryKey(Collections.singletonList("id"))
                                                        .build()));
                            }
                        });
        Thread t2 =
                new Thread(
                        () -> {
                            for (int i = 0; i < 10; i++) {
                                evolver.processEvent(
                                        new CreateTableEvent(
                                                TableId.tableId("testdb", "t2"),
                                                Schema.newBuilder()
                                                        .column(
                                                                Column.physicalColumn(
                                                                        "id", DataTypes.BIGINT()))
                                                        .primaryKey(Collections.singletonList("id"))
                                                        .build()));
                            }
                        });
        t1.start();
        t2.start();
        t1.join(5000);
        t2.join(5000);
        // No exception means thread safety is working
        evolver.shutdown();
    }

    // --- helpers ---

    private SchemaEvolver createEvolver() {
        return new SchemaEvolver("jdbc:mysql://localhost:3306/test", "root", "pass", "sink_");
    }

    /**
     * Invoke the private mapToMysqlType method via reflection for unit testing. Since
     * mapToMysqlType now delegates to MysqlUtil.mapType(), this validates the delegation.
     */
    private String invokeMapToMysqlType(SchemaEvolver evolver, String cdcType) {
        try {
            java.lang.reflect.Method method =
                    SchemaEvolver.class.getDeclaredMethod("mapToMysqlType", String.class);
            method.setAccessible(true);
            return (String) method.invoke(evolver, cdcType);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke mapToMysqlType", e);
        }
    }
}
