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

import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.TableId;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CDBBatchSinkTest {

    private CDBBatchSink sink;

    @BeforeEach
    void setUp() {
        sink = new CDBBatchSink("jdbc:mysql://localhost:3306/test", "root", "pass", 100, 1000);
    }

    // --- Record.extractValues tests ---

    @Test
    void record_extractValues_nullRow_returnsNull() {
        Object[] values = CDBBatchSink.Record.extractValues(null);
        assertNull(values);
    }

    @Test
    void record_extractValues_extractsAllFields() {
        Object[] row = new Object[] {1L, "Alice", 30};
        GenericRecordData recordData = GenericRecordData.of(row);

        Object[] values = CDBBatchSink.Record.extractValues(recordData);
        assertNotNull(values);
        assertEquals(3, values.length);
        assertEquals(1L, values[0]);
        assertEquals("Alice", values[1]);
        assertEquals(30, values[2]);
    }

    // --- groupByTable tests ---

    @Test
    void groupByTable_groupsRecordsByTableName() {
        TableId tid1 = TableId.tableId("db", "table1");
        TableId tid2 = TableId.tableId("db", "table2");

        CDBBatchSink.Record r1 =
                new CDBBatchSink.Record(
                        DataChangeEvent.insertEvent(tid1, GenericRecordData.of(new Object[] {1L})));
        CDBBatchSink.Record r2 =
                new CDBBatchSink.Record(
                        DataChangeEvent.insertEvent(tid2, GenericRecordData.of(new Object[] {2L})));
        CDBBatchSink.Record r3 =
                new CDBBatchSink.Record(
                        DataChangeEvent.insertEvent(tid1, GenericRecordData.of(new Object[] {3L})));

        List<CDBBatchSink.Record> records = Arrays.asList(r1, r2, r3);
        Map<String, List<CDBBatchSink.Record>> grouped = sink.groupByTable(records);

        assertEquals(2, grouped.size());
        assertEquals(2, grouped.get("table1").size());
        assertEquals(1, grouped.get("table2").size());
    }

    @Test
    void groupByTable_emptyList_returnsEmptyMap() {
        Map<String, List<CDBBatchSink.Record>> grouped = sink.groupByTable(new ArrayList<>());
        assertTrue(grouped.isEmpty());
    }

    // --- splitByOperation tests ---

    @Test
    void splitByOperation_separatesUpsertsAndDeletes() {
        TableId tid = TableId.tableId("db", "table");

        CDBBatchSink.Record insert =
                new CDBBatchSink.Record(
                        DataChangeEvent.insertEvent(tid, GenericRecordData.of(new Object[] {1L})));
        CDBBatchSink.Record update =
                new CDBBatchSink.Record(
                        DataChangeEvent.updateEvent(
                                tid,
                                GenericRecordData.of(new Object[] {1L}),
                                GenericRecordData.of(new Object[] {2L})));
        CDBBatchSink.Record delete =
                new CDBBatchSink.Record(
                        DataChangeEvent.deleteEvent(tid, GenericRecordData.of(new Object[] {3L})));

        List<CDBBatchSink.Record> upserts = new ArrayList<>();
        List<CDBBatchSink.Record> deletes = new ArrayList<>();
        sink.splitByOperation(Arrays.asList(insert, update, delete), upserts, deletes);

        assertEquals(2, upserts.size());
        assertEquals(1, deletes.size());
        assertEquals(OperationType.DELETE, deletes.get(0).op);
    }

    @Test
    void splitByOperation_allUpserts_noDeletes() {
        TableId tid = TableId.tableId("db", "table");
        CDBBatchSink.Record insert =
                new CDBBatchSink.Record(
                        DataChangeEvent.insertEvent(tid, GenericRecordData.of(new Object[] {1L})));

        List<CDBBatchSink.Record> upserts = new ArrayList<>();
        List<CDBBatchSink.Record> deletes = new ArrayList<>();
        sink.splitByOperation(Arrays.asList(insert), upserts, deletes);

        assertEquals(1, upserts.size());
        assertEquals(0, deletes.size());
    }
}
