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

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.TruncateTableEvent;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CdcEventDeserializerTest {

    private CdcEventDeserializer deserializer;

    private static final Schema SOURCE_SCHEMA =
            SchemaBuilder.struct()
                    .field("db", Schema.STRING_SCHEMA)
                    .field("table", Schema.STRING_SCHEMA)
                    .build();

    private static final Schema BEFORE_AFTER_SCHEMA =
            SchemaBuilder.struct()
                    .field("id", Schema.INT64_SCHEMA)
                    .field("name", Schema.STRING_SCHEMA)
                    .build();

    private static final Schema VALUE_SCHEMA =
            SchemaBuilder.struct()
                    .field("op", Schema.STRING_SCHEMA)
                    .field("source", SOURCE_SCHEMA)
                    .field("before", BEFORE_AFTER_SCHEMA)
                    .field("after", BEFORE_AFTER_SCHEMA)
                    .build();

    @BeforeEach
    void setUp() {
        deserializer = new CdcEventDeserializer();
        SharedSchemaState.schemas().clear();
        SharedSchemaState.pks().clear();
    }

    // --- convertValue tests ---

    @Test
    void convertValue_null_returnsNull() {
        assertNull(CdcEventDeserializer.convertValue(null));
    }

    @Test
    void convertValue_offsetDateTime_returnsTimestamp() {
        OffsetDateTime odt = OffsetDateTime.of(2024, 6, 15, 10, 30, 0, 0, ZoneOffset.ofHours(8));
        Object result = CdcEventDeserializer.convertValue(odt);
        assertTrue(result instanceof java.sql.Timestamp);
        assertEquals(odt.toInstant(), ((java.sql.Timestamp) result).toInstant());
    }

    @Test
    void convertValue_zonedDateTime_returnsTimestamp() {
        ZonedDateTime zdt = ZonedDateTime.of(2024, 6, 15, 10, 30, 0, 0, ZoneOffset.UTC);
        Object result = CdcEventDeserializer.convertValue(zdt);
        assertTrue(result instanceof java.sql.Timestamp);
        assertEquals(zdt.toInstant(), ((java.sql.Timestamp) result).toInstant());
    }

    @Test
    void convertValue_instant_returnsTimestamp() {
        Instant instant = Instant.parse("2024-06-15T10:30:00Z");
        Object result = CdcEventDeserializer.convertValue(instant);
        assertTrue(result instanceof java.sql.Timestamp);
        assertEquals(instant, ((java.sql.Timestamp) result).toInstant());
    }

    @Test
    void convertValue_localDateTime_returnsTimestamp() {
        LocalDateTime ldt = LocalDateTime.of(2024, 6, 15, 10, 30, 0);
        Object result = CdcEventDeserializer.convertValue(ldt);
        assertTrue(result instanceof java.sql.Timestamp);
        assertEquals(ldt, ((java.sql.Timestamp) result).toLocalDateTime());
    }

    @Test
    void convertValue_utilDate_returnsTimestamp() {
        java.util.Date date = new java.util.Date(1718450000000L);
        Object result = CdcEventDeserializer.convertValue(date);
        assertTrue(result instanceof java.sql.Timestamp);
        assertEquals(date.getTime(), ((java.sql.Timestamp) result).getTime());
    }

    @Test
    void convertValue_isoStringWithT_returnsTimestamp() {
        Object result = CdcEventDeserializer.convertValue("2024-06-15T10:30:00Z");
        assertTrue(result instanceof java.sql.Timestamp);
        assertEquals(
                Instant.parse("2024-06-15T10:30:00Z"), ((java.sql.Timestamp) result).toInstant());
    }

    @Test
    void convertValue_nonIsoString_returnsOriginal() {
        String value = "hello world";
        assertSame(value, CdcEventDeserializer.convertValue(value));
    }

    @Test
    void convertValue_number_returnsOriginal() {
        assertEquals(42, CdcEventDeserializer.convertValue(42));
        assertEquals(3.14, CdcEventDeserializer.convertValue(3.14));
    }

    // --- extractValues tests ---

    @Test
    void extractValues_null_returnsNull() {
        assertNull(CdcEventDeserializer.extractValues(null));
    }

    @Test
    void extractValues_extractsAllFields() {
        Schema schema =
                SchemaBuilder.struct()
                        .field("id", Schema.INT64_SCHEMA)
                        .field("name", Schema.STRING_SCHEMA)
                        .build();
        Struct row = new Struct(schema);
        row.put("id", 1L);
        row.put("name", "Alice");

        Object[] values = CdcEventDeserializer.extractValues(row);
        assertNotNull(values);
        assertEquals(2, values.length);
        assertEquals(1L, values[0]);
        assertEquals("Alice", values[1]);
    }

    // --- deserialize DML events ---

    @Test
    void deserialize_insertEvent() throws Exception {
        SourceRecord record = createDMLRecord("c", 1L, null, "Alice");
        List<Event> events = collectEvents(record);

        // First encounter emits CreateTableEvent + DataChangeEvent
        assertEquals(2, events.size());
        assertTrue(events.get(0) instanceof CreateTableEvent);
        assertTrue(events.get(1) instanceof DataChangeEvent);
        DataChangeEvent dce = (DataChangeEvent) events.get(1);
        assertEquals(OperationType.INSERT, dce.op());
        assertEquals("test_table", dce.tableId().getTableName());
    }

    @Test
    void deserialize_readEvent_alsoMapsToInsert() throws Exception {
        SourceRecord record = createDMLRecord("r", 1L, null, "Bob");
        List<Event> events = collectEvents(record);

        assertEquals(2, events.size());
        assertTrue(events.get(0) instanceof CreateTableEvent);
        assertTrue(events.get(1) instanceof DataChangeEvent);
        DataChangeEvent dce = (DataChangeEvent) events.get(1);
        assertEquals(OperationType.INSERT, dce.op());
    }

    @Test
    void deserialize_updateEvent() throws Exception {
        SourceRecord record = createDMLRecord("u", 1L, "Alice", "AliceUpdated");
        List<Event> events = collectEvents(record);

        assertEquals(2, events.size());
        assertTrue(events.get(0) instanceof CreateTableEvent);
        assertTrue(events.get(1) instanceof DataChangeEvent);
        DataChangeEvent dce = (DataChangeEvent) events.get(1);
        assertEquals(OperationType.UPDATE, dce.op());
    }

    @Test
    void deserialize_deleteEvent() throws Exception {
        SourceRecord record = createDMLRecord("d", 1L, "Alice", null);
        List<Event> events = collectEvents(record);

        assertEquals(2, events.size());
        assertTrue(events.get(0) instanceof CreateTableEvent);
        assertTrue(events.get(1) instanceof DataChangeEvent);
        DataChangeEvent dce = (DataChangeEvent) events.get(1);
        assertEquals(OperationType.DELETE, dce.op());
    }

    @Test
    void deserialize_truncateEvent() throws Exception {
        SourceRecord record = createDMLRecord("t", null, null, null);
        List<Event> events = collectEvents(record);

        // TRUNCATE has no before/after, so no CreateTableEvent is emitted
        assertEquals(1, events.size());
        assertTrue(events.get(0) instanceof TruncateTableEvent);
    }

    @Test
    void deserialize_unknownOp_emitsNothing() throws Exception {
        SourceRecord record = createDMLRecord("x", null, null, null);
        List<Event> events = collectEvents(record);

        assertEquals(0, events.size());
    }

    @Test
    void deserialize_nullRecord_emitsNothing() throws Exception {
        List<Event> events = collectEvents(null);
        assertEquals(0, events.size());
    }

    @Test
    void deserialize_nullValue_emitsNothing() throws Exception {
        SourceRecord record =
                new SourceRecord(
                        Collections.singletonMap("topic", "test"),
                        Collections.singletonMap("partition", 0),
                        "test",
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        List<Event> events = collectEvents(record);
        assertEquals(0, events.size());
    }

    // --- helpers ---

    private SourceRecord createDMLRecord(
            final String op, final Long id, final String beforeName, final String afterName) {
        Struct source = new Struct(SOURCE_SCHEMA);
        source.put("db", "testdb");
        source.put("table", "test_table");

        Struct value = new Struct(VALUE_SCHEMA);
        value.put("op", op);
        value.put("source", source);

        if (beforeName != null) {
            Struct before = new Struct(BEFORE_AFTER_SCHEMA);
            before.put("id", id);
            before.put("name", beforeName);
            value.put("before", before);
        }

        if (afterName != null) {
            Struct after = new Struct(BEFORE_AFTER_SCHEMA);
            after.put("id", id);
            after.put("name", afterName);
            value.put("after", after);
        }

        return new SourceRecord(
                Collections.singletonMap("topic", "test"),
                Collections.singletonMap("partition", 0),
                "test",
                null,
                null,
                null,
                VALUE_SCHEMA,
                value,
                null,
                null);
    }

    private List<Event> collectEvents(final SourceRecord record) throws Exception {
        List<Event> events = new ArrayList<>();
        deserializer.deserialize(
                record,
                new org.apache.flink.util.Collector<Event>() {
                    @Override
                    public void collect(Event event) {
                        events.add(event);
                    }

                    @Override
                    public void close() {}
                });
        return events;
    }
}
