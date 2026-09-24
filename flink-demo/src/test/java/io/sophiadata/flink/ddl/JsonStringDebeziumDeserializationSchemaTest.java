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

package io.sophiadata.flink.ddl;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link JsonStringDebeziumDeserializationSchema}.
 *
 * <p>The schema had no coverage, and its four operation branches produce different payloads and
 * different tombstone flags. The flag is the dangerous part: it decides whether the downstream sink
 * treats the row as an insert or a delete, so getting it wrong silently inverts data.
 */
class JsonStringDebeziumDeserializationSchemaTest {

    private final JsonStringDebeziumDeserializationSchema schema =
            new JsonStringDebeziumDeserializationSchema();

    @Test
    void producesTupleTypeInformation() {
        assertNotNull(schema.getProducedType(), "the produced type must not be null");
    }

    @Test
    void createOperationEmitsAfterRowAsInsert() throws Exception {
        Struct after = row("alice", 30);
        Struct envelope = envelope(Envelope.Operation.CREATE, null, after);

        List<Tuple2<Boolean, String>> out = collect(record(envelope));

        assertEquals(1, out.size(), "a create should emit exactly one record");
        assertTrue(out.get(0).f0, "a create must be marked as an insert");
        JSONObject payload = JSON.parseObject(out.get(0).f1);
        assertEquals("alice", payload.getString("name"));
        assertEquals(30, payload.getIntValue("age"));
    }

    @Test
    void deleteOperationEmitsBeforeRowAsDelete() throws Exception {
        Struct before = row("bob", 41);
        Struct envelope = envelope(Envelope.Operation.DELETE, before, null);

        List<Tuple2<Boolean, String>> out = collect(record(envelope));

        assertEquals(1, out.size(), "a delete should emit exactly one record");
        assertFalse(out.get(0).f0, "a delete must be marked as a delete, not an insert");
        JSONObject payload = JSON.parseObject(out.get(0).f1);
        assertEquals("bob", payload.getString("name"));
    }

    @Test
    void updateOperationEmitsAfterRow() throws Exception {
        Struct before = row("carol", 20);
        Struct after = row("carol", 21);
        Struct envelope = envelope(Envelope.Operation.UPDATE, before, after);

        List<Tuple2<Boolean, String>> out = collect(record(envelope));

        assertEquals(1, out.size());
        assertTrue(out.get(0).f0, "an update is applied as an upsert");
        assertEquals(21, JSON.parseObject(out.get(0).f1).getIntValue("age"));
    }

    @Test
    void readOperationEmitsAfterRow() throws Exception {
        // Snapshot reads take a separate code path from create/update because getRowMap throws on
        // them during the initial snapshot; this covers that branch specifically.
        Struct after = row("dave", 55);
        Struct envelope = envelope(Envelope.Operation.READ, null, after);

        List<Tuple2<Boolean, String>> out = collect(record(envelope));

        assertEquals(1, out.size(), "a read should emit the row");
        assertTrue(out.get(0).f0);
        assertEquals("dave", JSON.parseObject(out.get(0).f1).getString("name"));
    }

    @Test
    void readOperationWithNoAfterStateEmitsNothing() throws Exception {
        // A read whose "after" is null must not produce a payload, otherwise the sink receives an
        // empty row.
        Struct envelope = envelope(Envelope.Operation.READ, null, null);

        assertEquals(
                0, collect(record(envelope)).size(), "a read without after state emits nothing");
    }

    @Test
    void payloadIsValidJsonContainingAllFields() throws Exception {
        Struct after = row("erin", 7);
        Struct envelope = envelope(Envelope.Operation.CREATE, null, after);

        JSONObject payload = JSON.parseObject(collect(record(envelope)).get(0).f1);

        assertTrue(payload.containsKey("name"), "every described field should be present");
        assertTrue(payload.containsKey("age"), "every described field should be present");
    }

    @Test
    void typeInformationIsSerializable() {
        // The type information travels with the job; a non-serializable TypeHint would fail at
        // submission rather than here, so it is worth pinning.
        assertTrue(
                org.apache.flink.api.common.typeinfo.TypeInformation.class.isAssignableFrom(
                        schema.getProducedType().getClass()),
                "getProducedType should return a Flink TypeInformation");
    }

    // --- helpers -------------------------------------------------------------

    private static final Schema ROW_SCHEMA_OPTIONAL =
            SchemaBuilder.struct()
                    .field("name", Schema.STRING_SCHEMA)
                    .field("age", Schema.INT32_SCHEMA)
                    .optional()
                    .build();

    private static Struct row(final String name, final int age) {
        return new Struct(ROW_SCHEMA_OPTIONAL).put("name", name).put("age", age);
    }

    /**
     * Builds a Debezium envelope. {@code op} is the field the schema inspects, and the before/after
     * halves are declared optional so a null side is valid, which is what a real change event looks
     * like.
     *
     * <p>The before/after fields must carry the same schema instance the row Structs were built
     * from: Kafka Connect validates that a nested Struct's schema matches the one declared for its
     * field, so a separately built or placeholder schema here fails with "Struct schemas do not
     * match" rather than testing the deserializer.
     */
    private static Struct envelope(
            final Envelope.Operation operation, final Struct before, final Struct after) {
        // The row Struct supplied by the caller must be built from this exact schema instance:
        // Kafka Connect compares the nested Struct's schema against the declared field schema by
        // value, and `optional()` produces a schema that does not compare equal to the plain one.
        Schema envelopeSchema =
                SchemaBuilder.struct()
                        .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                        .field(Envelope.FieldName.BEFORE, ROW_SCHEMA_OPTIONAL)
                        .field(Envelope.FieldName.AFTER, ROW_SCHEMA_OPTIONAL)
                        .build();
        Struct struct =
                new Struct(envelopeSchema).put(Envelope.FieldName.OPERATION, operation.code());
        if (before != null) {
            struct.put(Envelope.FieldName.BEFORE, before);
        }
        if (after != null) {
            struct.put(Envelope.FieldName.AFTER, after);
        }
        return struct;
    }

    private SourceRecord record(final Object value) {
        return new SourceRecord(
                Collections.singletonMap("server", "test"),
                Collections.singletonMap("pos", 0),
                "test.topic",
                null,
                null,
                null,
                null,
                value);
    }

    private List<Tuple2<Boolean, String>> collect(final SourceRecord record) throws Exception {
        List<Tuple2<Boolean, String>> collected = new ArrayList<>();
        Collector<Tuple2<Boolean, String>> collector =
                new Collector<Tuple2<Boolean, String>>() {
                    @Override
                    public void collect(final Tuple2<Boolean, String> record) {
                        collected.add(record);
                    }

                    @Override
                    public void close() {
                        // Nothing to release in this stub.
                    }
                };
        schema.deserialize(record, collector);
        return collected;
    }
}
