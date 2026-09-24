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

package io.sophiadata.flink.paimon.mongo;

import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import org.apache.flink.util.Collector;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link MongoDBDebeziumDeserializer}, which had no coverage at all.
 *
 * <p>The deserializer decides what a CDC record turns into downstream, so the cases that matter are
 * the ones where it must stay silent (deletes, tombstone values) and the ones where nested BSON has
 * to be rebuilt faithfully - a lost nested field would silently drop data in the sink.
 */
class MongoDBDebeziumDeserializerTest {

    private final MongoDBDebeziumDeserializer deserializer = new MongoDBDebeziumDeserializer();

    @Test
    void producesDocumentTypeInformation() {
        assertNotNull(deserializer.getProducedType(), "the produced type must not be null");
    }

    @Test
    void nullValueProducesNothing() throws Exception {
        SourceRecord record = record(Topic.of("db", "users"), null);

        assertEquals(0, collect(record).size(), "a null value carries no document");
    }

    @Test
    void deleteEventProducesNothing() throws Exception {
        // A delete has no "after" state; emitting a document here would resurrect deleted rows.
        Struct value = new Struct(schemaWithOp()).put("op", "d");

        assertEquals(
                0,
                collect(record(Topic.of("db", "users"), value)).size(),
                "delete events must be skipped");
    }

    @Test
    void eventWithoutAfterStateProducesNothing() throws Exception {
        Struct value = new Struct(schemaWithOp()).put("op", "c");

        assertEquals(
                0,
                collect(record(Topic.of("db", "users"), value)).size(),
                "a record with no after state must be skipped");
    }

    @Test
    void insertEventProducesDocumentWithFields() throws Exception {
        Struct value = new Struct(valueSchema()).put("op", "c").put("after", after("alice", 30));

        List<Document> docs = collect(record(Topic.of("db", "users"), value));

        assertEquals(1, docs.size(), "exactly one document should be emitted");
        Document doc = docs.get(0);
        assertEquals("alice", doc.getString("name"));
        assertEquals(30, doc.getInteger("age"));
    }

    @Test
    void collectionIsDerivedFromTheTopicName() throws Exception {
        Struct value = new Struct(valueSchema()).put("op", "c").put("after", after("bob", 1));

        Document doc = collect(record(Topic.of("db", "inventory", "products"), value)).get(0);

        assertEquals("products", doc.getString("_collection"));
    }

    @Test
    void opIsRecordedOnTheDocument() throws Exception {
        Struct value = new Struct(valueSchema()).put("op", "u").put("after", after("carol", 2));

        Document doc = collect(record(Topic.of("db", "t"), value)).get(0);

        assertEquals("u", doc.getString("_op"));
    }

    @Test
    void nullFieldValuesAreOmitted() throws Exception {
        // The struct declares "nickname" but leaves it null; putting a null key would produce a
        // BSON
        // document with an explicit null, which is different from the field being absent.
        Struct after = new Struct(afterSchemaWithNickname());
        after.put("name", "dave");
        after.put("nickname", null);
        Struct value = new Struct(valueSchemaFor(afterSchemaWithNickname()));
        value.put("op", "c");
        value.put("after", after);

        Document doc = collect(record(Topic.of("db", "t"), value)).get(0);

        assertTrue(doc.containsKey("name"), "non-null fields should be present");
        assertFalse(doc.containsKey("nickname"), "null fields should be omitted entirely");
    }

    @Test
    void nestedStructBecomesNestedDocument() throws Exception {
        Schema innerSchema = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).build();
        Struct inner = new Struct(innerSchema).put("city", "berlin");
        Schema afterSchema = SchemaBuilder.struct().field("address", innerSchema).build();
        Struct after = new Struct(afterSchema).put("address", inner);
        Struct value = new Struct(valueSchemaFor(afterSchema)).put("op", "c").put("after", after);

        Document doc = collect(record(Topic.of("db", "t"), value)).get(0);

        Object nested = doc.get("address");
        assertTrue(nested instanceof Document, "a nested Struct should become a nested Document");
        assertEquals("berlin", ((Document) nested).getString("city"));
    }

    @Test
    void listValuesAreConvertedElementWise() throws Exception {
        Schema afterSchema =
                SchemaBuilder.struct()
                        .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                        .build();
        Struct after =
                new Struct(afterSchema).put("tags", new ArrayList<>(Arrays.asList("a", "b")));
        Struct value = new Struct(valueSchemaFor(afterSchema)).put("op", "c").put("after", after);

        Document doc = collect(record(Topic.of("db", "t"), value)).get(0);

        assertEquals(Arrays.asList("a", "b"), doc.get("tags"));
    }

    @Test
    void listOfStructsBecomesListOfDocuments() throws Exception {
        Schema itemSchema = SchemaBuilder.struct().field("sku", Schema.STRING_SCHEMA).build();
        Schema afterSchema =
                SchemaBuilder.struct()
                        .field("items", SchemaBuilder.array(itemSchema).build())
                        .build();
        Struct after =
                new Struct(afterSchema)
                        .put(
                                "items",
                                new ArrayList<>(
                                        Collections.singletonList(
                                                new Struct(itemSchema).put("sku", "X-1"))));
        Struct value = new Struct(valueSchemaFor(afterSchema)).put("op", "c").put("after", after);

        Document doc = collect(record(Topic.of("db", "t"), value)).get(0);

        List<?> items = (List<?>) doc.get("items");
        assertEquals(1, items.size());
        assertTrue(items.get(0) instanceof Document, "struct elements should be converted too");
        assertEquals("X-1", ((Document) items.get(0)).getString("sku"));
    }

    @Test
    void mapValuesBecomeNestedDocuments() throws Exception {
        Schema afterSchema =
                SchemaBuilder.struct()
                        .field(
                                "meta",
                                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)
                                        .build())
                        .build();
        Map<String, String> meta = new LinkedHashMap<>();
        meta.put("source", "mysql");
        Struct after = new Struct(afterSchema).put("meta", meta);
        Struct value = new Struct(valueSchemaFor(afterSchema)).put("op", "c").put("after", after);

        Document doc = collect(record(Topic.of("db", "t"), value)).get(0);

        Object converted = doc.get("meta");
        assertTrue(converted instanceof Document, "a Map should become a Document");
        assertEquals("mysql", ((Document) converted).getString("source"));
    }

    @Test
    void missingOpFieldStillEmitsTheDocument() throws Exception {
        // The op field is optional; failing to read it must not discard an otherwise valid row.
        Struct value = new Struct(valueSchema()).put("after", after("erin", 5));

        List<Document> docs = collect(record(Topic.of("db", "t"), value));

        assertEquals(1, docs.size(), "the document should still be emitted");
        assertEquals("erin", docs.get(0).getString("name"));
    }

    // --- helpers -------------------------------------------------------------

    private static final class Topic {
        private final String name;

        private Topic(final String name) {
            this.name = name;
        }

        static Topic of(final String... parts) {
            return new Topic(String.join(".", parts));
        }
    }

    private SourceRecord record(final Topic topic, final Object value) {
        return new SourceRecord(
                Collections.singletonMap("server", "test"),
                Collections.singletonMap("pos", 0),
                topic.name,
                null,
                null,
                null,
                null,
                value);
    }

    private List<Document> collect(final SourceRecord record) throws Exception {
        List<Document> collected = new ArrayList<>();
        Collector<Document> collector =
                new Collector<Document>() {
                    @Override
                    public void collect(final Document record) {
                        collected.add(record);
                    }

                    @Override
                    public void close() {
                        // Nothing to release in this stub.
                    }
                };
        deserializer.deserialize(record, collector);
        return collected;
    }

    private static Schema schemaWithOp() {
        return SchemaBuilder.struct().field("op", Schema.STRING_SCHEMA).build();
    }

    private static Schema afterSchema() {
        return SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .build();
    }

    private static Schema afterSchemaWithNickname() {
        // optional() is required: Kafka Connect rejects a null value for a required field, and this
        // test is specifically about how the deserializer treats an absent value.
        return SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("nickname", SchemaBuilder.string().optional().build())
                .build();
    }

    private static Struct after(final String name, final int age) {
        return new Struct(afterSchema()).put("name", name).put("age", age);
    }

    private static Schema valueSchema() {
        return valueSchemaFor(afterSchema());
    }

    private static Schema valueSchemaFor(final Schema afterSchema) {
        return SchemaBuilder.struct()
                .field("op", Schema.STRING_SCHEMA)
                .field("after", afterSchema)
                .build();
    }
}
