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

package io.sophiadata.flink.cdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import io.debezium.data.Envelope;
import io.sophiadata.flink.sync.table.CustomDebeziumDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives real Debezium {@link SourceRecord}s through {@link CustomDebeziumDeserializer}.
 *
 * <p>This is the layer the previous test suite never touched: the tests asserted that a source
 * object could be <em>constructed</em>, but nothing ever fed an actual change event through the
 * deserializer. A bug in row extraction, {@link RowKind} assignment or type conversion would
 * therefore have passed CI undetected.
 *
 * <p>Records are built with the same Kafka Connect schema shape Debezium produces ({@code Envelope}
 * with {@code before}/{@code after}/{@code source}), so no database or Docker is required and these
 * run on every build, on both CDC version lines.
 */
class CustomDebeziumDeserializerTest {

    private static final String TABLE = "orders";

    /** Debezium's `source` struct carries the originating table name. */
    private static final Schema SOURCE_SCHEMA =
            SchemaBuilder.struct()
                    .field("db", Schema.STRING_SCHEMA)
                    .field("table", Schema.STRING_SCHEMA)
                    .build();

    /**
     * The captured table's row shape: (id INT, name VARCHAR).
     *
     * <p>Marked {@code .optional()} so {@code before}/{@code after} can be null, matching Debezium:
     * an insert has a null {@code before} and a delete has a null {@code after}.
     */
    private static final Schema ROW_SCHEMA =
            SchemaBuilder.struct()
                    .name("server.db." + TABLE + ".Value")
                    .optional()
                    .field("id", Schema.INT32_SCHEMA)
                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                    .build();

    private CustomDebeziumDeserializer deserializer;
    private CollectingCollector collector;

    @BeforeEach
    void setUp() {
        // The deserializer maps table name -> RowType once, in the constructor.
        Map<String, RowType> tableRowTypes = new HashMap<>();
        tableRowTypes.put(TABLE, (RowType) rowType().getLogicalType());

        deserializer = new CustomDebeziumDeserializer(tableRowTypes);
        collector = new CollectingCollector();
    }

    private static DataType rowType() {
        return org.apache.flink.table.api.DataTypes.ROW(
                org.apache.flink.table.api.DataTypes.FIELD(
                        "id", org.apache.flink.table.api.DataTypes.INT()),
                org.apache.flink.table.api.DataTypes.FIELD(
                        "name", org.apache.flink.table.api.DataTypes.STRING()));
    }

    /**
     * Builds the envelope schema Debezium emits, with the operation field included.
     *
     * <p>{@code before} and {@code after} are both optional: an insert has a null {@code before}
     * and a delete has a null {@code after}. Encoding that faithfully matters, because it is
     * exactly why the deserializer must read the delete image from {@code before} rather than
     * {@code after}.
     */
    private static Schema envelopeSchema() {
        return SchemaBuilder.struct()
                .name("server.db." + TABLE + ".Envelope")
                .field(Envelope.FieldName.BEFORE, ROW_SCHEMA)
                .field(Envelope.FieldName.AFTER, ROW_SCHEMA)
                .field(Envelope.FieldName.SOURCE, SOURCE_SCHEMA)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .build();
    }

    private static Struct row(Integer id, String name) {
        return new Struct(ROW_SCHEMA).put("id", id).put("name", name);
    }

    /**
     * Creates a source record for the given operation.
     *
     * @param op Debezium operation code ({@code c}, {@code r}, {@code u}, {@code d})
     * @param before the before image, or {@code null}
     * @param after the after image, or {@code null}
     */
    private static SourceRecord record(String op, Struct before, Struct after) {
        Struct source = new Struct(SOURCE_SCHEMA).put("db", "test").put("table", TABLE);

        Struct envelope =
                new Struct(envelopeSchema())
                        .put(Envelope.FieldName.BEFORE, before)
                        .put(Envelope.FieldName.AFTER, after)
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.OPERATION, op);

        return new SourceRecord(
                Collections.singletonMap("server", "test"),
                Collections.singletonMap("file", "mysql-bin.000001"),
                "test." + TABLE,
                null,
                envelopeSchema(),
                envelope);
    }

    @Test
    void insertProducesASingleInsertRowWithConvertedValues() throws Exception {
        deserializer.deserialize(record("c", null, row(1, "alice")), collector);

        assertEquals(1, collector.results.size(), "an insert yields exactly one row");
        Tuple2<String, Row> emitted = collector.results.get(0);

        assertEquals(TABLE, emitted.f0, "table name must be propagated for downstream filtering");
        assertEquals(RowKind.INSERT, emitted.f1.getKind());
        assertEquals(1, emitted.f1.getField(0), "int column must be converted to Integer");
        // VARCHAR is carried as Flink's internal StringData, not java.lang.String.
        assertEquals(StringData.fromString("alice"), emitted.f1.getField(1));
    }

    @Test
    void updateProducesBeforeAndAfterInOrder() throws Exception {
        deserializer.deserialize(record("u", row(1, "alice"), row(1, "bob")), collector);

        assertEquals(2, collector.results.size(), "an update yields a before and an after image");

        Tuple2<String, Row> before = collector.results.get(0);
        assertEquals(RowKind.UPDATE_BEFORE, before.f1.getKind());
        assertEquals(StringData.fromString("alice"), before.f1.getField(1));

        Tuple2<String, Row> after = collector.results.get(1);
        assertEquals(RowKind.UPDATE_AFTER, after.f1.getKind());
        assertEquals(StringData.fromString("bob"), after.f1.getField(1));
    }

    @Test
    void deleteProducesADeleteRowFromTheBeforeImage() throws Exception {
        deserializer.deserialize(record("d", row(7, "carol"), null), collector);

        assertEquals(1, collector.results.size());
        Tuple2<String, Row> emitted = collector.results.get(0);
        assertEquals(RowKind.DELETE, emitted.f1.getKind());
        // The delete image must come from `before`, not `after` (which is null on a delete).
        assertEquals(7, emitted.f1.getField(0));
        assertEquals(StringData.fromString("carol"), emitted.f1.getField(1));
    }

    @Test
    void snapshotReadIsTreatedAsAnInsert() throws Exception {
        // Snapshot rows arrive with op="r" and must be replayed as inserts, otherwise the initial
        // load would be silently dropped.
        deserializer.deserialize(record("r", null, row(42, "dave")), collector);

        assertEquals(1, collector.results.size());
        assertEquals(RowKind.INSERT, collector.results.get(0).f1.getKind());
        assertEquals(42, collector.results.get(0).f1.getField(0));
    }

    @Test
    void nullColumnValuesSurviveAsNull() throws Exception {
        deserializer.deserialize(record("c", null, row(5, null)), collector);

        assertEquals(1, collector.results.size());
        assertEquals(5, collector.results.get(0).f1.getField(0));
        assertEquals(null, collector.results.get(0).f1.getField(1));
    }

    @Test
    void eventsForUnregisteredTablesFailWithAnActionableMessage() {
        Struct source = new Struct(SOURCE_SCHEMA).put("db", "test").put("table", "not_registered");
        Struct envelope =
                new Struct(envelopeSchema())
                        .put(Envelope.FieldName.AFTER, row(1, "x"))
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.OPERATION, "c");
        SourceRecord record =
                new SourceRecord(
                        Collections.singletonMap("server", "test"),
                        Collections.singletonMap("file", "bin"),
                        "test.not_registered",
                        null,
                        envelopeSchema(),
                        envelope);

        IllegalStateException ex =
                assertThrows(
                        IllegalStateException.class,
                        () -> deserializer.deserialize(record, collector));

        // Regression: this used to be a bare NullPointerException with no context.
        assertTrue(ex.getMessage().contains("not_registered"), ex.getMessage());
        assertTrue(
                ex.getMessage().contains(TABLE),
                "should list registered tables: " + ex.getMessage());
        assertTrue(collector.results.isEmpty(), "nothing should be emitted on failure");
    }

    @Test
    void deserializerIsUsableThroughTheFlinkCdcInterface() throws Exception {
        // The connector invokes the deserializer through Flink CDC's DebeziumDeserializationSchema,
        // so verify the concrete class satisfies that contract (not just its own methods).
        DebeziumDeserializationSchema<Tuple2<String, Row>> asInterface = deserializer;

        assertEquals(Tuple2.class, asInterface.getProducedType().getTypeClass());

        asInterface.deserialize(record("c", null, row(9, "erin")), collector);
        assertEquals(1, collector.results.size());
        assertEquals(9, collector.results.get(0).f1.getField(0));
    }

    /** Collects emitted tuples so assertions can inspect them. */
    private static final class CollectingCollector implements Collector<Tuple2<String, Row>> {

        final List<Tuple2<String, Row>> results = new ArrayList<>();

        @Override
        public void collect(Tuple2<String, Row> record) {
            results.add(record);
        }

        @Override
        public void close() {}
    }
}
