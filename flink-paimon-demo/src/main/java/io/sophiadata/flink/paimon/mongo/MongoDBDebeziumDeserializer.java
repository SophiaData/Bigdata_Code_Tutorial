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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Debezium deserialization schema that converts MongoDB CDC SourceRecords into BSON Documents.
 *
 * <p>Extracts the "after" state from Debezium change events, converting Kafka Connect Struct values
 * into MongoDB {@link Document} objects suitable for downstream processing.
 */
public class MongoDBDebeziumDeserializer implements DebeziumDeserializationSchema<Document> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBDebeziumDeserializer.class);

    @Override
    public void deserialize(final SourceRecord record, final Collector<Document> out)
            throws Exception {
        final Struct value = (Struct) record.value();
        if (value == null) {
            return;
        }

        final Struct after = getStructAfter(value);
        if (after == null) {
            // Skip delete events
            try {
                final String op = value.getString("op");
                if ("d".equals(op)) {
                    LOG.debug("Skipping delete event for topic: {}", record.topic());
                    return;
                }
            } catch (Exception e) {
                LOG.trace("Could not read op field from record", e);
            }
            LOG.debug("No after state in record for topic: {}", record.topic());
            return;
        }

        final Document doc = structToDocument(after);
        doc.put("_collection", extractCollectionFromTopic(record.topic()));
        try {
            doc.put("_op", value.getString("op"));
        } catch (Exception e) {
            LOG.trace("Op field not present in record", e);
        }

        out.collect(doc);
    }

    @Override
    public TypeInformation<Document> getProducedType() {
        return TypeInformation.of(Document.class);
    }

    private Struct getStructAfter(final Struct value) {
        try {
            return value.getStruct("after");
        } catch (Exception e) {
            return null;
        }
    }

    private String extractCollectionFromTopic(final String topic) {
        if (topic == null) {
            return "unknown";
        }
        final String[] parts = topic.split("\\.");
        return parts[parts.length - 1];
    }

    @SuppressWarnings("unchecked")
    private Document structToDocument(final Struct struct) {
        final Document doc = new Document();
        for (final org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Field
                field : struct.schema().fields()) {
            final Object val = struct.get(field);
            if (val != null) {
                doc.put(field.name(), convertValue(val));
            }
        }
        return doc;
    }

    @SuppressWarnings("unchecked")
    private Object convertValue(final Object value) {
        if (value instanceof Struct) {
            return structToDocument((Struct) value);
        } else if (value instanceof java.util.List) {
            final java.util.List<Object> list = new java.util.ArrayList<>();
            for (final Object item : (java.util.List<?>) value) {
                list.add(convertValue(item));
            }
            return list;
        } else if (value instanceof java.util.Map) {
            final java.util.Map<?, ?> map = (java.util.Map<?, ?>) value;
            final Document nested = new Document();
            for (final java.util.Map.Entry<?, ?> entry : map.entrySet()) {
                nested.put(String.valueOf(entry.getKey()), convertValue(entry.getValue()));
            }
            return nested;
        }
        return value;
    }
}
