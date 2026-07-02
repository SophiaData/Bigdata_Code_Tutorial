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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 将 Kafka Connect（Debezium）的 {@code SourceRecord} 转换为 Flink CDC 的 {@link Event}。
 *
 * <p>Debezium 从 MySQL binlog 解析出的原始记录格式为 Kafka Connect Struct， 包含 op（操作类型）、source（库表信息）、before /
 * after（变更前后行数据）。 本类将其映射为 Flink CDC 的 DataChangeEvent / TruncateTableEvent。
 *
 * <p>op 码映射：
 *
 * <ul>
 *   <li>{@code c} / {@code r} → insertEvent（create / read snapshot）
 *   <li>{@code u} → updateEvent
 *   <li>{@code d} → deleteEvent
 *   <li>{@code t} → TruncateTableEvent
 * </ul>
 *
 * <p>时间类型需要特殊处理：Debezium 可能返回 OffsetDateTime / ZonedDateTime 等， 需统一转换为 {@link java.sql.Timestamp}
 * 以匹配 JDBC sink 的类型期望。
 */
public class CdcEventDeserializer implements DebeziumDeserializationSchema<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(CdcEventDeserializer.class);

    /**
     * Shared schemas cache. Transient — Flink serialization creates independent copies which break
     * DDL-triggered updates. Always access via schemas() → SharedSchemaState.
     */
    private transient java.util.Map<String, java.util.Map<String, String>> schemas;

    CdcEventDeserializer() {
        this.schemas = SharedSchemaState.schemas();
    }

    private java.util.Map<String, java.util.Map<String, String>> schemas() {
        return SharedSchemaState.schemas();
    }

    @Override
    @SuppressWarnings("PMD.ImplicitSwitchFallThrough")
    public void deserialize(
            final org.apache.kafka.connect.source.SourceRecord record, final Collector<Event> out) {
        if (record == null) {
            return;
        }
        final org.apache.kafka.connect.data.Struct valueStruct =
                (org.apache.kafka.connect.data.Struct) record.value();
        if (valueStruct == null) {
            return;
        }

        final org.apache.kafka.connect.data.Struct source = valueStruct.getStruct("source");
        if (source == null) {
            LOG.warn(
                    "Missing source info, skipping event. value fields: {}",
                    valueStruct.schema().fields().stream()
                            .map(org.apache.kafka.connect.data.Field::name)
                            .collect(java.util.stream.Collectors.toList()));
            return;
        }
        final String dbName = source.getString("db");
        final String tableName = source.getString("table");

        final String op;
        try {
            op = valueStruct.getString("op");
        } catch (final org.apache.kafka.connect.errors.DataException e) {
            // DDL events (no "op" field) → parse schema changes so the schemas map
            // stays in sync with MySQL table evolution (ADD / DROP / ALTER columns).
            // Wrapped in try-catch so a DDL parsing failure never kills the pipeline.
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("DDL event for {}.{}, parsing schema changes", dbName, tableName);
                }
                emitSchemaChange(valueStruct, dbName, tableName, out);
            } catch (final Exception ddlEx) {
                LOG.warn(
                        "Failed to parse DDL event for {}.{}: {}",
                        dbName,
                        tableName,
                        ddlEx.getMessage());
            }
            return;
        }
        if (op == null) {
            return;
        }

        final org.apache.kafka.connect.data.Struct before = valueStruct.getStruct("before");
        final org.apache.kafka.connect.data.Struct after = valueStruct.getStruct("after");

        final Object[] beforeVals = extractValues(before);
        final Object[] afterVals = extractValues(after);
        final TableId tid = TableId.tableId(dbName, tableName);

        switch (op) {
            case "c":
            case "r":
                // fall through — both map to insertEvent
                out.collect(DataChangeEvent.insertEvent(tid, GenericRecordData.of(afterVals)));
                break;
            case "u":
                out.collect(
                        DataChangeEvent.updateEvent(
                                tid,
                                GenericRecordData.of(beforeVals),
                                GenericRecordData.of(afterVals)));
                break;
            case "d":
                out.collect(DataChangeEvent.deleteEvent(tid, GenericRecordData.of(beforeVals)));
                break;
            case "t":
                out.collect(new TruncateTableEvent(tid));
                break;
            default:
                LOG.debug("Unknown op: {}", op);
        }
    }

    /** 从 Kafka Connect Struct 中提取所有字段值为 Object 数组。 */
    @SuppressWarnings("PMD.ReturnEmptyCollectionRatherThanNull")
    static Object[] extractValues(final org.apache.kafka.connect.data.Struct row) {
        if (row == null) {
            return null;
        }
        Object[] values = new Object[row.schema().fields().size()];
        int i = 0;
        for (final org.apache.kafka.connect.data.Field f : row.schema().fields()) {
            values[i++] = convertValue(row.get(f.name()));
        }
        return values;
    }

    /**
     * Debezium 可能返回多种时间类型（OffsetDateTime、ZonedDateTime、Instant 等）， 统一转为 java.sql.Timestamp，确保 JDBC
     * PreparedStatement.setObject() 能正确绑定。 含 "T" 的字符串会尝试解析为 Instant 后再转换。
     */
    static final Object convertValue(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof java.time.OffsetDateTime) {
            return java.sql.Timestamp.from(((java.time.OffsetDateTime) value).toInstant());
        }
        if (value instanceof java.time.ZonedDateTime) {
            return java.sql.Timestamp.from(((java.time.ZonedDateTime) value).toInstant());
        }
        if (value instanceof java.time.Instant) {
            return java.sql.Timestamp.from((java.time.Instant) value);
        }
        if (value instanceof java.time.LocalDateTime) {
            return java.sql.Timestamp.valueOf((java.time.LocalDateTime) value);
        }
        if (value instanceof java.util.Date) {
            return new java.sql.Timestamp(((java.util.Date) value).getTime());
        }
        if (value instanceof String) {
            final String s = (String) value;
            if (s.contains("T")) {
                try {
                    return java.sql.Timestamp.from(java.time.Instant.parse(s));
                } catch (Exception e) {
                    return s;
                }
            }
            return s;
        }
        return value;
    }

    /**
     * Parse Debezium schema change events (DDL) and emit Flink CDC {@link SchemaChangeEvent}
     * objects (AddColumnEvent / DropColumnEvent).
     *
     * <p>Supports both {@code schemaChanges} (Debezium 1.x) and {@code tableChanges} (HistoryRecord
     * / CDC 3.x) formats. Compares the new column list against the current in-memory {@link
     * #schemas} map to determine which columns were added or dropped, then emits the corresponding
     * events so {@code FlinkSqlWDS.handleSchemaEvent()} can keep the schemas cache in sync.
     */
    @SuppressWarnings("unchecked")
    private void emitSchemaChange(
            final org.apache.kafka.connect.data.Struct valueStruct,
            final String dbName,
            final String tableName,
            final Collector<Event> out) {
        final java.util.Set<String> fieldNames =
                valueStruct.schema().fields().stream()
                        .map(org.apache.kafka.connect.data.Field::name)
                        .collect(java.util.stream.Collectors.toSet());

        // Flink CDC 3.x: historyRecord is a JSON string with tableChanges
        if (fieldNames.contains("historyRecord")) {
            final String json = valueStruct.getString("historyRecord");
            if (json != null) {
                emitFromHistoryRecord(json, dbName, tableName, out);
                return;
            }
        }

        // Fallback: Debezium 1.x schemaChanges / tableChanges Struct arrays
        emitFromStructChanges(valueStruct, fieldNames, dbName, tableName, out);
    }

    /**
     * Extract column-name → column-struct map from a Debezium schema-change or table-change struct.
     */
    private java.util.Map<String, org.apache.kafka.connect.data.Struct> extractColumnStructs(
            final org.apache.kafka.connect.data.Struct change) {
        final java.util.Map<String, org.apache.kafka.connect.data.Struct> result =
                new java.util.LinkedHashMap<>();
        final java.util.Set<String> fieldNames =
                change.schema().fields().stream()
                        .map(org.apache.kafka.connect.data.Field::name)
                        .collect(java.util.stream.Collectors.toSet());

        Object columnsRaw = null;
        if (fieldNames.contains("columns")) {
            columnsRaw = change.get("columns");
        } else if (fieldNames.contains("table")) {
            final org.apache.kafka.connect.data.Struct table = change.getStruct("table");
            if (table != null && table.schema().field("columns") != null) {
                columnsRaw = table.get("columns");
            }
        }
        if (!(columnsRaw instanceof java.util.List)) {
            return result;
        }
        for (final Object colObj : (java.util.List<?>) columnsRaw) {
            if (!(colObj instanceof org.apache.kafka.connect.data.Struct)) {
                continue;
            }
            final org.apache.kafka.connect.data.Struct col =
                    (org.apache.kafka.connect.data.Struct) colObj;
            final String name = col.getString("name");
            if (name != null) {
                result.put(name, col);
            }
        }
        return result;
    }

    /**
     * Compares the new column list from an ALTER event against the in-memory schemas map and emits
     * DropColumnEvent to keep the schemas cache in sync.
     */
    private void emitAlterColumns(
            final org.apache.kafka.connect.data.Struct change,
            final TableId tid,
            final Collector<Event> out) {
        final java.util.Map<String, org.apache.kafka.connect.data.Struct> newColStructs =
                extractColumnStructs(change);
        if (newColStructs.isEmpty()) {
            return;
        }

        final java.util.Map<String, String> existing = schemas().get(tid.getTableName());
        if (existing == null) {
            return;
        }

        // Detect dropped columns: present in old schemas but absent from the new DDL
        final java.util.Set<String> oldNames = new java.util.HashSet<>(existing.keySet());
        for (final String oldName : oldNames) {
            if (!newColStructs.containsKey(oldName)) {
                LOG.info("Detected dropped column: {}.{}", tid.getTableName(), oldName);
                // Directly update the static shared map — Flink serialization means
                // ProcessFunction/CDBBatchSink each get separate copies otherwise.
                schemas().get(tid.getTableName()).remove(oldName);
                out.collect(new DropColumnEvent(tid, java.util.List.of(oldName)));
            }
        }

        for (final String newName : newColStructs.keySet()) {
            if (!oldNames.contains(newName)) {
                LOG.info(
                        "Detected added column (schema ALTER handled by SchemaEvolver): {}.{}",
                        tid.getTableName(),
                        newName);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void emitFromStructChanges(
            final org.apache.kafka.connect.data.Struct valueStruct,
            final java.util.Set<String> fieldNames,
            final String dbName,
            final String tableName,
            final Collector<Event> out) {
        Object changesRaw = null;
        if (fieldNames.contains("schemaChanges")) {
            changesRaw = valueStruct.get("schemaChanges");
        } else if (fieldNames.contains("tableChanges")) {
            changesRaw = valueStruct.get("tableChanges");
        }
        if (!(changesRaw instanceof java.util.List)) {
            return;
        }
        final java.util.List<?> changes = (java.util.List<?>) changesRaw;
        final TableId tid = TableId.tableId(dbName, tableName);
        for (final Object changeObj : changes) {
            if (!(changeObj instanceof org.apache.kafka.connect.data.Struct)) {
                continue;
            }
            final org.apache.kafka.connect.data.Struct change =
                    (org.apache.kafka.connect.data.Struct) changeObj;
            final String type = change.getString("type");
            if ("ALTER".equals(type)) {
                emitAlterColumns(change, tid, out);
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("Unhandled schema change type: {} for {}.{}", type, dbName, tableName);
            }
        }
    }

    /**
     * Parse a Debezium historyRecord JSON string and emit DropColumnEvent for columns that were
     * removed from the table.
     */
    private void emitFromHistoryRecord(
            final String historyRecordJson,
            final String dbName,
            final String tableName,
            final Collector<Event> out) {
        final java.util.List<String> newColNames = parseHistoryRecord(historyRecordJson);
        if (newColNames.isEmpty()) {
            return;
        }

        final java.util.Map<String, String> existing = schemas().get(tableName);
        if (existing == null) {
            return;
        }

        final TableId tid = TableId.tableId(dbName, tableName);
        final java.util.Set<String> oldNames = new java.util.HashSet<>(existing.keySet());

        // Detect dropped columns
        for (final String oldName : oldNames) {
            if (!newColNames.contains(oldName)) {
                LOG.info("Detected dropped column: {}.{}", tableName, oldName);
                // Directly update schemas map here — Flink serialization means the
                // ProcessFunction's handleDropColumn operates on a different map copy.
                existing.remove(oldName);
                out.collect(new DropColumnEvent(tid, java.util.List.of(oldName)));
            }
        }

        // Log added columns (not emitting AddColumnEvent to avoid async ALTER timing issues)
        for (final String newName : newColNames) {
            if (!oldNames.contains(newName)) {
                LOG.info(
                        "Detected added column (schema ALTER handled by SchemaEvolver): {}.{}",
                        tableName,
                        newName);
            }
        }
    }

    private java.util.List<String> parseHistoryRecord(final String json) {
        final java.util.List<String> columnNames = new java.util.ArrayList<>();
        try {
            final com.fasterxml.jackson.databind.ObjectMapper mapper =
                    new com.fasterxml.jackson.databind.ObjectMapper();
            final com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(json);
            final com.fasterxml.jackson.databind.JsonNode tableChanges = root.get("tableChanges");
            if (tableChanges == null || !tableChanges.isArray() || tableChanges.isEmpty()) {
                return columnNames;
            }
            // Use the first (and usually only) change entry
            final com.fasterxml.jackson.databind.JsonNode firstChange = tableChanges.get(0);
            final com.fasterxml.jackson.databind.JsonNode tableNode = firstChange.get("table");
            if (tableNode == null) {
                return columnNames;
            }
            final com.fasterxml.jackson.databind.JsonNode columns = tableNode.get("columns");
            if (columns == null || !columns.isArray()) {
                return columnNames;
            }
            for (final com.fasterxml.jackson.databind.JsonNode col : columns) {
                final com.fasterxml.jackson.databind.JsonNode nameNode = col.get("name");
                if (nameNode != null) {
                    columnNames.add(nameNode.asText());
                }
            }
        } catch (final Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to parse historyRecord JSON: {}", e.getMessage());
            }
        }
        return columnNames;
    }

    /**
     * Extract column names from a Debezium historyRecord JSON string. Used for Flink CDC 3.x DDL
     * events where the schema changes are embedded as a JSON string rather than a Struct.
     */
    @SuppressWarnings("unchecked")
    private java.util.List<String> extractColumnNamesFromHistoryRecord(
            final org.apache.kafka.connect.data.Struct change) {
        // historyRecord can be a JSON string in some Debezium formats
        final Object hr = change.get("historyRecord");
        if (hr instanceof String) {
            return parseHistoryRecord((String) hr);
        }
        // Or columns may be directly available as in schemaChanges/tableChanges formats
        return new java.util.ArrayList<>(extractColumnStructs(change).keySet());
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
