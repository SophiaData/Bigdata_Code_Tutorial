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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
@SuppressWarnings("PMD.CyclomaticComplexity")
public class CdcEventDeserializer implements DebeziumDeserializationSchema<Event> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CdcEventDeserializer.class);

    /**
     * Shared schemas cache. Transient — Flink serialization creates independent copies which break
     * DDL-triggered updates. Always access via schemas() → SharedSchemaState.
     */
    private transient Map<String, Map<String, String>> schemas;

    CdcEventDeserializer() {
        this.schemas = SharedSchemaState.schemas();
    }

    private Map<String, Map<String, String>> schemas() {
        return SharedSchemaState.schemas();
    }

    @Override
    @SuppressWarnings("PMD.ImplicitSwitchFallThrough")
    public void deserialize(final SourceRecord record, final Collector<Event> out) {
        if (record == null) {
            return;
        }
        final Struct valueStruct = (Struct) record.value();
        if (valueStruct == null) {
            return;
        }

        final Struct source = valueStruct.getStruct("source");
        if (source == null) {
            LOG.warn(
                    "Missing source info, skipping event. value fields: {}",
                    valueStruct.schema().fields().stream()
                            .map(Field::name)
                            .collect(Collectors.toList()));
            return;
        }
        final String dbName = source.getString("db");
        final String tableName = source.getString("table");

        final String op;
        try {
            op = valueStruct.getString("op");
        } catch (final DataException e) {
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

        final Struct before = valueStruct.getStruct("before");
        final Struct after = valueStruct.getStruct("after");

        final Object[] beforeVals = extractValues(before);
        final Object[] afterVals = extractValues(after);
        final TableId tid = TableId.tableId(dbName, tableName);

        // Emit CreateTableEvent on first encounter so schema-evolver populates schemas cache
        emitCreateTableIfNeeded(record, valueStruct, tid, tableName, out);

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

    /**
     * Emit a CreateTableEvent on first encounter of a table so that schema-evolver populates the
     * schemas cache on the TaskManager side.
     */
    private void emitCreateTableIfNeeded(
            final SourceRecord record,
            final Struct valueStruct,
            final TableId tid,
            final String tableName,
            final Collector<Event> out) {
        if (schemas().containsKey(tableName)) {
            return;
        }
        try {
            final Struct after = valueStruct.getStruct("after");
            final Struct before = valueStruct.getStruct("before");
            // Use after if available, fall back to before (e.g. DELETE events have no after)
            final Struct row = after != null ? after : before;
            if (row == null) {
                return;
            }
            LOG.info(
                    "schema for {}: fields={}, row.class={}",
                    tableName,
                    row.schema().fields().stream()
                            .map(Field::name)
                            .collect(Collectors.joining(",")),
                    row.getClass().getName());
            final Schema.Builder schemaBuilder = Schema.newBuilder();
            for (final Field f : row.schema().fields()) {
                schemaBuilder.physicalColumn(f.name(), convertToCdcType(f.schema()));
            }
            final List<String> pkNames = new ArrayList<>();
            final Struct keyStruct = (Struct) record.key();
            if (keyStruct != null) {
                for (final Field kf : keyStruct.schema().fields()) {
                    pkNames.add(kf.name());
                }
            }
            final Schema schema = schemaBuilder.primaryKey(pkNames).build();
            final Map<String, String> colMap = new LinkedHashMap<>();
            for (final Field f : row.schema().fields()) {
                colMap.put(f.name(), f.schema().type().name());
            }
            SharedSchemaState.schemas().put(tableName, colMap);
            if (!pkNames.isEmpty()) {
                SharedSchemaState.pks().put(tableName, pkNames.get(0));
            }
            out.collect(new CreateTableEvent(tid, schema));
            LOG.info(
                    "Emitted CreateTableEvent for {} ({} columns, pk={})",
                    tableName,
                    colMap.size(),
                    pkNames);
        } catch (final Exception e) {
            LOG.warn("Failed to emit CreateTableEvent for {}: {}", tableName, e.getMessage());
        }
    }

    /** 从 Kafka Connect Struct 中提取所有字段值为 Object 数组。 */
    @SuppressWarnings("PMD.ReturnEmptyCollectionRatherThanNull")
    static Object[] extractValues(final Struct row) {
        if (row == null) {
            return null;
        }
        Object[] values = new Object[row.schema().fields().size()];
        int i = 0;
        for (final Field f : row.schema().fields()) {
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
        if (value instanceof OffsetDateTime) {
            return java.sql.Timestamp.from(((OffsetDateTime) value).toInstant());
        }
        if (value instanceof ZonedDateTime) {
            return java.sql.Timestamp.from(((ZonedDateTime) value).toInstant());
        }
        if (value instanceof Instant) {
            return java.sql.Timestamp.from((Instant) value);
        }
        if (value instanceof LocalDateTime) {
            return java.sql.Timestamp.valueOf((LocalDateTime) value);
        }
        if (value instanceof Date) {
            return new java.sql.Timestamp(((Date) value).getTime());
        }
        if (value instanceof String) {
            final String s = (String) value;
            if (s.contains("T")) {
                try {
                    return java.sql.Timestamp.from(Instant.parse(s));
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
            final Struct valueStruct,
            final String dbName,
            final String tableName,
            final Collector<Event> out) {
        final Set<String> fieldNames =
                valueStruct.schema().fields().stream().map(Field::name).collect(Collectors.toSet());

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
    private Map<String, Struct> extractColumnStructs(final Struct change) {
        final Map<String, Struct> result = new LinkedHashMap<>();
        final Set<String> fieldNames =
                change.schema().fields().stream().map(Field::name).collect(Collectors.toSet());

        Object columnsRaw = null;
        if (fieldNames.contains("columns")) {
            columnsRaw = change.get("columns");
        } else if (fieldNames.contains("table")) {
            final Struct table = change.getStruct("table");
            if (table != null && table.schema().field("columns") != null) {
                columnsRaw = table.get("columns");
            }
        }
        if (!(columnsRaw instanceof List)) {
            return result;
        }
        for (final Object colObj : (List<?>) columnsRaw) {
            if (!(colObj instanceof Struct)) {
                continue;
            }
            final Struct col = (Struct) colObj;
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
            final Struct change, final TableId tid, final Collector<Event> out) {
        final Map<String, Struct> newColStructs = extractColumnStructs(change);
        if (newColStructs.isEmpty()) {
            return;
        }

        final Map<String, String> existing = schemas().get(tid.getTableName());
        if (existing == null) {
            return;
        }

        // Detect dropped columns: present in old schemas but absent from the new DDL
        final Set<String> oldNames = new HashSet<>(existing.keySet());
        for (final String oldName : oldNames) {
            if (!newColStructs.containsKey(oldName)) {
                LOG.info("Detected dropped column: {}.{}", tid.getTableName(), oldName);
                // Directly update the static shared map — Flink serialization means
                // ProcessFunction/CDBBatchSink each get separate copies otherwise.
                schemas().get(tid.getTableName()).remove(oldName);
                out.collect(new DropColumnEvent(tid, List.of(oldName)));
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
            final Struct valueStruct,
            final Set<String> fieldNames,
            final String dbName,
            final String tableName,
            final Collector<Event> out) {
        Object changesRaw = null;
        if (fieldNames.contains("schemaChanges")) {
            changesRaw = valueStruct.get("schemaChanges");
        } else if (fieldNames.contains("tableChanges")) {
            changesRaw = valueStruct.get("tableChanges");
        }
        if (!(changesRaw instanceof List)) {
            return;
        }
        final List<?> changes = (List<?>) changesRaw;
        final TableId tid = TableId.tableId(dbName, tableName);
        for (final Object changeObj : changes) {
            if (!(changeObj instanceof Struct)) {
                continue;
            }
            final Struct change = (Struct) changeObj;
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
        final List<String> newColNames = parseHistoryRecord(historyRecordJson);
        if (newColNames.isEmpty()) {
            return;
        }

        final Map<String, String> existing = schemas().get(tableName);
        if (existing == null) {
            return;
        }

        final TableId tid = TableId.tableId(dbName, tableName);
        final Set<String> oldNames = new HashSet<>(existing.keySet());

        // Detect dropped columns
        for (final String oldName : oldNames) {
            if (!newColNames.contains(oldName)) {
                LOG.info("Detected dropped column: {}.{}", tableName, oldName);
                // Directly update schemas map here — Flink serialization means the
                // ProcessFunction's handleDropColumn operates on a different map copy.
                existing.remove(oldName);
                out.collect(new DropColumnEvent(tid, List.of(oldName)));
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

    private List<String> parseHistoryRecord(final String json) {
        final List<String> columnNames = new ArrayList<>();
        try {
            final ObjectMapper mapper = new ObjectMapper();
            final JsonNode root = mapper.readTree(json);
            final JsonNode tableChanges = root.get("tableChanges");
            if (tableChanges == null || !tableChanges.isArray() || tableChanges.isEmpty()) {
                return columnNames;
            }
            // Use the first (and usually only) change entry
            final JsonNode firstChange = tableChanges.get(0);
            final JsonNode tableNode = firstChange.get("table");
            if (tableNode == null) {
                return columnNames;
            }
            final JsonNode columns = tableNode.get("columns");
            if (columns == null || !columns.isArray()) {
                return columnNames;
            }
            for (final JsonNode col : columns) {
                final JsonNode nameNode = col.get("name");
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
     * Convert a Kafka Connect Schema type to a Flink CDC DataType. Handles the common MySQL types
     * encountered in Debezium CDC events.
     */
    private static DataType convertToCdcType(
            final org.apache.kafka.connect.data.Schema kafkaSchema) {
        if (kafkaSchema == null) {
            return DataTypes.STRING();
        }
        final org.apache.kafka.connect.data.Schema.Type type = kafkaSchema.type();
        switch (type) {
            case INT8:
                return DataTypes.TINYINT();
            case INT16:
                return DataTypes.SMALLINT();
            case INT32:
                return DataTypes.INT();
            case INT64:
                return DataTypes.BIGINT();
            case FLOAT32:
                return DataTypes.FLOAT();
            case FLOAT64:
                return DataTypes.DOUBLE();
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case BYTES:
                return DataTypes.BYTES();
            case STRING:
                return DataTypes.STRING();
            case ARRAY:
                return DataTypes.STRING();
            case MAP:
                return DataTypes.STRING();
            case STRUCT:
                return DataTypes.STRING();
            default:
                return DataTypes.STRING();
        }
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
