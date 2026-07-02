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
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;
import org.apache.flink.cdc.connectors.mysql.schema.MySqlTypeUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.table.api.DataTypes;
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
     * Shared schemas cache (tableName → ordered columnName → typeString). Used to detect DROP/ADD
     * columns when processing DDL events, so the column order in {@link CDBBatchSink} stays in sync
     * with the actual MySQL table schema.
     */
    private final java.util.Map<String, java.util.Map<String, String>> schemas;

    CdcEventDeserializer(final java.util.Map<String, java.util.Map<String, String>> schemas) {
        this.schemas = schemas;
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
        // Try schemaChanges field (Debezium 1.x MySQL connector format)
        Object changesRaw = valueStruct.get("schemaChanges");
        // Try tableChanges field (Debezium HistoryRecord / CDC 3.x format)
        if (!(changesRaw instanceof java.util.List)) {
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
            if (type == null) {
                continue;
            }
            switch (type) {
                case "ALTER":
                    emitAlterColumns(change, tid, out);
                    break;
                default:
                    LOG.debug(
                            "Unhandled schema change type: {} for {}.{}", type, dbName, tableName);
            }
        }
    }

    /**
     * Extract column-name → column-struct map from a Debezium schema-change or table-change struct.
     * Handles both {@code schemaChanges} (direct "columns" field) and {@code tableChanges} (nested
     * "table" → "columns") formats. Returns a {@link java.util.LinkedHashMap} to preserve insertion
     * order.
     */
    private java.util.Map<String, org.apache.kafka.connect.data.Struct> extractColumnStructs(
            final org.apache.kafka.connect.data.Struct change) {
        final java.util.Map<String, org.apache.kafka.connect.data.Struct> result =
                new java.util.LinkedHashMap<>();
        Object columnsRaw = change.get("columns");
        if (!(columnsRaw instanceof java.util.List)) {
            // Try nested "table" → "columns" (tableChanges / HistoryRecord format)
            final org.apache.kafka.connect.data.Struct table = change.getStruct("table");
            if (table != null) {
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
     * Compares the new column list from an ALTER event against the in-memory {@link #schemas} map
     * and emits {@link DropColumnEvent} / {@link AddColumnEvent} to keep the schemas cache in sync.
     *
     * <p>Column types for added columns are resolved via {@link MySqlTypeUtils#fromDbzColumn} so
     * the sink table's ALTER ADD COLUMN uses the correct Flink CDC {@link
     * org.apache.flink.table.types.DataType} (e.g. {@code DECIMAL(10,2)} → {@code
     * DataTypes.DECIMAL(10,2)}), not a hardcoded {@code STRING()}.
     */
    @SuppressWarnings("unchecked")
    private void emitAlterColumns(
            final org.apache.kafka.connect.data.Struct change,
            final TableId tid,
            final Collector<Event> out) {
        final java.util.Map<String, org.apache.kafka.connect.data.Struct> newColStructs =
                extractColumnStructs(change);
        if (newColStructs.isEmpty()) {
            return;
        }

        final java.util.Map<String, String> existing = schemas.get(tid.getTableName());
        if (existing == null) {
            return;
        }

        // Detect dropped columns: present in old schemas but absent from the new DDL
        final java.util.Set<String> oldNames = existing.keySet();
        for (final String oldName : oldNames) {
            if (!newColStructs.containsKey(oldName)) {
                LOG.info("Detected dropped column: {}.{}", tid.getTableName(), oldName);
                out.collect(new DropColumnEvent(tid, java.util.List.of(oldName)));
            }
        }

        // NOTE: We intentionally do NOT emit AddColumnEvent for added columns.
        // The schemas map is used by CDBBatchSink to build INSERT SQL column lists.
        // Emitting AddColumnEvent would update schemas before the async ALTER TABLE on
        // the sink completes, causing INSERT to reference non-existent columns → MySQL error.
        // DropColumnEvent is safe because it only removes columns from schemas,
        // making the INSERT SQL narrower (which always matches the sink table).
        for (final String newName : newColStructs.keySet()) {
            if (!oldNames.contains(newName)) {
                LOG.info(
                        "Detected added column (schema ALTER handled by SchemaEvolver): {}.{}",
                        tid.getTableName(),
                        newName);
            }
        }
    }

    /**
     * Convert a Debezium DDL column struct (Kafka Connect Struct) to Flink CDC {@link
     * org.apache.flink.cdc.common.types.DataType} by constructing a {@link
     * io.debezium.relational.Column} and delegating to {@link MySqlTypeUtils#fromDbzColumn}, then
     * converting via {@link DataTypeUtils#fromFlinkDataType}.
     *
     * <p>Falls back to {@code DataTypes.STRING()} if any part of the mapping fails (missing fields,
     * unknown type name, etc.).
     */
    @SuppressWarnings("unchecked")
    static org.apache.flink.cdc.common.types.DataType columnStructToFlinkType(
            final org.apache.kafka.connect.data.Struct colStruct) {
        try {
            final String typeName = colStruct.getString("typeName");
            if (typeName == null) {
                return DataTypeUtils.fromFlinkDataType(DataTypes.STRING());
            }

            final io.debezium.relational.ColumnEditor editor =
                    io.debezium.relational.Column.editor();
            editor.name(colStruct.getString("name"));
            editor.type(typeName);

            // length / precision
            final org.apache.kafka.connect.data.Field lengthField =
                    colStruct.schema().field("length");
            if (lengthField != null && colStruct.get("length") != null) {
                editor.length(colStruct.getInt32("length"));
            }

            // scale
            final org.apache.kafka.connect.data.Field scaleField =
                    colStruct.schema().field("scale");
            if (scaleField != null && colStruct.get("scale") != null) {
                editor.scale(colStruct.getInt32("scale"));
            }

            // optional
            final org.apache.kafka.connect.data.Field optionalField =
                    colStruct.schema().field("optional");
            if (optionalField != null && colStruct.get("optional") instanceof Boolean) {
                editor.optional(colStruct.getBoolean("optional"));
            }

            final io.debezium.relational.Column debeziumColumn = editor.create();
            final org.apache.flink.table.types.DataType flinkDataType =
                    MySqlTypeUtils.fromDbzColumn(debeziumColumn, false);
            return DataTypeUtils.fromFlinkDataType(flinkDataType);
        } catch (final Exception e) {
            LOG.warn(
                    "Failed to map DDL column type for '{}', falling back to STRING.",
                    colStruct.getString("name"),
                    e);
            return DataTypeUtils.fromFlinkDataType(DataTypes.STRING());
        }
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
