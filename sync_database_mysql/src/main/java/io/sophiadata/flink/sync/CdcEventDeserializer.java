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

        final String op = valueStruct.getString("op");
        if (op == null) {
            return;
        }

        final org.apache.kafka.connect.data.Struct source = valueStruct.getStruct("source");
        if (source == null) {
            LOG.warn("Missing source info, skipping event");
            return;
        }
        final String dbName = source.getString("db");
        final String tableName = source.getString("table");

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

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
