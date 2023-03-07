/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterType;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import static io.sophiadata.flink.sync.DebeziumJsonConstant.AFTER;
import static io.sophiadata.flink.sync.DebeziumJsonConstant.BEFORE;
import static io.sophiadata.flink.sync.DebeziumJsonConstant.SOURCE;
import static io.sophiadata.flink.sync.DebeziumJsonConstant.TABLE;
import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;
import static org.apache.flink.types.RowKind.UPDATE_BEFORE;
import static org.apache.kafka.connect.json.JsonConverterConfig.DECIMAL_FORMAT_CONFIG;
import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;

/** (@SophiaData) (@date 2023/3/7 20:20). */
@Getter
public abstract class RowKindJsonDeserializationSchemaBase
        implements DebeziumDeserializationSchema<
                RowKindJsonDeserializationSchemaBase.TableIRowKindJson> {
    private transient JsonConverter jsonConverter;
    private final boolean includeSchema;
    private final Map<String, Object> customConverterConfigs = Maps.newHashMap();

    public RowKindJsonDeserializationSchemaBase() {
        this(false);
    }

    public RowKindJsonDeserializationSchemaBase(boolean includeSchema) {
        this(includeSchema, Collections.emptyMap());
    }

    public RowKindJsonDeserializationSchemaBase(
            boolean includeSchema, Map<String, Object> customConverterConfigs) {
        this.includeSchema = includeSchema;
        this.customConverterConfigs.put("converter.type", ConverterType.VALUE.getName());
        this.customConverterConfigs.put(SCHEMAS_ENABLE_CONFIG, isIncludeSchema());
        this.customConverterConfigs.put(DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        this.customConverterConfigs.putAll(customConverterConfigs);
    }

    abstract JsonConverter initializeJsonConverter(Map<String, Object> customConverterConfigs);

    @Override
    public void deserialize(SourceRecord sr, Collector<TableIRowKindJson> out) {
        if (this.jsonConverter == null) {
            this.jsonConverter = this.initializeJsonConverter(this.customConverterConfigs);
        }

        Envelope.Operation op = Envelope.operationFor(sr);
        Struct value = (Struct) sr.value();
        final Struct source = value.getStruct(SOURCE);
        String table = source.getString(TABLE);
        Schema valueSchema = sr.valueSchema();
        if (op != Envelope.Operation.CREATE && op != Envelope.Operation.READ) {
            if (op == Envelope.Operation.DELETE) {
                emit(
                        new TableIRowKindJson(
                                table, DELETE, extractBeforeRow(sr.topic(), value, valueSchema)),
                        out);
            } else {
                emit(
                        new TableIRowKindJson(
                                table,
                                UPDATE_BEFORE,
                                extractBeforeRow(sr.topic(), value, valueSchema)),
                        out);
                emit(
                        new TableIRowKindJson(
                                table,
                                UPDATE_AFTER,
                                extractAfterRow(sr.topic(), value, valueSchema)),
                        out);
            }
        } else {
            emit(
                    new TableIRowKindJson(
                            table, INSERT, extractAfterRow(sr.topic(), value, valueSchema)),
                    out);
        }
    }

    private String extractAfterRow(String topic, Struct value, Schema valueSchema) {
        return new String(
                jsonConverter.fromConnectData(
                        topic, valueSchema.field(AFTER).schema(), value.getStruct(AFTER)),
                StandardCharsets.UTF_8);
    }

    private String extractBeforeRow(String topic, Struct value, Schema valueSchema) {
        return new String(
                jsonConverter.fromConnectData(
                        topic, valueSchema.field(BEFORE).schema(), value.getStruct(BEFORE)),
                StandardCharsets.UTF_8);
    }

    private void emit(TableIRowKindJson row, Collector<TableIRowKindJson> collector) {
        collector.collect(row);
    }

    @Override
    public TypeInformation<TableIRowKindJson> getProducedType() {
        return TypeInformation.of(TableIRowKindJson.class);
    }

    /** 提取出 [table、rowKind、rowKing 对应结果的 json] 序列化结果. */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TableIRowKindJson implements Serializable {
        private String table;
        private RowKind rowKind;
        private String json;
    }
}
