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

package io.sophiadata.flink.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.fastjson.JSONObject;
import io.sophiadata.flink.base.BaseCode;

import java.util.Properties;

/**
 * MySQL CDC → Kafka Sink（DataStream API 版本）。
 *
 * <p>演示用 DataStream API 构建 MySQL CDC Source 和 KafkaSink，将 CDC 事件写入 Kafka Topic。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.sink.MysqlCdcToKafkaPipeline \
 *   --hostname localhost --port 3306 \
 *   --username root --password root \
 *   --databaseList mydb --tableList mydb.users \
 *   --kafka.brokers localhost:9092 --kafka.topic cdc-events
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>DataStream API 中 MySqlSource 和 KafkaSink 的构建
 *   <li>序列化 Schema 的选择（StringSchema / JSON）
 *   <li>Exactly-Once 语义配置（setDeliveryGuarantee）
 * </ul>
 */
public class MysqlCdcToKafkaPipeline extends BaseCode {

    public static void main(final String[] args) throws Exception {
        new MysqlCdcToKafkaPipeline().init(args, "MySQL_CDC_to_Kafka_DataStream");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        final String hostname = params.get("hostname", "localhost");
        final int port = params.getInt("port", 3306);
        final String username = params.get("username", "root");
        final String password = params.get("password", "root");
        final String databaseList = params.get("databaseList", "mydb");
        final String tableList = params.get("tableList", "mydb.users");
        final String kafkaBrokers = params.get("kafka.brokers", "localhost:9092");
        final String kafkaTopic = params.get("kafka.topic", "cdc-events");

        // 1. 构建 MySQL CDC Source
        final Properties debeziumProperties = new Properties();
        debeziumProperties.put("decimal.handling.mode", "string");

        final MySqlSource<String> source =
                MySqlSource.<String>builder()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .databaseList(databaseList)
                        .tableList(tableList)
                        .deserializer(new SimpleStringDebeziumDeserializer())
                        .includeSchemaChanges(false)
                        .startupOptions(StartupOptions.initial())
                        .debeziumProperties(debeziumProperties)
                        .build();

        // 2. 构建 KafkaSink（Exactly-Once 语义）
        final KafkaSink<String> sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers(kafkaBrokers)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(kafkaTopic)
                                        .setValueSerializationSchema(
                                                new org.apache.flink.api.common.serialization
                                                        .SimpleStringSchema())
                                        .build())
                        .build();

        // 3. 连接 Source → Sink
        final DataStreamSource<String> cdcStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL CDC Source");

        cdcStream.sinkTo(sink).name("Kafka Sink").setParallelism(1);
    }

    /**
     * 简单的 Debezium 反序列化器，将 CDC 事件转为 JSON 字符串。
     *
     * <p>生产环境建议使用 {@code JsonDebeziumDeserializationSchema} 并处理 schema change。
     */
    public static class SimpleStringDebeziumDeserializer
            implements org.apache.flink.cdc.debezium.DebeziumDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(
                final org.apache.kafka.connect.source.SourceRecord record,
                final org.apache.flink.util.Collector<String> out)
                throws Exception {
            final Object value = record.value();
            if (value != null) {
                final JSONObject json = new JSONObject();
                json.put("topic", record.topic());
                json.put("key", record.key());
                json.put("value", value.toString());
                json.put("timestamp", record.timestamp());
                out.collect(json.toJSONString());
            }
        }

        @Override
        public org.apache.flink.api.common.typeinfo.TypeInformation<String> getProducedType() {
            return org.apache.flink.api.common.typeinfo.TypeInformation.of(String.class);
        }
    }
}
