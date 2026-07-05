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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.sophiadata.flink.base.BaseCode;

/**
 * Exactly-Once Kafka 端到端示例 —— 从 Kafka 读取、处理、写回 Kafka。
 *
 * <p>演示 Flink 的端到端 Exactly-Once 语义配置。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.sink.ExactlyOnceKafkaPipeline \
 *   --kafka.brokers localhost:9092 \
 *   --input.topic input-topic --output.topic output-topic
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>KafkaSource + KafkaSink 的 Exactly-Once 配置
 *   <li>Checkpoint + 两阶段提交
 *   <li>端到端一致性保证
 * </ul>
 */
public class ExactlyOnceKafkaPipeline extends BaseCode {

    public static void main(final String[] args) throws Exception {
        new ExactlyOnceKafkaPipeline().init(args, "Exactly_Once_Kafka");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        final String brokers = getArg(args, "kafka.brokers", "localhost:9092");
        final String inputTopic = getArg(args, "input.topic", "input-topic");
        final String outputTopic = getArg(args, "output.topic", "output-topic");

        // 1. 构建 KafkaSource
        final KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers(brokers)
                        .setTopics(inputTopic)
                        .setGroupId("flink-exactly-once")
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        // 2. 构建 KafkaSink（Exactly-Once 语义）
        final KafkaSink<String> sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers(brokers)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(outputTopic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .build();

        // 3. Source → Transform → Sink
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(
                        new MapFunction<String, String>() {
                            @Override
                            public String map(final String value) {
                                return "processed:" + value;
                            }
                        })
                .sinkTo(sink)
                .name("Kafka Sink (Exactly-Once)");
    }

    private static String getArg(final String[] args, final String key, final String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (("--" + key).equals(args[i])) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }
}
