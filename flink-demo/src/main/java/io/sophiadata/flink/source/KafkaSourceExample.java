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

package io.sophiadata.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.sophiadata.flink.base.BaseCode;

/**
 * Kafka Source 示例 —— 从 Kafka Topic 读取数据并处理。
 *
 * <p>演示 KafkaSource 构建器模式、偏移策略、Consumer Group 配置。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.source.KafkaSourceExample \
 *   --kafka.brokers localhost:9092 --kafka.topic input-topic
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>KafkaSource 构建器模式
 *   <li>OffsetsInitializer 的 4 种起始偏移策略
 *   <li>Consumer Group 和并行度配置
 * </ul>
 */
public class KafkaSourceExample extends BaseCode {

    public static void main(final String[] args) throws Exception {
        new KafkaSourceExample().init(args, "Kafka_Source_Example");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        final String brokers = getArg(args, "kafka.brokers", "localhost:9092");
        final String topic = getArg(args, "kafka.topic", "input-topic");
        final String groupId = getArg(args, "kafka.group.id", "flink-demo-group");

        // 1. 构建 KafkaSource
        final KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers(brokers)
                        .setTopics(topic)
                        .setGroupId(groupId)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        // 2. 从 Source 读取数据
        final DataStreamSource<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 3. 处理数据
        final SingleOutputStreamOperator<String> processed =
                stream.map(
                        new MapFunction<String, String>() {
                            @Override
                            public String map(final String value) {
                                return "processed: " + value;
                            }
                        });

        // 4. 输出结果
        processed.print("Result").setParallelism(1);
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
