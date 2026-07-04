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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 端到端测试：验证 Flink 数据实际写入 Kafka 并能被消费。
 *
 * <p>需要 Docker 环境。无 Docker 时自动跳过。
 */
public class MysqlCdcToKafkaPipelineE2E {

    static KafkaContainer kafka;

    @ClassRule
    public static final org.apache.flink.test.util.MiniClusterWithClientResource MINI_CLUSTER =
            new org.apache.flink.test.util.MiniClusterWithClientResource(
                    new org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
                                    .Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    @BeforeClass
    public static void startKafka() {
        Assume.assumeTrue("Docker not available, skipping E2E test", isDockerAvailable());
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));
        kafka.start();
    }

    @AfterClass
    public static void stopKafka() {
        if (kafka != null) {
            kafka.stop();
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testWriteToKafkaAndConsume() throws Exception {
        final String topic = "test-flink-cdc-output";
        final String bootstrapServers = kafka.getBootstrapServers();

        final org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env =
                org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
                        .getExecutionEnvironment();
        env.setParallelism(1);

        final KafkaSink<String> sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(topic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .build();

        env.fromElements(
                        "{\"id\":1,\"name\":\"Alice\"}",
                        "{\"id\":2,\"name\":\"Bob\"}",
                        "{\"id\":3,\"name\":\"Charlie\"}")
                .sinkTo(sink);

        env.execute("Kafka_E2E_Write");

        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "e2e-test-group");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        final List<String> consumed = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            final long deadline = System.currentTimeMillis() + 30_000;
            while (consumed.size() < 3 && System.currentTimeMillis() < deadline) {
                final var records = consumer.poll(Duration.ofMillis(1000));
                for (final ConsumerRecord<String, String> record : records) {
                    consumed.add(record.value());
                }
            }
        }

        assertThat(consumed).hasSize(3);
        assertThat(consumed).anyMatch(s -> s.contains("Alice"));
        assertThat(consumed).anyMatch(s -> s.contains("Bob"));
        assertThat(consumed).anyMatch(s -> s.contains("Charlie"));
    }

    private static boolean isDockerAvailable() {
        try {
            final Process p = Runtime.getRuntime().exec(new String[] {"docker", "info"});
            return p.waitFor() == 0;
        } catch (final IOException | InterruptedException e) {
            return false;
        }
    }
}
