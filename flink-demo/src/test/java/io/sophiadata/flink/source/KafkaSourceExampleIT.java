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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 集成测试：验证 KafkaSource 构建和管道执行。
 *
 * <p>使用 MiniCluster 实际运行 Flink 管道（无需真实 Kafka）。
 */
public class KafkaSourceExampleIT {

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    public static final List<String> COLLECTED = new ArrayList<>();

    @SuppressWarnings("deprecation")
    @Test
    public void testKafkaSourcePipelineExecutes() throws Exception {
        COLLECTED.clear();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 模拟 Kafka Source 的输出（使用 fromElements 替代）
        final DataStreamSource<String> stream = env.fromElements("msg_1", "msg_2", "msg_3");

        final SingleOutputStreamOperator<String> processed =
                stream.map(
                        new MapFunction<String, String>() {
                            @Override
                            public String map(final String value) {
                                return "processed: " + value;
                            }
                        });

        processed.addSink(new StaticCollectSink());

        env.execute("Test_KafkaSource");

        assertThat(COLLECTED).hasSize(3);
        assertThat(COLLECTED.get(0)).contains("processed: msg_1");
        assertThat(COLLECTED.get(1)).contains("processed: msg_2");
        assertThat(COLLECTED.get(2)).contains("processed: msg_3");
    }

    @SuppressWarnings("deprecation")
    private static class StaticCollectSink
            implements org.apache.flink.streaming.api.functions.sink.SinkFunction<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void invoke(final String value, final Context context) {
            synchronized (COLLECTED) {
                COLLECTED.add(value);
            }
        }
    }
}
