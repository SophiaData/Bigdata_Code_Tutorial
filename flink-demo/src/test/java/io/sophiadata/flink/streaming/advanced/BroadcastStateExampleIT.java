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

package io.sophiadata.flink.streaming.advanced;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** 集成测试：验证 Broadcast State 管道执行。 */
public class BroadcastStateExampleIT {

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
    public void testBroadcastStatePipelineExecutes() throws Exception {
        COLLECTED.clear();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final DataStream<Tuple2<String, String>> ruleStream =
                env.fromElements(Tuple2.of("rule_1", "error"), Tuple2.of("rule_2", "warning"));

        final DataStream<Tuple2<String, String>> dataStream =
                env.fromElements(
                        Tuple2.of("user_1", "error: connection failed"),
                        Tuple2.of("user_2", "info: login success"),
                        Tuple2.of("user_3", "warning: disk low"));

        final BroadcastStream<Tuple2<String, String>> broadcastRules =
                ruleStream.broadcast(BroadcastStateExample.RULE_DESCRIPTOR);

        dataStream
                .keyBy(t -> t.f0)
                .connect(broadcastRules)
                .process(new BroadcastStateExample.RuleMatchFunction())
                .addSink(new StaticCollectSink());

        env.execute("Test_BroadcastState");

        assertThat(COLLECTED).isNotEmpty();
        assertThat(COLLECTED).anyMatch(s -> s.contains("matched rule"));
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
