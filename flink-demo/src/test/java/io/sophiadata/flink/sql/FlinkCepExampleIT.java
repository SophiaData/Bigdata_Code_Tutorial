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

package io.sophiadata.flink.sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** 集成测试：验证 CEP 模式匹配管道执行。 */
public class FlinkCepExampleIT {

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
    public void testCepPipelineExecutes() throws Exception {
        COLLECTED.clear();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final DataStream<Tuple3<String, String, Long>> loginStream =
                env.fromElements(
                                Tuple3.of("user_1", "login_fail", 1000L),
                                Tuple3.of("user_1", "login_fail", 2000L),
                                Tuple3.of("user_1", "login_fail", 3000L))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(
                                                Duration.ZERO)
                                        .withTimestampAssigner((event, ts) -> event.f2));

        final Pattern<Tuple3<String, String, Long>, ?> pattern =
                Pattern.<Tuple3<String, String, Long>>begin("start")
                        .where(
                                new SimpleCondition<Tuple3<String, String, Long>>() {
                                    @Override
                                    public boolean filter(
                                            final Tuple3<String, String, Long> value) {
                                        return "login_fail".equals(value.f1);
                                    }
                                })
                        .times(3)
                        .within(org.apache.flink.streaming.api.windowing.time.Time.seconds(10));

        final PatternStream<Tuple3<String, String, Long>> patternStream =
                CEP.pattern(loginStream.keyBy(t -> t.f0), pattern);

        patternStream
                .select(
                        new PatternSelectFunction<Tuple3<String, String, Long>, String>() {
                            @Override
                            public String select(
                                    final java.util.Map<
                                                    String,
                                                    java.util.List<Tuple3<String, String, Long>>>
                                            pattern) {
                                final java.util.List<Tuple3<String, String, Long>> start =
                                        pattern.get("start");
                                return "ALERT: " + start.get(0).f0 + " failed 3 times";
                            }
                        })
                .addSink(new StaticCollectSink());

        env.execute("Test_CEP");

        assertThat(COLLECTED).hasSize(1);
        assertThat(COLLECTED.get(0)).contains("ALERT");
        assertThat(COLLECTED.get(0)).contains("user_1");
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
