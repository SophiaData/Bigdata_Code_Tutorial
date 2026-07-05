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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** 集成测试：验证 Window Join 管道执行。 */
public class IntervalJoinExampleIT {

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
    public void testWindowJoinExecutes() throws Exception {
        COLLECTED.clear();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final DataStream<Tuple3<String, String, Long>> orderStream =
                env.fromElements(
                                Tuple3.of("user_1", "order_1", 1000L),
                                Tuple3.of("user_2", "order_2", 2000L))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(
                                                Duration.ZERO)
                                        .withTimestampAssigner((event, ts) -> event.f2));

        final DataStream<Tuple3<String, String, Long>> paymentStream =
                env.fromElements(
                                Tuple3.of("user_1", "pay_1", 1500L),
                                Tuple3.of("user_2", "pay_2", 2500L))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(
                                                Duration.ZERO)
                                        .withTimestampAssigner((event, ts) -> event.f2));

        orderStream
                .join(paymentStream)
                .where(order -> order.f0)
                .equalTo(payment -> payment.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(
                        new JoinFunction<
                                Tuple3<String, String, Long>,
                                Tuple3<String, String, Long>,
                                String>() {
                            @Override
                            public String join(
                                    final Tuple3<String, String, Long> order,
                                    final Tuple3<String, String, Long> payment) {
                                return "User "
                                        + order.f0
                                        + " paid "
                                        + payment.f1
                                        + " for "
                                        + order.f1;
                            }
                        })
                .addSink(new StaticCollectSink());

        env.execute("Test_WindowJoin");

        assertThat(COLLECTED).hasSize(2);
        assertThat(COLLECTED).anyMatch(s -> s.contains("user_1"));
        assertThat(COLLECTED).anyMatch(s -> s.contains("user_2"));
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
