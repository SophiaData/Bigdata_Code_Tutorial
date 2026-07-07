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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class FullOuterJoinExampleTest {

    @Test
    void shouldBuildJoinPipeline() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final var orders =
                env.fromElements(
                                Tuple3.of("user_1", "order_1", 1000L),
                                Tuple3.of("user_2", "order_2", 2000L))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(
                                                Duration.ZERO)
                                        .withTimestampAssigner((event, ts) -> event.f2));

        final var payments =
                env.fromElements(
                                Tuple3.of("user_1", "pay_1", 1500L),
                                Tuple3.of("user_2", "pay_2", 2500L))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(
                                                Duration.ZERO)
                                        .withTimestampAssigner((event, ts) -> event.f2));

        final var result =
                orders.join(payments)
                        .where(order -> order.f0)
                        .equalTo(payment -> payment.f0)
                        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                        .apply((order, payment) -> "User " + order.f0 + " paid " + payment.f2);

        assertThat(result).isNotNull();
    }
}
