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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class WatermarkWithLateDataExampleTest {

    @Test
    void shouldCreateWatermarkStrategy() {
        final WatermarkStrategy<Tuple3<String, Long, Long>> strategy =
                WatermarkStrategy.<Tuple3<String, Long, Long>>forBoundedOutOfOrderness(
                                Duration.ofSeconds(5))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f2);
        assertThat(strategy).isNotNull();
    }

    @Test
    void shouldCreateSideOutputTag() {
        final OutputTag<Tuple3<String, Long, Long>> lateTag =
                new OutputTag<Tuple3<String, Long, Long>>("late-data") {};
        assertThat(lateTag.getId()).isEqualTo("late-data");
    }

    @Test
    void shouldBuildPipelineWithoutError() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final WatermarkStrategy<Tuple3<String, Long, Long>> strategy =
                WatermarkStrategy.<Tuple3<String, Long, Long>>forBoundedOutOfOrderness(
                                Duration.ofSeconds(5))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f2);

        final OutputTag<Tuple3<String, Long, Long>> lateTag =
                new OutputTag<Tuple3<String, Long, Long>>("late-data") {};

        final SingleOutputStreamOperator<String> result =
                env.fromElements(
                                Tuple3.of("order_1", 100L, 1000L),
                                Tuple3.of("order_2", 200L, 2000L))
                        .assignTimestampsAndWatermarks(strategy)
                        .keyBy(t -> "all")
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .allowedLateness(Duration.ofSeconds(5))
                        .sideOutputLateData(lateTag)
                        .process(
                                new org.apache.flink.streaming.api.functions.windowing
                                                .ProcessWindowFunction<
                                        Tuple3<String, Long, Long>,
                                        String,
                                        String,
                                        org.apache.flink.streaming.api.windowing.windows
                                                .TimeWindow>() {
                                    @Override
                                    public void process(
                                            final String key,
                                            final Context context,
                                            final Iterable<Tuple3<String, Long, Long>> elements,
                                            final org.apache.flink.util.Collector<String> out) {
                                        long sum = 0;
                                        int count = 0;
                                        for (final Tuple3<String, Long, Long> e : elements) {
                                            sum += e.f1;
                                            count++;
                                        }
                                        out.collect("orders=" + count + ", total=" + sum);
                                    }
                                });

        assertThat(result).isNotNull();
    }
}
