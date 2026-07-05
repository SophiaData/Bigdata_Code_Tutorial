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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
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

/** 集成测试：验证 WindowWithProcessFunction 的窗口聚合和 TopN 逻辑。 */
public class WindowWithProcessFunctionIT {

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
    public void testSensorAggregatorSumsValues() throws Exception {
        COLLECTED.clear();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 使用 Event Time 窗口 + Watermark
        env.fromElements(
                        Tuple2.of("sensor_1", 10.0),
                        Tuple2.of("sensor_2", 20.0),
                        Tuple2.of("sensor_1", 15.0))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Double>>forBoundedOutOfOrderness(
                                        Duration.ZERO)
                                .withTimestampAssigner(
                                        (event, timestamp) -> System.currentTimeMillis()))
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new WindowWithProcessFunction.SensorAggregator())
                .name("Test_Aggregator")
                .map(t -> t.f0 + ":" + t.f1)
                .addSink(new StaticCollectSink());

        env.execute("Test_SensorAggregator");

        assertThat(COLLECTED).hasSize(2);
        assertThat(COLLECTED.get(0)).contains("25.0");
        assertThat(COLLECTED.get(1)).contains("20.0");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testTopNProcessorSortsCorrectly() throws Exception {
        COLLECTED.clear();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TopNProcessor 在每条数据到达时都输出当前 TopN
        env.fromElements(
                        Tuple2.of("sensor_1", 100.0),
                        Tuple2.of("sensor_2", 200.0),
                        Tuple2.of("sensor_3", 50.0))
                .keyBy(value -> 0)
                .process(new WindowWithProcessFunction.TopNProcessor(2))
                .name("Test_TopN")
                .addSink(new StaticCollectSink());

        env.execute("Test_TopN");

        // 最后一条输出包含所有 3 个传感器的排序结果
        assertThat(COLLECTED).hasSize(3);
        final String lastResult = COLLECTED.get(2);
        assertThat(lastResult).contains("Top 2");
        assertThat(lastResult).contains("sensor_2");
        assertThat(lastResult).contains("sensor_1");
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
