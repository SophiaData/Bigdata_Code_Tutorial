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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import io.sophiadata.flink.base.BaseCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Window + ProcessFunction 示例 —— 演示滑动窗口内 TopN 的实现。
 *
 * <p>场景：对传感器读数按窗口聚合，找出每个窗口内读数最多的 Top 3 传感器。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.streaming.advanced.WindowWithProcessFunction
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>TumblingProcessingTimeWindows 的使用
 *   <li>MapState 在窗口 ProcessFunction 中的应用
 *   <li>TopN 实现模式：先聚合再排序
 * </ul>
 */
public class WindowWithProcessFunction extends BaseCode {

    static final int TOP_N = 3;

    public static void main(final String[] args) throws Exception {
        new WindowWithProcessFunction().init(args, "Window_TopN_Example");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        env.setParallelism(1);

        // 模拟传感器数据流：(sensorId, reading)
        final DataStream<Tuple2<String, Double>> source =
                env.fromElements(
                        Tuple2.of("sensor_1", 10.0),
                        Tuple2.of("sensor_2", 20.0),
                        Tuple2.of("sensor_1", 15.0),
                        Tuple2.of("sensor_3", 30.0),
                        Tuple2.of("sensor_2", 25.0),
                        Tuple2.of("sensor_1", 12.0),
                        Tuple2.of("sensor_3", 35.0),
                        Tuple2.of("sensor_2", 22.0),
                        Tuple2.of("sensor_1", 18.0),
                        Tuple2.of("sensor_3", 28.0));

        // 窗口聚合 + TopN
        final DataStream<String> topNResult =
                source.keyBy(value -> value.f0)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                        .process(new SensorAggregator())
                        .name("Sensor_Aggregator")
                        .keyBy(value -> 0)
                        .process(new TopNProcessor(TOP_N))
                        .name("TopN_Processor");

        topNResult.print("TopN").setParallelism(1);
    }

    /** 窗口聚合：统计每个传感器在窗口内的读数总和。 */
    public static class SensorAggregator
            extends ProcessWindowFunction<
                    Tuple2<String, Double>, Tuple2<String, Double>, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void process(
                final String key,
                final Context context,
                final Iterable<Tuple2<String, Double>> elements,
                final Collector<Tuple2<String, Double>> out)
                throws Exception {
            double sum = 0.0;
            for (final Tuple2<String, Double> element : elements) {
                sum += element.f1;
            }
            out.collect(Tuple2.of(key, sum));
        }
    }

    /** TopN 处理：从聚合结果中找出 Top N。 */
    public static class TopNProcessor
            extends KeyedProcessFunction<Integer, Tuple2<String, Double>, String> {
        private static final long serialVersionUID = 1L;
        private final int topN;
        private transient MapState<String, Double> sensorState;

        public TopNProcessor(final int topN) {
            this.topN = topN;
        }

        @Override
        public void open(final org.apache.flink.configuration.Configuration parameters)
                throws Exception {
            sensorState =
                    getRuntimeContext()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            "sensor-values", Types.STRING, Types.DOUBLE));
        }

        @Override
        public void processElement(
                final Tuple2<String, Double> value, final Context ctx, final Collector<String> out)
                throws Exception {
            sensorState.put(value.f0, value.f1);

            final List<Map.Entry<String, Double>> all = new ArrayList<>();
            for (final Map.Entry<String, Double> entry : sensorState.entries()) {
                all.add(entry);
            }
            all.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

            final StringBuilder sb = new StringBuilder();
            sb.append("=== Top ").append(topN).append(" 传感器 ===").append(System.lineSeparator());
            final int limit = Math.min(topN, all.size());
            for (int i = 0; i < limit; i++) {
                final Map.Entry<String, Double> entry = all.get(i);
                sb.append("  #")
                        .append(i + 1)
                        .append(" ")
                        .append(entry.getKey())
                        .append(": ")
                        .append(String.format("%.2f", entry.getValue()))
                        .append(System.lineSeparator());
            }
            out.collect(sb.toString());
        }
    }
}
