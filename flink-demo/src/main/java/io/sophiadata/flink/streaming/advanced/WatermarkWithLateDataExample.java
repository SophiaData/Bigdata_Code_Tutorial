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

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import io.sophiadata.flink.base.BaseCode;

import java.time.Duration;

/**
 * Watermark + 延迟数据处理示例 —— 演示 Watermark 生成策略和延迟数据处理。
 *
 * <p>场景：电商订单统计，10 秒窗口内统计每 10 秒的订单金额。允许 5 秒延迟，超过 5 秒的迟到数据输出到侧输出流。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.streaming.advanced.WatermarkWithLateDataExample
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>WatermarkStrategy.forBoundedOutOfOrderness() 有界乱序 Watermark
 *   <li>allowedLateness() 允许延迟时间
 *   <li>sideOutputLateData() 侧输出迟到数据
 *   <li>SerializableTimestampAssigner 时间戳分配
 * </ul>
 */
public class WatermarkWithLateDataExample extends BaseCode {

    private static final Duration MAX_OUT_OF_ORDER = Duration.ofSeconds(5);
    private static final Duration ALLOWED_LATENESS = Duration.ofSeconds(5);

    public static void main(final String[] args) throws Exception {
        new WatermarkWithLateDataExample().init(args, "Watermark_LateData_Example");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        // (orderId, amount, timestamp)
        final DataStream<Tuple3<String, Long, Long>> source =
                env.fromElements(
                        Tuple3.of("order_1", 100L, 1000L),
                        Tuple3.of("order_2", 200L, 2000L),
                        Tuple3.of("order_3", 150L, 3000L),
                        // 正常数据
                        Tuple3.of("order_4", 300L, 8000L),
                        Tuple3.of("order_5", 250L, 9000L),
                        // 迟到数据（时间戳在早期窗口，但到达时间晚）
                        Tuple3.of("order_late_1", 50L, 1500L),
                        Tuple3.of("order_late_2", 60L, 2500L),
                        // 严重迟到数据（超过 allowedLateness）
                        Tuple3.of("order_very_late", 70L, 500L));

        final OutputTag<Tuple3<String, Long, Long>> lateTag =
                new OutputTag<Tuple3<String, Long, Long>>("late-data") {};

        final WatermarkStrategy<Tuple3<String, Long, Long>> watermarkStrategy =
                WatermarkStrategy.<Tuple3<String, Long, Long>>forBoundedOutOfOrderness(
                                MAX_OUT_OF_ORDER)
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<Tuple3<String, Long, Long>>)
                                        (element, recordTimestamp) -> element.f2);

        final SingleOutputStreamOperator<String> result =
                source.assignTimestampsAndWatermarks(watermarkStrategy)
                        .keyBy(t -> "all")
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .allowedLateness(ALLOWED_LATENESS)
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
                                            final Collector<String> out) {
                                        long sum = 0;
                                        int count = 0;
                                        for (final Tuple3<String, Long, Long> e : elements) {
                                            sum += e.f1;
                                            count++;
                                        }
                                        final org.apache.flink.streaming.api.windowing.windows
                                                        .TimeWindow
                                                window = context.window();
                                        out.collect(
                                                "Window ["
                                                        + window.getStart()
                                                        + ", "
                                                        + window.getEnd()
                                                        + "): "
                                                        + count
                                                        + " orders, total="
                                                        + sum);
                                    }
                                });

        result.print("Window_Result").setParallelism(1);

        // 侧输出迟到数据
        final DataStream<Tuple3<String, Long, Long>> lateStream = result.getSideOutput(lateTag);
        lateStream.print("Late_Data").setParallelism(1);
    }
}
