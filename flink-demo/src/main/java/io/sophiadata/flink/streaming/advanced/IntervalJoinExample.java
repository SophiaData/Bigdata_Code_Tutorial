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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import io.sophiadata.flink.base.BaseCode;

import java.time.Duration;

/**
 * Interval Join 示例 —— 流流时间区间关联（订单+支付场景）。
 *
 * <p>演示两条流按 key 和时间窗口进行关联。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.streaming.advanced.IntervalJoinExample
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>两条流按 key 关联
 *   <li>Window Join 的使用方式
 *   <li>处理乱序数据的 watermark 机制
 * </ul>
 */
public class IntervalJoinExample extends BaseCode {

    public static void main(final String[] args) throws Exception {
        new IntervalJoinExample().init(args, "Interval_Join_Example");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        // 1. 订单流：(userId, orderId, timestamp)
        final DataStream<Tuple3<String, String, Long>> orderStream =
                env.fromElements(
                                Tuple3.of("user_1", "order_1", 1000L),
                                Tuple3.of("user_2", "order_2", 2000L),
                                Tuple3.of("user_1", "order_3", 3000L),
                                Tuple3.of("user_3", "order_4", 4000L))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(
                                                Duration.ZERO)
                                        .withTimestampAssigner((event, ts) -> event.f2));

        // 2. 支付流：(userId, paymentId, timestamp)
        final DataStream<Tuple3<String, String, Long>> paymentStream =
                env.fromElements(
                                Tuple3.of("user_1", "pay_1", 1500L),
                                Tuple3.of("user_2", "pay_2", 2500L),
                                Tuple3.of("user_1", "pay_3", 3500L))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(
                                                Duration.ZERO)
                                        .withTimestampAssigner((event, ts) -> event.f2));

        // 3. Window Join：5 秒窗口内关联
        final DataStream<String> result =
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
                                });

        // 4. 输出结果
        result.print("Join_Result").setParallelism(1);
    }
}
