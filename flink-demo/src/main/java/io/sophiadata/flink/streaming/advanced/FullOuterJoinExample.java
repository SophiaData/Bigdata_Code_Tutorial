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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import io.sophiadata.flink.base.BaseCode;

import java.time.Duration;

/**
 * 双流 Join 示例 —— 演示 Window Join 实现流流关联。
 *
 * <p>场景：订单流和支付流在 5 秒窗口内按 userId 关联。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.streaming.advanced.FullOuterJoinExample
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>orderStream.join(paymentStream).where().equalTo() 内连接
 *   <li>TumblingEventTimeWindows 窗口关联
 *   <li>WatermarkStrategy 处理乱序数据
 *   <li>对比 CoGroup 实现全外连接的区别
 * </ul>
 */
public class FullOuterJoinExample extends BaseCode {

    public static void main(final String[] args) throws Exception {
        new FullOuterJoinExample().init(args, "FullOuterJoin_Example");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        final DataStream<Tuple3<String, String, Long>> orders = createOrderStream(env);
        final DataStream<Tuple3<String, String, Long>> payments = createPaymentStream(env);

        // Window Join：5 秒窗口内按 userId 关联
        final DataStream<String> result =
                orders.join(payments)
                        .where(order -> order.f0)
                        .equalTo(payment -> payment.f0)
                        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                        .apply(
                                (order, payment) ->
                                        "User "
                                                + order.f0
                                                + " | Order: "
                                                + order.f1
                                                + " | Payment: "
                                                + payment.f1
                                                + " | Amount: "
                                                + payment.f2);

        result.print("Join_Result").setParallelism(1);
    }

    private DataStream<Tuple3<String, String, Long>> createOrderStream(
            final StreamExecutionEnvironment env) {
        return env.fromElements(
                        Tuple3.of("user_1", "order_1", 1000L),
                        Tuple3.of("user_2", "order_2", 2000L),
                        Tuple3.of("user_1", "order_3", 3000L),
                        Tuple3.of("user_3", "order_4", 4000L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(
                                        Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.f2));
    }

    private DataStream<Tuple3<String, String, Long>> createPaymentStream(
            final StreamExecutionEnvironment env) {
        return env.fromElements(
                        Tuple3.of("user_1", "pay_1", 1500L),
                        Tuple3.of("user_2", "pay_2", 2500L),
                        Tuple3.of("user_1", "pay_3", 3500L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(
                                        Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.f2));
    }
}
