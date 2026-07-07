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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import io.sophiadata.flink.base.BaseCode;

/**
 * ProcessFunction 进阶示例 —— 演示 Event Time 定时器、注销定时器、定时器 + 状态组合。
 *
 * <p>场景：订单超时监控，用户下单后 10 秒内未支付则取消订单。支持：
 *
 * <ul>
 *   <li>Event Time 定时器（基于事件时间推进）
 *   <li>动态注销/重置定时器
 *   <li>定时器 + ValueState 组合实现复杂业务逻辑
 * </ul>
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.streaming.advanced.ProcessFunctionAdvancedExample
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>registerEventTimeTimer() 事件时间定时器
 *   <li>deleteEventTimeTimer() 注销定时器
 *   <li>定时器 + 状态组合实现超时检测
 *   <li>Context.timerService().currentWatermark() 获取当前 Watermark
 * </ul>
 */
public class ProcessFunctionAdvancedExample extends BaseCode {

    private static final long ORDER_TIMEOUT_MS = 10_000L;

    public static void main(final String[] args) throws Exception {
        new ProcessFunctionAdvancedExample().init(args, "ProcessFunction_Advanced_Example");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        // (orderId, action, timestamp)
        final DataStream<Tuple3<String, String, Long>> source =
                env.fromElements(
                        Tuple3.of("order_1", "create", 1000L),
                        Tuple3.of("order_2", "create", 2000L),
                        Tuple3.of("order_1", "pay", 5000L),
                        Tuple3.of("order_3", "create", 3000L),
                        Tuple3.of("order_2", "pay", 8000L),
                        // order_3 未支付，会超时
                        Tuple3.of("order_4", "create", 4000L),
                        Tuple3.of("order_4", "cancel", 6000L));

        source.keyBy(t -> t.f0)
                .process(new OrderTimeoutMonitor())
                .print("Order_Result")
                .setParallelism(1);
    }

    /** 订单超时监控：基于 Event Time 定时器实现。 */
    public static class OrderTimeoutMonitor
            extends KeyedProcessFunction<String, Tuple3<String, String, Long>, String> {

        private static final long serialVersionUID = 1L;
        private transient ValueState<Long> createTimeState;
        private transient ValueState<Boolean> paidState;

        @Override
        public void open(final Configuration parameters) throws Exception {
            createTimeState =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("createTime", Long.class));
            paidState =
                    getRuntimeContext().getState(new ValueStateDescriptor<>("paid", Boolean.class));
        }

        @Override
        public void processElement(
                final Tuple3<String, String, Long> value,
                final Context ctx,
                final Collector<String> out)
                throws Exception {
            final String action = value.f1;

            if ("create".equals(action)) {
                // 记录创建时间，注册 Event Time 定时器
                createTimeState.update(value.f2);
                paidState.update(false);

                final long timerTime = value.f2 + ORDER_TIMEOUT_MS;
                ctx.timerService().registerEventTimeTimer(timerTime);

                out.collect(value.f0 + " created, timer set at " + timerTime);

            } else if ("pay".equals(action)) {
                // 支付成功，注销定时器
                final Long createTime = createTimeState.value();
                if (createTime != null) {
                    ctx.timerService().deleteEventTimeTimer(createTime + ORDER_TIMEOUT_MS);
                }
                paidState.update(true);
                out.collect(value.f0 + " paid, timer cancelled");

            } else if ("cancel".equals(action)) {
                // 取消订单，注销定时器
                final Long createTime = createTimeState.value();
                if (createTime != null) {
                    ctx.timerService().deleteEventTimeTimer(createTime + ORDER_TIMEOUT_MS);
                }
                out.collect(value.f0 + " cancelled, timer cancelled");
            }
        }

        @Override
        public void onTimer(
                final long timestamp, final OnTimerContext ctx, final Collector<String> out)
                throws Exception {
            final Boolean paid = paidState.value();
            if (paid != null && !paid) {
                out.collect("TIMEOUT: " + ctx.getCurrentKey() + " order cancelled (unpaid)");
            }
        }
    }
}
