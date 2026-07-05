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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import io.sophiadata.flink.base.BaseCode;

/**
 * Timer 定时器示例 —— 演示处理时间/事件时间定时器和超时检测。
 *
 * <p>场景：用户行为监控，5 秒内无新事件则触发超时告警。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.streaming.advanced.TimerExample
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>ctx.timerService().registerProcessingTimeTimer() 处理时间定时器
 *   <li>ctx.timerService().registerEventTimeTimer() 事件时间定时器
 *   <li>onTimer() 回调处理
 *   <li>超时检测模式
 * </ul>
 */
public class TimerExample extends BaseCode {

    private static final long TIMEOUT_MS = 5000L;

    public static void main(final String[] args) throws Exception {
        new TimerExample().init(args, "Timer_Example");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        // 模拟用户行为流：(userId, action)
        final DataStream<Tuple2<String, String>> stream =
                env.fromElements(
                        Tuple2.of("user_1", "click"),
                        Tuple2.of("user_2", "scroll"),
                        Tuple2.of("user_1", "purchase"),
                        Tuple2.of("user_3", "click"));

        stream.keyBy(t -> t.f0)
                .process(new TimeoutDetector())
                .print("Timer_Result")
                .setParallelism(1);
    }

    /** 超时检测：用户超过 5 秒无活动则告警。 */
    public static class TimeoutDetector
            extends KeyedProcessFunction<String, Tuple2<String, String>, String> {

        private static final long serialVersionUID = 1L;
        private transient ValueState<Long> lastEventTime;

        @Override
        public void open(final Configuration parameters) throws Exception {
            lastEventTime =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("lastEventTime", Long.class));
        }

        @Override
        public void processElement(
                final Tuple2<String, String> value, final Context ctx, final Collector<String> out)
                throws Exception {
            final long now = ctx.timerService().currentProcessingTime();
            lastEventTime.update(now);

            // 注册/更新定时器
            ctx.timerService().deleteProcessingTimeTimer(now + TIMEOUT_MS);
            ctx.timerService().registerProcessingTimeTimer(now + TIMEOUT_MS);

            out.collect(value.f0 + " performed: " + value.f1);
        }

        @Override
        public void onTimer(
                final long timestamp, final OnTimerContext ctx, final Collector<String> out)
                throws Exception {
            final Long lastTime = lastEventTime.value();
            if (lastTime != null && timestamp == lastTime + TIMEOUT_MS) {
                out.collect("TIMEOUT: " + ctx.getCurrentKey() + " inactive for 5s");
            }
        }
    }
}
