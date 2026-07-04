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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import io.sophiadata.flink.base.BaseCode;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink Keyed State 示例 —— 演示 ValueState / ListState / ReducingState 三种状态的用法。
 *
 * <p>场景：对流中的 (key, value) 数据进行三种状态处理：
 *
 * <ul>
 *   <li><b>ValueState</b>：统计每个 key 的出现次数
 *   <li><b>ListState</b>：收集每个 key 最近 5 条记录
 *   <li><b>ReducingState</b>：实时累加每个 key 的值
 * </ul>
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.streaming.advanced.ProcessFunctionStateExample
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>KeyedProcessFunction 的 open() / processElement() 生命周期
 *   <li>ValueState / ListState / ReducingState 的声明和使用
 *   <li>状态的读取（value()）和更新（update() / add()）
 * </ul>
 */
public class ProcessFunctionStateExample extends BaseCode {

    private static final int MAX_RECENT_RECORDS = 5;

    public static void main(final String[] args) throws Exception {
        new ProcessFunctionStateExample().init(args, "Keyed_State_Example");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        final DataStream<Tuple2<String, Long>> source =
                env.fromElements(
                        Tuple2.of("sensor_1", 10L),
                        Tuple2.of("sensor_1", 20L),
                        Tuple2.of("sensor_2", 30L),
                        Tuple2.of("sensor_1", 15L),
                        Tuple2.of("sensor_2", 25L),
                        Tuple2.of("sensor_1", 5L),
                        Tuple2.of("sensor_2", 40L),
                        Tuple2.of("sensor_1", 12L),
                        Tuple2.of("sensor_2", 35L),
                        Tuple2.of("sensor_1", 8L));

        final KeyedStream<Tuple2<String, Long>, String> keyed = source.keyBy(value -> value.f0);

        final DataStream<String> countResult =
                keyed.process(new CountFunction()).name("ValueState_Count");
        final DataStream<String> recentResult =
                keyed.process(new RecentFunction()).name("ListState_RecentRecords");
        final DataStream<String> sumResult =
                keyed.process(new SumFunction()).name("ReducingState_Sum");

        countResult.print("Count").setParallelism(1);
        recentResult.print("Recent").setParallelism(1);
        sumResult.print("Sum").setParallelism(1);
    }

    /** ValueState：统计出现次数。 */
    public static class CountFunction
            extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {
        private static final long serialVersionUID = 1L;
        private transient ValueState<Long> countState;

        @Override
        public void open(final org.apache.flink.configuration.Configuration parameters)
                throws Exception {
            countState =
                    getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
        }

        @Override
        public void processElement(
                final Tuple2<String, Long> value, final Context ctx, final Collector<String> out)
                throws Exception {
            Long count = countState.value();
            if (count == null) {
                count = 0L;
            }
            count += 1;
            countState.update(count);
            out.collect(value.f0 + " 出现了 " + count + " 次，当前值: " + value.f1);
        }
    }

    /** ListState：收集最近 N 条记录。 */
    public static class RecentFunction
            extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {
        private static final long serialVersionUID = 1L;
        private transient ListState<Long> recentState;

        @Override
        public void open(final org.apache.flink.configuration.Configuration parameters)
                throws Exception {
            recentState =
                    getRuntimeContext()
                            .getListState(new ListStateDescriptor<>("recent", Long.class));
        }

        @Override
        public void processElement(
                final Tuple2<String, Long> value, final Context ctx, final Collector<String> out)
                throws Exception {
            recentState.add(value.f1);
            final List<Long> all = new ArrayList<>();
            for (final Long r : recentState.get()) {
                all.add(r);
            }
            while (all.size() > MAX_RECENT_RECORDS) {
                all.remove(0);
            }
            recentState.update(all);
            out.collect(value.f0 + " 最近记录: " + all + " (共 " + all.size() + " 条)");
        }
    }

    /** ReducingState：实时累加求和。 */
    public static class SumFunction
            extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {
        private static final long serialVersionUID = 1L;
        private transient ReducingState<Long> sumState;

        @Override
        public void open(final org.apache.flink.configuration.Configuration parameters)
                throws Exception {
            sumState =
                    getRuntimeContext()
                            .getReducingState(
                                    new ReducingStateDescriptor<>("sum", Long::sum, Long.class));
        }

        @Override
        public void processElement(
                final Tuple2<String, Long> value, final Context ctx, final Collector<String> out)
                throws Exception {
            sumState.add(value.f1);
            out.collect(value.f0 + " 累加和: " + sumState.get() + "，当前值: " + value.f1);
        }
    }
}
