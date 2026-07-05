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

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import io.sophiadata.flink.base.BaseCode;

import java.util.Map;

/**
 * Broadcast State 示例 —— 动态规则引擎。
 *
 * <p>演示通过广播流动态更新处理规则，无需重启作业。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.streaming.advanced.BroadcastStateExample
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>MapStateDescriptor 定义广播状态
 *   <li>BroadcastStream 和 connect 模式
 *   <li>processBroadcastElement vs processElement 的执行语义
 * </ul>
 */
public class BroadcastStateExample extends BaseCode {

    static final MapStateDescriptor<String, String> RULE_DESCRIPTOR =
            new MapStateDescriptor<>("rules", Types.STRING, Types.STRING);

    public static void main(final String[] args) throws Exception {
        new BroadcastStateExample().init(args, "Broadcast_State_Example");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        // 1. 规则流：动态更新的处理规则 (ruleId, pattern)
        final DataStream<Tuple2<String, String>> ruleStream =
                env.fromElements(
                        Tuple2.of("rule_1", "error"),
                        Tuple2.of("rule_2", "warning"),
                        Tuple2.of("rule_1", "critical"));

        // 2. 数据流：需要处理的数据 (userId, message)
        final DataStream<Tuple2<String, String>> dataStream =
                env.fromElements(
                        Tuple2.of("user_1", "error: connection failed"),
                        Tuple2.of("user_2", "info: login success"),
                        Tuple2.of("user_3", "warning: disk low"),
                        Tuple2.of("user_1", "critical: system crash"));

        // 3. 广播规则流
        final BroadcastStream<Tuple2<String, String>> broadcastRules =
                ruleStream.broadcast(RULE_DESCRIPTOR);

        // 4. 连接数据流和广播流，应用规则
        final DataStream<String> result =
                dataStream
                        .keyBy(t -> t.f0)
                        .connect(broadcastRules)
                        .process(new RuleMatchFunction());

        // 5. 输出结果
        result.print("Rule_Match").setParallelism(1);
    }

    /** 规则匹配函数：从广播状态读取规则，匹配数据流。 */
    public static class RuleMatchFunction
            extends KeyedBroadcastProcessFunction<
                    String, Tuple2<String, String>, Tuple2<String, String>, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(
                final Tuple2<String, String> value,
                final ReadOnlyContext ctx,
                final Collector<String> out)
                throws Exception {
            final org.apache.flink.api.common.state.ReadOnlyBroadcastState<String, String>
                    ruleState = ctx.getBroadcastState(RULE_DESCRIPTOR);

            for (final Map.Entry<String, String> entry : ruleState.immutableEntries()) {
                if (value.f1.toLowerCase().contains(entry.getValue())) {
                    out.collect(
                            "User "
                                    + value.f0
                                    + " matched rule "
                                    + entry.getKey()
                                    + " ("
                                    + entry.getValue()
                                    + "): "
                                    + value.f1);
                }
            }
        }

        @Override
        public void processBroadcastElement(
                final Tuple2<String, String> value, final Context ctx, final Collector<String> out)
                throws Exception {
            final BroadcastState<String, String> ruleState = ctx.getBroadcastState(RULE_DESCRIPTOR);
            ruleState.put(value.f0, value.f1);
            out.collect("Rule updated: " + value.f0 + " -> " + value.f1);
        }
    }
}
