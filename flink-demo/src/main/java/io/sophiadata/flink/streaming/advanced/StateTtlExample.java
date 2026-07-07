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

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import io.sophiadata.flink.base.BaseCode;

/**
 * State TTL 示例 —— 演示状态自动过期和清理策略。
 *
 * <p>场景：用户会话跟踪，5 秒无活动则状态自动过期。演示：
 *
 * <ul>
 *   <li>StateTtlConfig 配置 TTL 时间
 *   <li>UpdateType.OnReadAndWrite 读写都刷新 TTL
 *   <li>StateVisibility.NeverReturnExpired 永不返回过期状态
 *   <li>CleanupFullSnapshot 全量快照时清理
 * </ul>
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.streaming.advanced.StateTtlExample
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>StateTtlConfig.Builder 构建 TTL 配置
 *   <li>TTL 时间设置和刷新策略
 *   <li>状态清理策略：full snapshot / incremental cleanup / incremental cleanup
 *   <li>生产环境建议：大状态必须配置 TTL 避免 OOM
 * </ul>
 */
public class StateTtlExample extends BaseCode {

    private static final long TTL_SECONDS = 5L;

    public static void main(final String[] args) throws Exception {
        new StateTtlExample().init(args, "State_TTL_Example");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        final DataStream<Tuple2<String, Long>> source =
                env.fromElements(
                        Tuple2.of("user_1", 10L),
                        Tuple2.of("user_1", 20L),
                        Tuple2.of("user_2", 30L),
                        Tuple2.of("user_1", 15L),
                        Tuple2.of("user_2", 25L));

        source.keyBy(t -> t.f0)
                .process(new SessionTracker())
                .print("StateTTL_Result")
                .setParallelism(1);
    }

    /** 会话跟踪器：使用 State TTL 自动清理不活跃的会话状态。 */
    public static class SessionTracker
            extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {

        private static final long serialVersionUID = 1L;
        private transient ValueState<Long> sessionCount;
        private transient ValueState<Long> lastActivity;

        @Override
        public void open(final Configuration parameters) throws Exception {
            final StateTtlConfig ttlConfig =
                    StateTtlConfig.newBuilder(Time.seconds(TTL_SECONDS))
                            .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                            .cleanupFullSnapshot()
                            .build();

            final ValueStateDescriptor<Long> countDesc =
                    new ValueStateDescriptor<>("sessionCount", Long.class);
            countDesc.enableTimeToLive(ttlConfig);
            sessionCount = getRuntimeContext().getState(countDesc);

            final ValueStateDescriptor<Long> activityDesc =
                    new ValueStateDescriptor<>("lastActivity", Long.class);
            activityDesc.enableTimeToLive(ttlConfig);
            lastActivity = getRuntimeContext().getState(activityDesc);
        }

        @Override
        public void processElement(
                final Tuple2<String, Long> value, final Context ctx, final Collector<String> out)
                throws Exception {
            Long count = sessionCount.value();
            if (count == null) {
                count = 0L;
            }
            count += 1;
            sessionCount.update(count);
            lastActivity.update(ctx.timerService().currentProcessingTime());

            out.collect(
                    value.f0
                            + " session_count="
                            + count
                            + ", value="
                            + value.f1
                            + " (TTL="
                            + TTL_SECONDS
                            + "s)");
        }
    }
}
