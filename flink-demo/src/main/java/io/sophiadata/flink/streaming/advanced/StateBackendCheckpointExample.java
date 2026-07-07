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
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import io.sophiadata.flink.base.BaseCode;

/**
 * State Backend + Checkpoint 示例 —— 演示 RocksDB 状态后端和 Checkpoint 配置。
 *
 * <p>场景：带状态的计数器，演示如何配置 RocksDB 状态后端、Checkpoint 间隔、故障恢复策略。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.streaming.advanced.StateBackendCheckpointExample
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>RocksDB 状态后端配置（适合大状态）
 *   <li>Checkpoint 间隔和超时配置
 *   <li>EXACTLY_ONCE 语义保证
 *   <li>ExternalizedCheckpointRetention 作业取消后保留 Checkpoint
 *   <li>RestartStrategy 固定延迟重启
 * </ul>
 */
public class StateBackendCheckpointExample extends BaseCode {

    public static void main(final String[] args) throws Exception {
        new StateBackendCheckpointExample().init(args, "StateBackend_Checkpoint_Example");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        // 配置 Checkpoint
        configureCheckpoint(env);

        // 模拟数据流
        final DataStream<Tuple2<String, Long>> source =
                env.fromElements(
                        Tuple2.of("sensor_1", 10L),
                        Tuple2.of("sensor_1", 20L),
                        Tuple2.of("sensor_2", 30L),
                        Tuple2.of("sensor_1", 15L),
                        Tuple2.of("sensor_2", 25L),
                        Tuple2.of("sensor_1", 5L),
                        Tuple2.of("sensor_2", 40L));

        source.keyBy(t -> t.f0)
                .process(new StatefulCounter())
                .print("StateBackend_Result")
                .setParallelism(1);
    }

    private void configureCheckpoint(final StreamExecutionEnvironment env) {
        // Checkpoint 间隔：每 10 秒做一次 Checkpoint
        env.enableCheckpointing(10_000);

        final CheckpointConfig config = env.getCheckpointConfig();

        // EXACTLY_ONCE 语义
        config.setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);

        // Checkpoint 超时时间：3 分钟
        config.setCheckpointTimeout(3 * 60 * 1000);

        // 两次 Checkpoint 之间最小间隔：500ms
        config.setMinPauseBetweenCheckpoints(500);

        // 同时进行的最大 Checkpoint 数量
        config.setMaxConcurrentCheckpoints(1);

        // 可容忍的 Checkpoint 失败次数
        config.setTolerableCheckpointFailureNumber(3);

        // 作业取消后保留 Checkpoint（用于故障恢复）
        config.setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    }

    /** 带状态的计数器，使用 ValueState 保存计数。 */
    public static class StatefulCounter
            extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {

        private static final long serialVersionUID = 1L;
        private transient ValueState<Long> countState;
        private transient ValueState<Long> sumState;

        @Override
        public void open(final Configuration parameters) throws Exception {
            countState =
                    getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
            sumState = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Long.class));
        }

        @Override
        public void processElement(
                final Tuple2<String, Long> value, final Context ctx, final Collector<String> out)
                throws Exception {
            Long count = countState.value();
            Long sum = sumState.value();

            if (count == null) {
                count = 0L;
            }
            if (sum == null) {
                sum = 0L;
            }

            count += 1;
            sum += value.f1;

            countState.update(count);
            sumState.update(sum);

            out.collect(
                    value.f0 + " count=" + count + ", sum=" + sum + ", avg=" + (sum * 1.0 / count));
        }
    }
}
