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

package io.sophiadata.flink.base;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/** (@SophiaData) (@date 2022/10/27 13:26). */
public abstract class BaseCode {
    private static final Logger LOG = LoggerFactory.getLogger(BaseCode.class);

    public void init(
            String[] args, String jobName, Boolean hashMap, Boolean localpath, String ckPath)
            throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        checkpoint(env, ckPath, hashMap, localpath);

        restartTask(env);

        handle(args, env);
        env.execute(jobName); // 传入一个job的名字
    }

    public void init(String[] args, String jobName) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Force STREAMING mode so bounded sources (fromCollection / fromElements) don't trigger
        // Flink 1.18+ auto-batch, which silently finishes the job before our explicit
        // env.execute() and then fails with "No operators defined in streaming topology".
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        handle(args, env);
        env.execute(jobName); // 传入一个job的名字
    }

    public abstract void handle(String[] args, StreamExecutionEnvironment env) throws Exception;

    @SuppressWarnings("deprecation")
    public void checkpoint(
            StreamExecutionEnvironment env, String ckPath, Boolean hashMap, Boolean localpath) {
        // NOTE: setStateBackend(StateBackend) is deprecated in Flink 1.18+, replaced by
        // StreamExecutionEnvironment#configure(ConfiguredStateBackend). The replacement requires
        // also migrating the underlying state backend (HashMapStateBackend /
        // EmbeddedRocksDBStateBackend) construction. Tracked for a follow-up refactor; suppressed
        // here so the build stays warning-clean.
        if (hashMap) {
            env.setStateBackend(new HashMapStateBackend());
        } else {
            // 该类型 State Backend 支持 Changelog 增量检查点
            env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        }
        if (localpath) {
            env.enableCheckpointing(3000);
            // 注意这里默认把状态存储在内存中，如内存打满将导致 checkpoint 失败
            // 测试任务如数据量较大请指定文件存储
            // env.getCheckpointConfig()
            //    .setCheckpointStorage(ckPath);
        } else {
            env.getCheckpointConfig().setCheckpointStorage(ckPath);
            // Hadoop HA 写法：
            // hdfs://nameService_id/path/file
            env.enableCheckpointing(60 * 1000);
        }
        env.getCheckpointConfig().setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(3 * 60 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        env.getCheckpointConfig()
                .setExternalizedCheckpointRetention(
                        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    }

    @SuppressWarnings("deprecation")
    public void restartTask(StreamExecutionEnvironment env) {
        // RestartStrategies + setRestartStrategy() are deprecated in Flink 1.20; the new way is
        // Configuration-driven restart via PipelineOptions.RESTART_STRATEGY. Kept as-is for now.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Duration.ofSeconds(10)));
    }
}
