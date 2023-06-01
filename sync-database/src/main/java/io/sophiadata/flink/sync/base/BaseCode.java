/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sophiadata.flink.sync.base;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** (@gtk) (@date 2023/5/31 18:56). */
public abstract class BaseCode {
    public void init(String[] args, String ckPathAndJobId, Boolean hashMap, Boolean localpath)
            throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // set sql job name
        tEnv.getConfig().getConfiguration().setString("pipeline.name", ckPathAndJobId);

        checkpoint(env, ckPathAndJobId, hashMap, localpath);

        restartTask(env);

        handle(args, env, tEnv);
    }

    public void init(String[] args, String ckPathAndJobId) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // set sql job name
        tEnv.getConfig().getConfiguration().setString("pipeline.name", ckPathAndJobId);

        handle(args, env, tEnv);
    }

    public abstract void handle(
            String[] args, StreamExecutionEnvironment env, StreamTableEnvironment tEnv)
            throws Exception;

    public void checkpoint(
            StreamExecutionEnvironment env,
            String ckPathAndJobId,
            Boolean hashMap,
            Boolean localpath) {
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
            //    .setCheckpointStorage("file:///user/flink/" + ckPathAndJobId);
        } else {
            env.getCheckpointConfig()
                    .setCheckpointStorage("hdfs://hadoop1:8020/flink/" + ckPathAndJobId);
            // Hadoop HA 写法：
            // hdfs://nameService_id/path/file
            env.enableCheckpointing(60 * 1000);
        }
        // Changelog 是一项旨在减少检查点时间的功能，因此可以减少一次模式下的端到端延迟。
        // 在 EmbeddedRocksDBStateBackend 中受到支持
        env.enableChangelogStateBackend(false); // 启用 Changelog 可能会对应用程序的性能产生负面影响。
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(3 * 60 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    public void restartTask(StreamExecutionEnvironment env) {
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(10)));
    }
}
