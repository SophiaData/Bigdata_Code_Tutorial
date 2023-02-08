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

package io.sophiadata.flink.base;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** (@SophiaData) (@date 2022/10/27 13:26). */
public abstract class BaseCode {
    private static final Logger LOG = LoggerFactory.getLogger(BaseCode.class);

    public void init(String[] args, String ckPathAndJobId, Boolean hashMap, Boolean localpath)
            throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        checkpoint(env, ckPathAndJobId, hashMap, localpath);

        restartTask(env);

        handle(env, params);
        try {
            env.execute(ckPathAndJobId); // 传入一个job的名字
        } catch (Exception e) {
            LOG.error(ckPathAndJobId + " 程序异常信息输出：", e);
        }
    }

    public void init(String[] args, String ckPathAndJobId) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        handle(env, params);

        try {
            env.execute(ckPathAndJobId); // 传入一个job的名字
        } catch (Exception e) {
            LOG.error(ckPathAndJobId + " 异常信息输出：", e);
        }
    }

    public abstract void handle(StreamExecutionEnvironment env, ParameterTool params)
            throws Exception;

    public void checkpoint(
            StreamExecutionEnvironment env,
            String ckPathAndJobId,
            Boolean hashMap,
            Boolean localpath) {
        if (hashMap) {
            env.setStateBackend(new HashMapStateBackend());
        } else {
            // 该类型 State Backend 支持 changelog 增量检查点
            env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        }
        if (localpath) {
            env.enableCheckpointing(3000);
        } else {
            env.getCheckpointConfig()
                    .setCheckpointStorage("hdfs://hadoop1:8020/flink/" + ckPathAndJobId);
            env.enableCheckpointing(60 * 1000);
        }
        // Changelog 是一项旨在减少检查点时间的功能，因此可以减少一次模式下的端到端延迟。
        // 在 EmbeddedRocksDBStateBackend 中受到支持
        env.enableChangelogStateBackend(false); // 启用Changelog可能会对应用程序的性能产生负面影响。
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(3 * 60 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    public void restartTask(StreamExecutionEnvironment env) {
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(10, Time.minutes(5), Time.seconds(10)));
    }
}
