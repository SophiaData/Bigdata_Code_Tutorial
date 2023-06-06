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

package io.sophiadata.flink.sync.base;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

/** (@SophiaData) (@date 2023/6/5 10:17). */
public abstract class BaseCode {

    public void init(String[] args, String JobName, Boolean hashMap, String ckPath)
            throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // set sql job name
        tEnv.getConfig().getConfiguration().setString("pipeline.name", JobName);

        checkpoint(env, hashMap, ckPath);

        restartTask(env);

        handle(args, env, tEnv);
    }

    public void init(String[] args, String jobName) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings fsSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);
        // set sql job name
        tEnv.getConfig().getConfiguration().setString("pipeline.name", jobName);

        restartTask(env);

        handle(args, env, tEnv);
    }

    public abstract void handle(
            String[] args, StreamExecutionEnvironment env, StreamTableEnvironment tEnv)
            throws Exception;

    public void checkpoint(StreamExecutionEnvironment env, Boolean hashMap, String ckPath)
            throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");

        if (hashMap) {
            StateBackend fsStateBackend = new FsStateBackend(ckPath, true);
            env.setStateBackend(fsStateBackend);
        } else {
            StateBackend rocksDBStateBackend = new RocksDBStateBackend(ckPath, true);
            env.setStateBackend(rocksDBStateBackend);
        }
        env.enableCheckpointing(60 * 1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(5 * 60 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(50);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    public void restartTask(StreamExecutionEnvironment env) {
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(0, Time.seconds(10)));
    }
}
