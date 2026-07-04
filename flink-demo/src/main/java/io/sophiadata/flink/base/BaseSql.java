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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/** (@SophiaData) (@date 2022/10/25 10:58). */
public abstract class BaseSql {
    public void init(
            final String[] args,
            final String jobName,
            final Boolean hashMap,
            final Boolean localpath,
            final String ckPath)
            throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // set sql job name
        tEnv.getConfig().getConfiguration().setString("pipeline.name", jobName);

        checkpoint(env, ckPath, hashMap, localpath);

        restartTask(env);

        handle(args, env, tEnv);
    }

    public void init(final String[] args, final String jobName) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // set sql job name
        tEnv.getConfig().getConfiguration().setString("pipeline.name", jobName);

        handle(args, env, tEnv);
    }

    public abstract void handle(
            final String[] args,
            final StreamExecutionEnvironment env,
            final StreamTableEnvironment tEnv)
            throws Exception;

    public void checkpoint(
            final StreamExecutionEnvironment env,
            final String ckPath,
            final Boolean hashMap,
            final Boolean localpath) {
        final Configuration config = new Configuration();
        if (hashMap) {
            config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        } else {
            config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        }
        env.configure(config);

        if (localpath) {
            env.enableCheckpointing(3000);
        } else {
            env.getCheckpointConfig().setCheckpointStorage(ckPath);
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

    public void restartTask(final StreamExecutionEnvironment env) {
        final Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 10);
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));
        env.configure(config);
    }
}
