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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * Template base class for Flink streaming jobs.
 *
 * <p><b>Two-phase bootstrap:</b> {@code init(...)} creates the {@link StreamExecutionEnvironment}
 * and optional checkpoint/restart config, then delegates to the abstract {@link #handle} method
 * which contains the actual pipeline logic.
 *
 * <p><b>Execute semantics:</b> This base class does <i>not</i> call {@code env.execute()}.
 * Subclasses are responsible for calling {@code statementSet.execute()} (or {@code env.execute()})
 * at the appropriate point in {@link #handle}.
 *
 * <p><b>Overloads:</b>
 *
 * <ul>
 *   <li>{@link #init(String[], String, Boolean, Boolean)} — full bootstrap with checkpoint and
 *       restart strategy
 *   <li>{@link #init(String[], String)} — minimal bootstrap without checkpoint/restart
 * </ul>
 */
public abstract class BaseCode {
    /**
     * Bootstrap a Flink SQL/Table job. <b>Does not call {@code env.execute()}</b> — the concrete
     * {@link #handle} implementation is expected to call {@code statementSet.execute()} (or
     * similar) so the job name and any post-DDL pipeline steps (e.g. JDBC sink auto-table-create)
     * run inside the same program.
     */
    public void init(
            final String[] args,
            final String ckPathAndJobId,
            final Boolean hashMap,
            final Boolean localpath)
            throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().getConfiguration().setString("pipeline.name", ckPathAndJobId);

        checkpoint(env, ckPathAndJobId, hashMap, localpath);

        restartTask(env);

        handle(args, env, tEnv);
    }

    /** Minimal entry point without checkpoint or restart configuration. */
    public void init(final String[] args, final String ckPathAndJobId) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().getConfiguration().setString("pipeline.name", ckPathAndJobId);

        handle(args, env, tEnv);
    }

    public abstract void handle(
            String[] args, StreamExecutionEnvironment env, StreamTableEnvironment tEnv)
            throws Exception;

    public void checkpoint(
            final StreamExecutionEnvironment env,
            final String ckPathAndJobId,
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
            // TM-local storage; defaults to the working dir of each task manager.
            // Suitable for throwaway test runs only — production should set an explicit
            // file:// or hdfs:// path.
            env.enableCheckpointing(5 * 60 * 1000);
        } else {
            // Override via -DcheckpointStorage=file:///path or hdfs://nameservice/path
            env.getCheckpointConfig()
                    .setCheckpointStorage(new Path("hdfs://hadoop1:8020/flink/" + ckPathAndJobId));
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
