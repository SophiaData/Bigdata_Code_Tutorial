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
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
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

    @SuppressWarnings("deprecation")
    public void checkpoint(
            final StreamExecutionEnvironment env,
            final String ckPathAndJobId,
            final Boolean hashMap,
            final Boolean localpath) {
        // NOTE: setStateBackend(StateBackend) is deprecated in Flink 1.18+, replaced by
        // StreamExecutionEnvironment#configure(ConfiguredStateBackend). The replacement requires
        // also migrating the underlying state backend (HashMapStateBackend /
        // EmbeddedRocksDBStateBackend) construction. Tracked for a follow-up refactor; suppressed
        // here so the build stays warning-clean.
        if (hashMap) {
            env.setStateBackend(new HashMapStateBackend());
        } else {
            // EmbeddedRocksDBStateBackend supports Changelog-style incremental checkpoints.
            env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        }
        if (localpath) {
            // TM-local storage; defaults to the working dir of each task manager. Suitable for
            // throwaway test runs only — production should set an explicit file:// or hdfs:// path.
            env.enableCheckpointing(5 * 60 * 1000);
        } else {
            // Override via -DcheckpointStorage=file:///path or hdfs://nameservice/path
            // (env.getCheckpointConfig().setCheckpointStorage(System.getProperty("checkpointStorage", ...))).
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

    @SuppressWarnings("deprecation")
    public void restartTask(final StreamExecutionEnvironment env) {
        // fixedDelayRestart(int, java.time.Duration) replaces fixedDelayRestart(int, Time).
        // Flink 1.18+ uses java.time.Duration throughout the public API.
        //
        // NOTE: RestartStrategies + setRestartStrategy() are deprecated in Flink 1.20; the new
        // way is to construct RestartStrategy via Configuration / PipelineOptions.RESTART_STRATEGY
        // and pass it via StreamExecutionEnvironment#configure(...). Tracked for follow-up; kept
        // here because the simple two-line replacement here is far easier to audit than a full
        // migration to Configuration-driven restart.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Duration.ofSeconds(10)));
    }
}
