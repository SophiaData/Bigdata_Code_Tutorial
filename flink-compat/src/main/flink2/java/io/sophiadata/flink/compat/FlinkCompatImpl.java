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

package io.sophiadata.flink.compat;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * Flink 2.x implementation of {@link FlinkCompat}.
 *
 * <p><strong>This is a {@code flink2} source-set file.</strong>
 *
 * <p>Flink 2.0 removed several programmatic configuration APIs in favour of declarative {@code
 * Configuration} options. Each method below writes the equivalent option(s):
 *
 * <ul>
 *   <li>restart strategy — {@link RestartStrategyOptions} ({@code setRestartStrategy} removed)
 *   <li>state backend — {@link StateBackendOptions#STATE_BACKEND} ({@code setStateBackend} removed)
 *   <li>checkpoint storage — {@link CheckpointingOptions#CHECKPOINT_STORAGE} ({@code
 *       CheckpointConfig#setCheckpointStorage} removed)
 *   <li>externalized checkpoints — {@link CheckpointingOptions#EXTERNALIZED_CHECKPOINT_RETENTION}
 *       ({@code setExternalizedCheckpointCleanup} removed)
 * </ul>
 */
final class FlinkCompatImpl {

    private FlinkCompatImpl() {}

    static void fixedDelayRestart(StreamExecutionEnvironment env, int attempts, long delaySeconds) {
        Configuration configuration = configuration(env);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, attempts);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                Duration.ofSeconds(delaySeconds));
    }

    static void configureStateBackend(StreamExecutionEnvironment env, boolean rocksDb) {
        // RocksDB is the durable choice; HashMapStateBackend is the in-memory default.
        configuration(env)
                .set(
                        StateBackendOptions.STATE_BACKEND,
                        rocksDb
                                ? "org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend"
                                : "org.apache.flink.runtime.state.hashmap.HashMapStateBackend");
    }

    static void setCheckpointStorage(StreamExecutionEnvironment env, String checkpointStorage) {
        configuration(env).set(CheckpointingOptions.CHECKPOINT_STORAGE, checkpointStorage);
    }

    static void retainCheckpointsOnCancellation(StreamExecutionEnvironment env) {
        configuration(env)
                .set(
                        CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    }

    /**
     * Returns the environment's mutable configuration.
     *
     * <p>{@code getConfiguration()} is typed as {@code ReadableConfig} on this line, but the instance
     * the environment holds is a {@code Configuration}.
     */
    private static Configuration configuration(StreamExecutionEnvironment env) {
        return (Configuration) env.getConfiguration();
    }
}
