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

/**
 * Version-neutral helpers for Flink APIs that Flink 2.0 changed or removed.
 *
 * <p>Flink 2.0 dropped the programmatic restart-strategy API: {@code
 * StreamExecutionEnvironment#setRestartStrategy} and {@code
 * org.apache.flink.api.common.restartstrategy.RestartStrategies} no longer exist, and the behaviour
 * is configured declaratively instead. This class exposes one method that does the right thing on
 * whichever Flink line is being compiled.
 *
 * <p>Implementations live in the {@code flink20} and {@code flink2} source sets.
 */
public final class FlinkCompat {

    private FlinkCompat() {}

    /**
     * Configures a fixed-delay restart strategy on the given environment.
     *
     * <p>On Flink 1.20 this calls {@code env.setRestartStrategy(...)}; on Flink 2.x it writes the
     * declarative {@code RestartStrategyOptions} into the environment configuration, because the
     * programmatic API was removed.
     *
     * @param env the environment to configure
     * @param attempts maximum number of restart attempts
     * @param delaySeconds delay between attempts, in seconds
     */
    public static void fixedDelayRestart(
            org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env,
            int attempts,
            long delaySeconds) {
        FlinkCompatImpl.fixedDelayRestart(env, attempts, delaySeconds);
    }

    /**
     * Selects a hash-map state backend, or RocksDB when {@code rocksDb} is true.
     *
     * <p>Flink 2.0 removed {@code StreamExecutionEnvironment#setStateBackend}; the backend is
     * chosen through configuration instead. On Flink 1.20 the original call is made.
     *
     * @param env the environment to configure
     * @param rocksDb true for the embedded RocksDB backend, false for the hash-map backend
     */
    public static void configureStateBackend(
            org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env,
            boolean rocksDb) {
        FlinkCompatImpl.configureStateBackend(env, rocksDb);
    }

    /**
     * Sets the checkpoint storage path.
     *
     * <p>Flink 2.0 removed {@code CheckpointConfig#setCheckpointStorage(String)}; on that line the
     * equivalent {@code CheckpointingOptions} entries are written to the environment configuration.
     *
     * @param env the environment to configure
     * @param checkpointStorage the checkpoint storage path
     */
    public static void setCheckpointStorage(
            org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env,
            String checkpointStorage) {
        FlinkCompatImpl.setCheckpointStorage(env, checkpointStorage);
    }

    /**
     * Retains checkpoints when the job is cancelled.
     *
     * <p>Flink 2.0 removed {@code CheckpointConfig#setExternalizedCheckpointCleanup} together with
     * the {@code ExternalizedCheckpointCleanup} enum, replacing it with the {@code
     * EXTERNALIZED_CHECKPOINT} option.
     *
     * @param env the environment to configure
     */
    public static void retainCheckpointsOnCancellation(
            org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env) {
        FlinkCompatImpl.retainCheckpointsOnCancellation(env);
    }
}
