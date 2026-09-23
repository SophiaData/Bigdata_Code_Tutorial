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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 1.20 implementation of {@link FlinkCompat}.
 *
 * <p><strong>This is a {@code flink20} source-set file.</strong> Every API this class needs still
 * exists on Flink 1.20, so the original calls are used verbatim. Each corresponding method in the
 * {@code flink2} source set reimplements the behaviour against Flink 2.x, where these APIs were
 * removed.
 */
final class FlinkCompatImpl {

    private FlinkCompatImpl() {}

    static void fixedDelayRestart(StreamExecutionEnvironment env, int attempts, long delaySeconds) {
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(attempts, Time.seconds(delaySeconds)));
    }

    static void configureStateBackend(StreamExecutionEnvironment env, boolean rocksDb) {
        env.setStateBackend(
                rocksDb ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend());
    }

    static void setCheckpointStorage(StreamExecutionEnvironment env, String checkpointStorage) {
        env.getCheckpointConfig().setCheckpointStorage(checkpointStorage);
    }

    static void retainCheckpointsOnCancellation(StreamExecutionEnvironment env) {
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }
}
