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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 2.x implementation of {@link FlinkCompat}.
 *
 * <p><strong>This is a {@code flink2} source-set file.</strong> Flink 2.0 removed {@code
 * CheckpointConfig#setCheckpointStorage(String)}; the equivalent is the declarative {@code
 * CheckpointingOptions.CHECKPOINT_STORAGE} option, written into the environment configuration.
 */
final class FlinkCompatImpl {

    private FlinkCompatImpl() {}

    static void setCheckpointStorage(
            final StreamExecutionEnvironment env, final String checkpointStorage) {
        configuration(env).set(CheckpointingOptions.CHECKPOINT_STORAGE, checkpointStorage);
    }

    /**
     * Returns the environment's mutable configuration.
     *
     * <p>{@code getConfiguration()} is typed as {@code ReadableConfig} on this line, but the instance
     * the environment holds is a {@code Configuration}.
     *
     * @param env the environment
     * @return its configuration
     */
    private static Configuration configuration(final StreamExecutionEnvironment env) {
        return (Configuration) env.getConfiguration();
    }
}