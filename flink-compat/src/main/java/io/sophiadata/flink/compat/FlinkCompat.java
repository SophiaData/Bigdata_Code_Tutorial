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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Version-neutral helpers for Flink APIs that Flink 2.0 removed.
 *
 * <p>Flink 2.0 replaced several programmatic configuration methods with declarative {@code
 * Configuration} options. This class exposes one method per removed API, implemented by the
 * version-specific source set:
 *
 * <ul>
 *   <li>{@code src/main/flink20/java} — calls the original methods
 *   <li>{@code src/main/flink2/java} — writes the equivalent {@code Configuration} options
 * </ul>
 */
public final class FlinkCompat {

    private FlinkCompat() {}

    /**
     * Configures where checkpoints are stored.
     *
     * <p>Flink 1.20 exposes {@code CheckpointConfig#setCheckpointStorage}; Flink 2.0 removed it in
     * favour of {@code CheckpointingOptions#CHECKPOINT_STORAGE}.
     *
     * @param env the environment to configure
     * @param checkpointStorage a {@code file://} or {@code hdfs://} checkpoint path
     */
    public static void setCheckpointStorage(
            final StreamExecutionEnvironment env, final String checkpointStorage) {
        FlinkCompatImpl.setCheckpointStorage(env, checkpointStorage);
    }
}
