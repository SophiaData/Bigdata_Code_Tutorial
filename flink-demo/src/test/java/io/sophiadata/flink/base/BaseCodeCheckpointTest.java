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

import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for the checkpoint and restart configuration shared by every demo job.
 *
 * <p>{@link BaseCode} and {@link BaseSql} are the base classes all examples extend, so a mistake
 * here changes the reliability settings of every job in the module at once. The configuration is
 * built on a plain {@code StreamExecutionEnvironment}, so it can be asserted without a cluster.
 */
class BaseCodeCheckpointTest {

    /** Concrete subclass: BaseCode is abstract and implementers only supply handle(). */
    private static final class TestCode extends BaseCode {
        @Override
        public void handle(final String[] args, final StreamExecutionEnvironment env) {
            // No operators: these tests only exercise configuration.
        }
    }

    @Test
    void checkpointUsesExactlyOnceSemantics() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TestCode code = new TestCode();

        code.checkpoint(env, "file:///tmp/ck", true, true);

        CheckpointConfig config = env.getCheckpointConfig();
        assertEquals(
                CheckpointingMode.EXACTLY_ONCE,
                config.getCheckpointingConsistencyMode(),
                "the default must remain exactly-once");
    }

    @Test
    void checkpointKeepsTheExpectedTimeoutAndConcurrencyLimits() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        new TestCode().checkpoint(env, "file:///tmp/ck", true, true);

        CheckpointConfig config = env.getCheckpointConfig();
        assertEquals(3 * 60 * 1000L, config.getCheckpointTimeout(), "checkpoint timeout");
        assertEquals(2, config.getMaxConcurrentCheckpoints(), "max concurrent checkpoints");
        assertEquals(500L, config.getMinPauseBetweenCheckpoints(), "pause between checkpoints");
        assertEquals(
                10, config.getTolerableCheckpointFailureNumber(), "tolerable consecutive failures");
    }

    @Test
    void checkpointIntervalDependsOnTheLocalPathFlag() {
        // localpath=true keeps everything on the JobManager with a short interval; false writes to
        // the supplied storage path on a longer interval.
        StreamExecutionEnvironment local = StreamExecutionEnvironment.getExecutionEnvironment();
        new TestCode().checkpoint(local, "file:///tmp/ck", true, true);
        assertEquals(3000L, local.getCheckpointConfig().getCheckpointInterval());

        StreamExecutionEnvironment remote = StreamExecutionEnvironment.getExecutionEnvironment();
        new TestCode().checkpoint(remote, "file:///tmp/ck", true, false);
        assertEquals(60 * 1000L, remote.getCheckpointConfig().getCheckpointInterval());
    }

    @Test
    void checkpointSelectsTheStateBackendFromTheFlag() {
        StreamExecutionEnvironment hashMap = StreamExecutionEnvironment.getExecutionEnvironment();
        new TestCode().checkpoint(hashMap, "file:///tmp/ck", true, true);
        assertEquals(
                "hashmap",
                hashMap.getConfiguration().get(StateBackendOptions.STATE_BACKEND),
                "hashMap=true should select the hashmap backend");

        StreamExecutionEnvironment rocks = StreamExecutionEnvironment.getExecutionEnvironment();
        new TestCode().checkpoint(rocks, "file:///tmp/ck", false, true);
        assertEquals(
                "rocksdb",
                rocks.getConfiguration().get(StateBackendOptions.STATE_BACKEND),
                "hashMap=false should select rocksdb");
    }

    @Test
    void checkpointRetainsExternalizedStateOnCancellation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        new TestCode().checkpoint(env, "file:///tmp/ck", true, true);

        assertEquals(
                org.apache.flink.configuration.ExternalizedCheckpointRetention
                        .RETAIN_ON_CANCELLATION,
                env.getCheckpointConfig().getExternalizedCheckpointRetention(),
                "state should survive cancellation so a job can be resumed");
    }

    @Test
    void restartTaskAppliesFixedDelayStrategy() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        new TestCode().restartTask(env);

        assertEquals(
                "fixed-delay",
                env.getConfiguration().get(RestartStrategyOptions.RESTART_STRATEGY),
                "the restart strategy should be fixed-delay");
        assertEquals(
                10,
                env.getConfiguration()
                        .get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS),
                "retry attempts");
        assertEquals(
                java.time.Duration.ofSeconds(10),
                env.getConfiguration()
                        .get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY),
                "delay between attempts");
    }

    @Test
    void baseSqlAppliesTheSameCheckpointSettings() {
        // BaseSql duplicates this logic rather than inheriting it, so it needs its own assertion:
        // the two copies can otherwise drift apart.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        BaseSql sql =
                new BaseSql() {
                    @Override
                    public void handle(
                            final String[] args,
                            final StreamExecutionEnvironment env,
                            final org.apache.flink.table.api.bridge.java.StreamTableEnvironment
                                    tEnv) {
                        // No operators: these tests only exercise configuration.
                    }
                };

        sql.checkpoint(env, "file:///tmp/ck", false, true);

        CheckpointConfig config = env.getCheckpointConfig();
        assertEquals(CheckpointingMode.EXACTLY_ONCE, config.getCheckpointingConsistencyMode());
        assertEquals(3000L, config.getCheckpointInterval());
        assertEquals(3 * 60 * 1000L, config.getCheckpointTimeout());
        assertEquals(2, config.getMaxConcurrentCheckpoints());
        assertEquals("rocksdb", env.getConfiguration().get(StateBackendOptions.STATE_BACKEND));
    }

    @Test
    void baseSqlAppliesTheSameRestartSettings() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        BaseSql sql =
                new BaseSql() {
                    @Override
                    public void handle(
                            final String[] args,
                            final StreamExecutionEnvironment env,
                            final org.apache.flink.table.api.bridge.java.StreamTableEnvironment
                                    tEnv) {
                        // No operators: these tests only exercise configuration.
                    }
                };

        sql.restartTask(env);

        assertEquals(
                "fixed-delay", env.getConfiguration().get(RestartStrategyOptions.RESTART_STRATEGY));
        assertEquals(
                10,
                env.getConfiguration()
                        .get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS));
    }
}
