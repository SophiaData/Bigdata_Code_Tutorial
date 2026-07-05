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

package io.sophiadata.flink.streaming.advanced;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 集成测试：验证 ProcessFunctionStateExample 的三种状态处理逻辑能正确执行。
 *
 * <p>使用 MiniCluster 实际运行 Flink 管道，验证状态读写逻辑。
 */
public class ProcessFunctionStateExampleIT {

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    /** 使用静态集合收集结果（SinkFunction 被序列化后仍可访问）。 */
    public static final List<String> COLLECTED = new ArrayList<>();

    @SuppressWarnings("deprecation")
    @Test
    public void testCountFunctionTracksOccurrences() throws Exception {
        COLLECTED.clear();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(
                        Tuple2.of("a", 1L),
                        Tuple2.of("a", 2L),
                        Tuple2.of("b", 3L),
                        Tuple2.of("a", 4L))
                .keyBy(value -> value.f0)
                .process(new ProcessFunctionStateExample.CountFunction())
                .addSink(new StaticCollectSink());

        env.execute("Test_CountFunction");

        assertThat(COLLECTED).hasSize(4);
        assertThat(COLLECTED.get(0)).contains("a 出现了 1 次");
        assertThat(COLLECTED.get(1)).contains("a 出现了 2 次");
        assertThat(COLLECTED.get(2)).contains("b 出现了 1 次");
        assertThat(COLLECTED.get(3)).contains("a 出现了 3 次");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testRecentFunctionCollectsRecentRecords() throws Exception {
        COLLECTED.clear();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(
                        Tuple2.of("x", 10L),
                        Tuple2.of("x", 20L),
                        Tuple2.of("x", 30L),
                        Tuple2.of("x", 40L),
                        Tuple2.of("x", 50L),
                        Tuple2.of("x", 60L))
                .keyBy(value -> value.f0)
                .process(new ProcessFunctionStateExample.RecentFunction())
                .addSink(new StaticCollectSink());

        env.execute("Test_RecentFunction");

        assertThat(COLLECTED).hasSize(6);
        final String lastResult = COLLECTED.get(5);
        assertThat(lastResult).contains("[20, 30, 40, 50, 60]");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSumFunctionAccumulatesValues() throws Exception {
        COLLECTED.clear();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(Tuple2.of("s", 10L), Tuple2.of("s", 20L), Tuple2.of("s", 30L))
                .keyBy(value -> value.f0)
                .process(new ProcessFunctionStateExample.SumFunction())
                .addSink(new StaticCollectSink());

        env.execute("Test_SumFunction");

        assertThat(COLLECTED).hasSize(3);
        assertThat(COLLECTED.get(0)).contains("累加和: 10");
        assertThat(COLLECTED.get(1)).contains("累加和: 30");
        assertThat(COLLECTED.get(2)).contains("累加和: 60");
    }

    /** 写入静态集合的 Sink（避免序列化问题）。 */
    @SuppressWarnings("deprecation")
    private static class StaticCollectSink
            implements org.apache.flink.streaming.api.functions.sink.SinkFunction<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void invoke(final String value, final Context context) {
            synchronized (COLLECTED) {
                COLLECTED.add(value);
            }
        }
    }
}
