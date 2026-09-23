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

package io.sophiadata.flink.streming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies {@link IncrementMapFunction} adds one to each element.
 *
 * <p>The original version accumulated results into a static {@code List} that was never reset
 * between tests, making the assertion history-dependent: a second run in the same JVM failed
 * because the sink kept appending. The collector is still static (Flink serializes operator
 * instances, so an instance field would not be the object that receives records), but it is now
 * cleared in {@code @BeforeEach} and exposed through a freshly-reset view.
 */
class IncrementMapFunctionTest {

    @BeforeEach
    void resetCollector() {
        CollectingSink.VALUES.clear();
    }

    @Test
    void testIncrementMapFunction() throws Exception {
        List<Long> testData = new ArrayList<>();
        testData.add(1L);
        testData.add(2L);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Long> testDataStream = env.fromCollection(testData);

        SingleOutputStreamOperator<Long> map =
                testDataStream.map(
                        new MapFunction<Long, Long>() {
                            @Override
                            public Long map(Long value) {
                                return value + 1L;
                            }
                        });

        map.addSink(new CollectingSink()).setParallelism(1);

        env.execute("IncrementMapFunctionTest");

        List<Long> expected = Arrays.asList(2L, 3L);
        List<Long> actual = new ArrayList<>(CollectingSink.VALUES);
        Collections.sort(actual);
        assertEquals(expected, actual);
    }

    /**
     * Sink that records emitted values.
     *
     * <p>{@code VALUES} is static because Flink serializes the operator before running it, so an
     * instance field written by {@code invoke} would belong to the deserialized copy rather than to
     * the object the test holds. The test resets it per method.
     */
    static final class CollectingSink implements SinkFunction<Long> {

        private static final long serialVersionUID = 1L;

        static final List<Long> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Long value, Context context) {
            VALUES.add(value);
        }
    }
}
