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

package io.sophiadata.flink.sync.executor;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import com.alibaba.fastjson.JSONObject;
import io.sophiadata.flink.sync.Constant;
import io.sophiadata.flink.sync.operator.MyBroadcastProcessFunction;
import io.sophiadata.flink.sync.operator.MyFilterFunction;
import io.sophiadata.flink.sync.operator.MyKeyedProcessFunction;
import io.sophiadata.flink.sync.operator.MyRichMapFunction;
import io.sophiadata.flink.sync.sink.MyOracleSink;
import io.sophiadata.flink.sync.source.MyKafkaSource;
import io.sophiadata.flink.sync.source.MyRedisSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

/** (@SophiaData) (@date 2023/6/5 10:19). */
public class KafkaToOracle {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaToOracle.class);

    public void kafkaToOracle(StreamExecutionEnvironment env, ParameterTool params) {

        SingleOutputStreamOperator<String> kafkaSourceStream =
                new MyKafkaSource()
                        .singleOutputStreamOperator(params, env)
                        .uid("KafkaToOracle-kafkaSourceStream")
                        .name("KafkaToOracle-kafkaSourceStream");

        LOG.info(params.get("jobName") + " -- kafka source normal ");
        SingleOutputStreamOperator<String> filterDS =
                kafkaSourceStream
                        .filter(new MyFilterFunction())
                        .uid("KafkaToOracle-filter")
                        .name("KafkaToOracle-filter");
        SingleOutputStreamOperator<JSONObject> mapDS =
                filterDS.map(new MyRichMapFunction(params))
                        .uid("KafkaToOracle-map")
                        .name("KafkaToOracle-map");
                new OutputTag<JSONObject>("receType3OutputTag") {};

        SingleOutputStreamOperator<Map<String, String>> redisSource =
                env.addSource(new MyRedisSource(params))
                        .uid("KafkaToOracle-redisSource")
                        .name("KafkaToOracle-redisSource");

        MapStateDescriptor<String, String> mapStateDescriptor =
                new MapStateDescriptor<>("map-state", String.class, String.class);

        BroadcastStream<Map<String, String>> broadcastStream =
                redisSource.broadcast(mapStateDescriptor);
        // 合并redis输入流
        BroadcastConnectedStream<JSONObject, Map<String, String>> connect =
                mapDS.connect(broadcastStream);
        SingleOutputStreamOperator<JSONObject> processStream =
                connect.process(new MyBroadcastProcessFunction(params, mapStateDescriptor))
                        .uid("KafkaToOracle-Broadcast")
                        .name("KafkaToOracle-Broadcast");

        KeyedStream<JSONObject, String> StringKeyedStream =
                processStream.keyBy(
                        data ->
                                data.getString(Constant.TASK_ID)
                                        + "-"
                                        + data.getString(Constant.TABLE_CODE));

        SingleOutputStreamOperator<ArrayList<JSONObject>> operator =
                StringKeyedStream.process(new MyKeyedProcessFunction(params))
                        .uid("KafkaToOracle-keyProcess")
                        .name("KafkaToOracle-keyProcess")
                        .disableChaining();
        operator.addSink(new MyOracleSink(params))
                .uid("KafkaToOracle-sink")
                .name("KafkaToOracle-sink");

        try {
            env.execute(params.get("jobName"));
        } catch (Exception e) {
            LOG.error(" flink job exception {} ", e.getMessage());
        }
    }
}
