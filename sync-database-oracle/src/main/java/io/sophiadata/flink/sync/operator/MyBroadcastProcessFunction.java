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

package io.sophiadata.flink.sync.operator;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSONObject;
import io.sophiadata.flink.sync.Constant;
import io.sophiadata.flink.sync.utils.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** (@SophiaData) (@date 2023/6/5 10:24). */
public class MyBroadcastProcessFunction
        extends BroadcastProcessFunction<JSONObject, Map<String, String>, JSONObject> {

    private static final Logger LOG = LoggerFactory.getLogger(MyBroadcastProcessFunction.class);
    private Map<String, String> tableInfoMap;

    private Map<String, String> tableInfoMap1;

    private final ParameterTool params;

    private final MapStateDescriptor<String, String> mapStateDescriptor;

    private JedisCluster jedisCluster;

    @Override
    public void open(Configuration parameters) {
        try {
            jedisCluster = RedisUtil.getRedisClient(params);
        } catch (Exception e) {
            LOG.error("Exception -> {}", e.getMessage());
        }
    }

    public MyBroadcastProcessFunction(
            ParameterTool params, MapStateDescriptor<String, String> mapStateDescriptor) {
        this.params = params;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    // kafka输入流数据处理
    @Override
    public void processElement(
            JSONObject value,
            BroadcastProcessFunction<JSONObject, Map<String, String>, JSONObject>.ReadOnlyContext
                    readOnlyContext,
            Collector<JSONObject> out) {

        ReadOnlyBroadcastState<String, String> broadcastState =
                readOnlyContext.getBroadcastState(mapStateDescriptor);
        // 获取表名称
        String tableCode = value.getString("TABLE_CODE");
        // 获取入口总数
        String kafkaCount = value.getString("kafkaCount");
        // 获取入口类型(方式)
        String receType = value.getString(Constant.RECE_TYPE);
        if (kafkaCount != null) {
            value.put("batchSize", kafkaCount);
        }

        String tableCode1 = tableCode;
        try {
            if (broadcastState.get("ZB_" + tableCode) != null) {
                value.remove("TABLE_CODE");
                value.put("TABLE_CODE", "ZB_" + tableCode);
                tableCode1 = value.getString("TABLE_CODE");
            }
        } catch (Exception e) {
            LOG.error("Exception -> {}", e.getMessage());
        }

        if (tableInfoMap1 == null) {
            tableInfoMap1 = new HashMap<>();
        }
        // 根据表名称从redis获取表结构
        Map<String, String> stringStringMap = jedisCluster.hgetAll(tableCode1);
        if (!stringStringMap.isEmpty()) {
            List<String> keys1 = new ArrayList<>(stringStringMap.keySet());
            // 入库类型为增量更新时，拉取表主键
            if (receType.equals("03")) {
                String primary;
                try {
                    primary = tableInfoMap.get(tableCode1);
                    tableInfoMap1.put(tableCode1, primary);
                    value.put("tableInfoMap", tableInfoMap1);
                } catch (Exception e) {
                    LOG.error(" 该表没有主键 {} ", e.getMessage());
                }
            }
            keys1.add("taskType");
            keys1.add("TABLE_CODE");
            keys1.add("RECE_TYPE");
            keys1.add("TASK_ID");
            keys1.add("batchSize");
            keys1.add("kafkaCount");
            value.keySet().removeIf(key -> !keys1.contains(key));
            out.collect(value);
            tableInfoMap1.remove(tableCode1);
        } else {
            LOG.error(" redis table schema is null --> {}", tableCode1);
        }
    }

    // redis 主键输入流数据处理
    @Override
    public void processBroadcastElement(
            Map<String, String> value,
            BroadcastProcessFunction<JSONObject, Map<String, String>, JSONObject>.Context ctx,
            Collector<JSONObject> out) {
        tableInfoMap = value;
    }

    @Override
    public void close() {
        if (jedisCluster != null) {
            RedisUtil.closeConnection();
        }
    }
}
