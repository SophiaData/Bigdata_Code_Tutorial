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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import com.alibaba.fastjson.JSONObject;
import io.sophiadata.flink.sync.Constant;
import io.sophiadata.flink.sync.utils.JdbcUtil;
import io.sophiadata.flink.sync.utils.RedisUtil;
import lombok.Cleanup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** (@SophiaData) (@date 2023/4/20 15:01). */
public class MyRichMapFunction extends RichMapFunction<String, JSONObject> {

    private static final Logger LOG = LoggerFactory.getLogger(MyRichMapFunction.class);

    private final ParameterTool params;
    private transient Connection connection;

    private HashMap<String, String> receTypeMap;

    JedisCluster jedisCluster;

    public MyRichMapFunction(ParameterTool params) {
        this.params = params;
    }

    // 执行一次
    @Override
    public void open(Configuration parameters) {
        try {
            connection = JdbcUtil.getOracleConnection(params).getConnection();
        } catch (SQLException e) {
            LOG.error("SQLException -> {}", e.getMessage());
        }
        receTypeMap = new HashMap<>();
        try {
            jedisCluster = RedisUtil.getRedisClient(params);
        } catch (Exception e) {
            LOG.error(" redis error ");
        }
    }

    // 输入流每条数据执行一次
    @Override
    public JSONObject map(String value) {

        JSONObject replaceJson = new JSONObject();

        if (receTypeMap.size() == 100000) {
            receTypeMap.clear();
        }

        try {
            replaceJson = JSONObject.parseObject(value);
        } catch (Exception e) {
            LOG.error(" json syntax ->  {} reason: {}  data -> {}", value, e.getMessage(), value);
        }

        // Your business logic
        return replaceJson;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }
}
