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

package io.sophiadata.flink.sync.sink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.sophiadata.flink.sync.Constant;
import io.sophiadata.flink.sync.utils.JdbcUtil;
import io.sophiadata.flink.sync.utils.NullValueHandlerUtil;
import io.sophiadata.flink.sync.utils.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** (@SophiaData) (@date 2023/5/16 11:40). */
public class MyOracleSink extends RichSinkFunction<ArrayList<JSONObject>> {

    private static final Logger LOG = LoggerFactory.getLogger(MyOracleSink.class);

    private final ParameterTool params;
    private transient Connection oracleConnection;
    private transient PreparedStatement stmt;

    JedisCluster jedisCluster;

    public MyOracleSink(ParameterTool params) {
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) {
        try {
            oracleConnection = JdbcUtil.getOracleConnection(params).getConnection();
        } catch (SQLException e) {
            LOG.error("oracleConnection error -> {}", e.getMessage());
        }
        try {
            jedisCluster = RedisUtil.getRedisClient(params);
        } catch (Exception e) {
            LOG.error("Exception -> {}", e.getMessage());
        }
    }

    @Override
    public void invoke(ArrayList<JSONObject> values, Context context) {

        // Your business logic
    }

    @Override
    public void close() throws Exception {
        super.close();
        // 关闭数据库连接
        if (stmt != null) {
            stmt.close();
        }
        if (oracleConnection != null) {
            oracleConnection.close();
        }
    }
}
