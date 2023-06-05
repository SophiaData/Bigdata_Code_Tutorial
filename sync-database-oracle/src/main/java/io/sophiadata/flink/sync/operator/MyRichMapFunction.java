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

    private JedisCluster jedisCluster;

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

        String replaceJsonString = replaceJson.getString(Constant.TABLE_CODE);
        String templateId = replaceJson.getString("TEMPLATE_ID");

        if (!jedisCluster.hgetAll("ZB_" + replaceJsonString).isEmpty()) {
            // 内网测试环境
            calibrate2(replaceJson, templateId);
        } else {
            // 正式环境
            calibrate(replaceJson, templateId);
        }
        return replaceJson;
    }

    private void calibrate(JSONObject replaceJson, String templateId) {
        if (templateId == null || templateId.isEmpty()) {
            replaceJson.put("RECE_TYPE", "02");
            //            LOG.warn(" templateId is empty : {} ", templateId);
        } else {
            String taskId = replaceJson.getString("TASK_ID");
            if (receTypeMap.containsKey(taskId)) {
                replaceJson.put("RECE_TYPE", receTypeMap.get(taskId));
            } else {
                String receTypeSql =
                        "select RECE_TYPE from SGAMI_HEAD_OPERATION.A_DG_EXTRACT_TASK_DET where DICT_TEMPLATE_ID = ?";
                PreparedStatement statement2 = null;
                try {
                    statement2 = connection.prepareStatement(receTypeSql);

                    statement2.setString(1, templateId);
                    @Cleanup ResultSet resultSet2 = statement2.executeQuery();
                    List<JSONObject> queryResult2 = new ArrayList<>();
                    while (resultSet2.next()) {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("RECE_TYPE", resultSet2.getString("RECE_TYPE"));
                        queryResult2.add(jsonObject);
                    }

                    if (!queryResult2.isEmpty()) {
                        String receType2 = queryResult2.get(0).getString("RECE_TYPE");
                        if (receType2 != null) {
                            receTypeMap.put(taskId, receType2);
                            replaceJson.put("RECE_TYPE", receType2);
                        } else {
                            receTypeMap.put(taskId, "02");
                            replaceJson.put("RECE_TYPE", "02");
                        }
                    } else {
                        replaceJson.put("RECE_TYPE", "00");
                        receTypeMap.put(taskId, "00");
                        LOG.error(" queryResult size is 0 ");
                    }
                } catch (SQLException e) {
                    LOG.error(" SQLException -> {}", e.getMessage());
                }
            }
        }
    }

    private void calibrate2(JSONObject replaceJson, String templateId) {
        if (templateId == null || templateId.isEmpty()) {
            replaceJson.put("RECE_TYPE", "02");
            //            LOG.warn(" templateId is empty : {} ", templateId);
        } else {
            String taskId = replaceJson.getString("TASK_ID");
            if (receTypeMap.containsKey(taskId)) {
                replaceJson.put("RECE_TYPE", receTypeMap.get(taskId));
            } else {
                String receTypeSql =
                        "select RECE_TYPE from ZB_SGAMI_HEAD_OPER.A_DG_EXTRACT_TASK_DET where DICT_TEMPLATE_ID = ?";
                PreparedStatement statement2;
                try {
                    statement2 = connection.prepareStatement(receTypeSql);

                    statement2.setString(1, templateId);
                    @Cleanup ResultSet resultSet2 = statement2.executeQuery();
                    List<JSONObject> queryResult2 = new ArrayList<>();
                    while (resultSet2.next()) {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("RECE_TYPE", resultSet2.getString("RECE_TYPE"));
                        queryResult2.add(jsonObject);
                    }

                    if (!queryResult2.isEmpty()) {
                        String receType2 = queryResult2.get(0).getString("RECE_TYPE");
                        if (receType2 != null) {
                            receTypeMap.put(taskId, receType2);
                            replaceJson.put("RECE_TYPE", receType2);
                        } else {
                            receTypeMap.put(taskId, "02");
                            replaceJson.put("RECE_TYPE", "02");
                        }
                    } else {
                        replaceJson.put("RECE_TYPE", "00");
                        receTypeMap.put(taskId, "00");
                        LOG.error(" queryResult size is 0 ");
                    }
                } catch (SQLException e) {
                    LOG.error(" SQLException -> {}", e.getMessage());
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }
}
