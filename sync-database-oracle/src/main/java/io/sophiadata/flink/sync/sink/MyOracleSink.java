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
    String resultSql;
    Long batchSize;

    HashMap<String, Long> batchSizeMap = new HashMap<>();

    HashMap<String, Long> pdDataNumMap = new HashMap<>();

    HashMap<String, Long> inDbFailNumMap = new HashMap<>();

    private String taskId;

    String receType;

    private String tableCode;

    HashMap<String, String> tableCodeMap = new HashMap<>();

    int count2 = 0;

    private String kafkaCount;

    int numFields = 0;

    String[] fieldNames = new String[0];

    private JedisCluster jedisCluster;

    HashMap<String, String> proMgtCodeMap = new HashMap<>();

    HashSet<String> keysToRemove = new HashSet<>();

    HashMap<String, String[]> fieldNamesMap = new HashMap<>();

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

        count2 = values.size();
        System.out.println(" 当前 list 大小 " + count2);

        if (values.size() == 1) {
            for (JSONObject value1 : values) {
                if (value1.getString(Constant.RECE_TYPE).equals("03")) {
                    extracted(value1);
                } else {

                    incremental(values);
                }
            }
        } else {

            incremental(values);
        }
    }

    private void extracted(JSONObject value1) {
        tableCode = value1.getString(Constant.TABLE_CODE);
        taskId = value1.getString(Constant.TASK_ID);
        String proMgtOrgCode = value1.getString(Constant.PRO_MGT_ORG_CODE);
        proMgtCodeMap.put(taskId, proMgtOrgCode);
        String tableInfoMap = value1.getString("tableInfoMap");

        Map<String, String> stringMap = jedisCluster.hgetAll(tableCode);

        String kafkaCount = value1.getString("kafkaCount");
        if (kafkaCount != null) {
            value1.remove("kafkaCount");
        }
        value1.remove("tableInfoMap");
        value1.remove(Constant.TABLE_CODE);
        value1.remove(Constant.RECE_TYPE);
        value1.remove(Constant.TASK_ID);
        JSONObject tableInfoMapJson = JSONObject.parseObject(tableInfoMap);
        String valueJsonString = value1.toJSONString();
        Map<String, Object> map = JSON.parseObject(valueJsonString, Map.class);

        if (map.keySet().size() < stringMap.keySet().size()) {
            for (String key : stringMap.keySet()) {
                if (!map.containsKey(key)) {
                    map.put(key, null);
                }
            }
        } else if (map.keySet().size() > stringMap.keySet().size()) {
            Set<String> keysToRemove = new HashSet<>();
            for (String key : map.keySet()) {
                if (!stringMap.containsKey(key)) {
                    keysToRemove.add(key);
                }
            }

            for (String key : keysToRemove) {
                map.remove(key);
            }
        }

        String keysString = String.join(",", map.keySet());
        String jsonString = JSON.toJSONString(map);
        String nullValue = NullValueHandlerUtil.nullValue(jsonString);
        JSONObject jsonObject = JSONObject.parseObject(nullValue);

        fieldNames = jsonObject.keySet().toArray(new String[0]);
        fieldNamesMap.put(taskId, fieldNames);
        String[] fieldValues = new String[fieldNamesMap.get(taskId).length];
        for (int i = 0; i < fieldNamesMap.get(taskId).length; i++) {
            fieldValues[i] = jsonObject.getString(fieldNamesMap.get(taskId)[i]);
        }

        // 解析数据

        numFields = fieldNamesMap.get(taskId).length;

        String primaryKey = tableInfoMapJson.getString(tableCode);

        String[] split = keysString.split(",");
        List<String> primaryKeyList = Arrays.asList(primaryKey.split(","));
        List<String> updateFieldList = new ArrayList<>();
        for (String elem : split) {
            if (!primaryKeyList.contains(elem)) {
                updateFieldList.add(elem);
            }
        }

        String[] fields = primaryKey.split(",");
        StringBuilder result = new StringBuilder();
        for (String field : fields) {
            result.append("t.").append(field).append("=s.").append(field).append(" AND ");
        }
        result = new StringBuilder(result.substring(0, result.length() - 5));

        String updateField = String.join(",", updateFieldList);
        String[] updateFieldSet = updateField.split(",");

        // 构造 SQL 语句  这里使用字符串拼接方式未使用预编译方式
        StringBuilder sqlBuilder = new StringBuilder("MERGE INTO ");
        sqlBuilder.append(tableCode.toUpperCase()).append(" t USING (SELECT ");
        for (int i = 0; i < numFields; i++) {
            if (stringMap.get(fieldNames[i]).equals("\"DATE\"")) {
                sqlBuilder
                        .append("TO_DATE(SUBSTR(")
                        .append("'")
                        .append(fieldValues[i])
                        .append("',1,19),'YYYY-MM-DD HH24:MI:SS') AS ")
                        .append(fieldNames[i]);
            } else if (stringMap.get(fieldNames[i]).startsWith("TIMESTAMP", 1)) {
                sqlBuilder
                        .append("TO_TIMESTAMP(SUBSTR(")
                        .append("'")
                        .append(fieldValues[i])
                        .append("',1,19),'YYYY-MM-DD HH24:MI:SS') AS ")
                        .append(fieldNames[i]);
            } else {
                sqlBuilder.append("'").append(fieldValues[i]).append("' AS ").append(fieldNames[i]);
            }
            if (i != numFields - 1) {
                sqlBuilder.append(", ");
            }
        }
        sqlBuilder.append(" FROM DUAL) s ON ( ");
        sqlBuilder.append(" %s ) WHEN MATCHED THEN UPDATE SET ");
        for (int i = 0; i < updateFieldSet.length; i++) {
            sqlBuilder.append("t.").append(updateFieldSet[i]);
            sqlBuilder.append(" = s.");
            sqlBuilder.append(updateFieldSet[i]);
            if (i != updateFieldSet.length - 1) {
                sqlBuilder.append(",");
            }
        }
        sqlBuilder.append(" WHEN NOT MATCHED THEN INSERT (");
        for (int i = 0; i < numFields; i++) {
            sqlBuilder.append("t.").append(fieldNames[i]);
            if (i != numFields - 1) {
                sqlBuilder.append(",");
            }
        }
        sqlBuilder.append(") VALUES (");
        for (int i = 0; i < numFields; i++) {
            sqlBuilder.append("s.");
            sqlBuilder.append(fieldNames[i]);
            if (i != numFields - 1) {
                sqlBuilder.append(",");
            }
        }
        sqlBuilder.append(")");

        resultSql = String.format(sqlBuilder.toString(), result);

        try {
            if (resultSql != null) {
                stmt = oracleConnection.prepareStatement(resultSql);
                stmt.execute();
                LOG.info(" success insert result sql: {}", resultSql);
            }
        } catch (SQLException e) {
            LOG.error(" SQLException sql " + resultSql);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    LOG.error(" stmt Exception: {}" + e.getMessage());
                }
            }
        }
    }

    private void incremental(ArrayList<JSONObject> values) {
        if (values.size() < 500) {
            StringBuilder builder = getBuilder(values);
            String replace1 = builder.toString().replace("'null'", "null");
            resultSql = replace1;

            try {
                stmt = oracleConnection.prepareStatement(replace1);
                int i = stmt.executeUpdate();
                pdDataNumMap.putIfAbsent(taskId, 0L);
                inDbFailNumMap.putIfAbsent(taskId, 0L);
                Long aLong = pdDataNumMap.get(taskId);
                Long aLong1 = inDbFailNumMap.get(taskId);
                aLong = aLong + i;
                pdDataNumMap.put(taskId, aLong);
                aLong1 = (long) (values.size() - i);
                inDbFailNumMap.put(taskId, aLong1);
                LOG.info(
                        " success insert result sql: {}",
                        i + "--" + taskId + " failed sql --- " + aLong1);
            } catch (SQLException e) {
                LOG.error(" 异常 " + e.getMessage());
            }

        } else {

            int count3 = 500;
            int numBatches = (int) Math.ceil(values.size() / (double) count3);
            count2 = count2 % 500;
            ArrayList<ArrayList<JSONObject>> batches = new ArrayList<>();
            for (int i1 = 0; i1 < numBatches; i1++) {
                int start = i1 * count3;
                int end = Math.min(start + count3, values.size());
                ArrayList<JSONObject> sublist = new ArrayList<>(values.subList(start, end));
                batches.add(sublist);
            }

            for (ArrayList<JSONObject> data : batches) {
                StringBuilder builder = getStringBuilder(data);
                String replace2 = builder.toString().replace("'null'", "null");
                resultSql = replace2;

                try {
                    stmt = oracleConnection.prepareStatement(replace2);
                    int i = stmt.executeUpdate();
                    long aLong = pdDataNumMap.get(taskId);
                    long aLong2 = inDbFailNumMap.get(taskId);
                    pdDataNumMap.put(taskId, aLong);
                    aLong2 = data.size() - i;
                    inDbFailNumMap.put(taskId, aLong2);
                    LOG.info(
                            " success insert result sql: {}",
                            i + "--" + taskId + " failed sql --- " + aLong2);
                } catch (SQLException e) {
                    LOG.error(" 异常 " + e.getMessage());
                }
            }
        }
    }

    private StringBuilder getBuilder(ArrayList<JSONObject> values) {
        StringBuilder builder = new StringBuilder();
        builder.append(" INSERT ALL \n");
        for (JSONObject value : values) {

            extracted(builder, value);
        }
        builder.append("SELECT 1 FROM dual\n");
        return builder;
    }

    private StringBuilder getStringBuilder(ArrayList<JSONObject> data) {
        StringBuilder builder = new StringBuilder();
        builder.append(" INSERT ALL \n");
        for (JSONObject value : data) {

            extracted(builder, value);
        }
        builder.append("SELECT 1 FROM dual\n");
        return builder;
    }

    private void extracted(StringBuilder builder, JSONObject value) {
        tableCode = value.getString(Constant.TABLE_CODE);
        taskId = value.getString(Constant.TASK_ID);
        tableCodeMap.put(taskId, tableCode);

        Map<String, String> stringStringMap = jedisCluster.hgetAll(tableCodeMap.get(taskId));

        batchSize = Long.valueOf(value.getString("batchSize"));
        batchSizeMap.put(taskId, batchSize);
        String proMgtOrgCode = value.getString(Constant.PRO_MGT_ORG_CODE);

        proMgtCodeMap.put(taskId, proMgtOrgCode);
        kafkaCount = value.getString("kafkaCount");

        receType = value.getString(Constant.RECE_TYPE);

        value.remove("batchSize");
        value.remove("tableMap");
        value.remove(Constant.TABLE_CODE);
        value.remove(Constant.RECE_TYPE);
        value.remove(Constant.TASK_ID);
        value.remove("tableInfoMap");

        if (kafkaCount != null) {
            value.remove("kafkaCount");
        }

        String valueJsonString = value.toJSONString();
        Map<String, Object> map = JSON.parseObject(valueJsonString, Map.class);

        if (map.keySet().size() < stringStringMap.keySet().size()) {
            for (String key : stringStringMap.keySet()) {
                map.putIfAbsent(key, null);
            }
        } else if (map.keySet().size() > stringStringMap.keySet().size()) {
            for (String key : map.keySet()) {
                if (!stringStringMap.containsKey(key)) {
                    keysToRemove.add(key);
                }
            }
            for (String key : keysToRemove) {
                map.remove(key);
            }

            keysToRemove.clear();
        }

        String jsonString = JSON.toJSONString(map);
        String nullValue = NullValueHandlerUtil.nullValue(jsonString);
        JSONObject jsonObject = JSONObject.parseObject(nullValue);

        fieldNames = jsonObject.keySet().toArray(new String[0]);
        fieldNamesMap.put(taskId, fieldNames);
        String[] fieldValues = new String[fieldNamesMap.get(taskId).length];
        for (int i = 0; i < fieldNamesMap.get(taskId).length; i++) {
            fieldValues[i] = jsonObject.getString(fieldNamesMap.get(taskId)[i]);
        }

        // 解析数据

        numFields = fieldNamesMap.get(taskId).length;

        // Iterate over the values and add them to the list
        StringBuilder valuesBuilder = new StringBuilder();
        valuesBuilder.append("(");
        for (int i = 0; i < numFields; i++) {
            if (stringStringMap.get(fieldNamesMap.get(taskId)[i]).equals("\"DATE\"")) {
                if (fieldValues[i] == null) {
                    valuesBuilder.append("null");
                } else {
                    valuesBuilder
                            .append("TO_DATE(SUBSTR(")
                            .append("'")
                            .append(fieldValues[i])
                            .append("',1,19),'YYYY-MM-DD HH24:MI:SS') ");
                }

            } else if (stringStringMap
                    .get(fieldNamesMap.get(taskId)[i])
                    .startsWith("TIMESTAMP", 1)) {
                if (fieldValues[i] == null) {
                    valuesBuilder.append("null");
                } else {
                    valuesBuilder
                            .append("TO_TIMESTAMP(SUBSTR(")
                            .append("'")
                            .append(fieldValues[i])
                            .append("',1,19),'YYYY-MM-DD HH24:MI:SS') ");
                }

            } else {
                valuesBuilder.append("'").append(fieldValues[i]).append("' ");
            }
            if (i != numFields - 1) {
                valuesBuilder.append(", ");
            }
        }
        valuesBuilder.append(")");

        builder.append(" INTO ");
        builder.append(tableCodeMap.get(taskId));
        builder.append(" (");
        for (int j = 0; j < numFields; j++) {
            builder.append(fieldNamesMap.get(taskId)[j]);
            if (j != numFields - 1) {
                builder.append(",");
            }
        }
        builder.append(") VALUES ");
        builder.append(valuesBuilder);
        builder.append(" LOG ERRORS INTO SGAMI_SUPPORT.S_ERR_LOG ('");
        builder.append(taskId);
        builder.append("') REJECT LIMIT UNLIMITED ");
        builder.append("\n");
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
