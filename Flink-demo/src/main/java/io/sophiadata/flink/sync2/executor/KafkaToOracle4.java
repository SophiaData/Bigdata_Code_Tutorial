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
package io.sophiadata.flink.sync2.executor;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.sophiadata.flink.sync2.source.MyKafkaSource;
import io.sophiadata.flink.sync2.source.RedisSource;
import io.sophiadata.flink.sync2.utils.JdbcUtil;
import io.sophiadata.flink.sync2.utils.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** (@SophiaData) (@date 2023/4/11 17:15). */
public class KafkaToOracle4 {
    // merge 写入
    private static final Logger LOG = LoggerFactory.getLogger(KafkaToOracle4.class);

    public void kafkaToOracle4(StreamExecutionEnvironment env, ParameterTool params)
            throws Exception {

        env.setParallelism(1);

        SingleOutputStreamOperator<String> kafkaSourceStream =
                new MyKafkaSource()
                        .singleOutputStreamOperator(params, env, "DEFAULT_GROUP")
                        .uid("KafkaToOracle-kafkaSourceStream")
                        .name("KafkaToOracle-kafkaSourceStream");

        kafkaSourceStream.print("kafka: ->  ");

        SingleOutputStreamOperator<String> filterDS =
                filter(kafkaSourceStream).uid("KafkaToOracle-filter").name("KafkaToOracle-filter");

        SingleOutputStreamOperator<JSONObject> mapDS =
                map(filterDS).uid("KafkaToOracle-map").name("KafkaToOracle-map");

        DataStreamSource<Map<String, String>> redisSource = env.addSource(new RedisSource(params));

        redisSource.print("redisSource: -> ");

        MapStateDescriptor<String, String> mapStateDescriptor =
                new MapStateDescriptor<>("map-state", String.class, String.class);

        BroadcastStream<Map<String, String>> broadcastStream =
                redisSource.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, Map<String, String>> connect =
                mapDS.connect(broadcastStream);

        SingleOutputStreamOperator<JSONObject> process =
                connect.process(
                        new BroadcastProcessFunction<
                                JSONObject, Map<String, String>, JSONObject>() {

                            Map<String, String> tableInfoMap;
                            Map<String, Map<String, String>> tableMap;

                            Map<String, String> tableSchema = new HashMap<>();

                            @Override
                            public void processElement(
                                    JSONObject jsonObject,
                                    BroadcastProcessFunction<
                                                            JSONObject,
                                                            Map<String, String>,
                                                            JSONObject>
                                                    .ReadOnlyContext
                                            readOnlyContext,
                                    Collector<JSONObject> collector)
                                    throws Exception {
                                ReadOnlyBroadcastState<String, String> broadcastState =
                                        readOnlyContext.getBroadcastState(mapStateDescriptor);
                                String tableCode = jsonObject.getString("TABLE_CODE");
                                System.out.println("tableCode: " + tableCode);

                                if (broadcastState.get("XX_" + tableCode) != null) {
                                    jsonObject.remove("TABLE_CODE");
                                    jsonObject.put("TABLE_CODE", "ZB_" + tableCode);
                                    String tableCode1 = jsonObject.getString("TABLE_CODE");
                                    String keys = broadcastState.get(tableCode1);
                                    if (keys != null) {
                                        System.out.println("keys 不为 null ---> " + keys);

                                        jsonObject.keySet().removeIf(key -> !keys.contains(key));

                                        String tableMapJson = JSON.toJSONString(tableMap);
                                        String tableInfoMapJson = JSON.toJSONString(tableInfoMap);

                                        jsonObject.put("tableMap", tableMapJson);
                                        jsonObject.put("tableInfoMap", tableInfoMapJson);

                                        collector.collect(jsonObject);
                                    }
                                } else {
                                    String keys = broadcastState.get(tableCode);
                                    if (keys != null) {
                                        System.out.println("keys 不为 null ---> " + keys);

                                        jsonObject.keySet().removeIf(key -> !keys.contains(key));

                                        String tableMapJson = JSON.toJSONString(tableMap);
                                        String tableInfoMapJson = JSON.toJSONString(tableInfoMap);

                                        jsonObject.put("tableMap", tableMapJson);
                                        jsonObject.put("tableInfoMap", tableInfoMapJson);

                                        collector.collect(jsonObject);
                                    }
                                }
                            }

                            @Override
                            public void processBroadcastElement(
                                    Map<String, String> value,
                                    BroadcastProcessFunction<
                                                            JSONObject,
                                                            Map<String, String>,
                                                            JSONObject>
                                                    .Context
                                            context,
                                    Collector<JSONObject> collector)
                                    throws Exception {
                                BroadcastState<String, String> broadcastState =
                                        context.getBroadcastState(mapStateDescriptor);

                                tableInfoMap = value;
                                String jsonString = JSON.toJSONString(value);

                                JSONObject tableListJson = JSONObject.parseObject(jsonString);
                                Set<String> tableListSets = tableListJson.keySet();

                                List<String> tables = new ArrayList<>(tableListSets);
                                Map<String, Map<String, String>> stringMapMap = new HashMap<>();
                                for (String table : tables) {
                                    tableSchema = RedisUtil.getRedisClient(params).hgetAll(table);
                                    System.out.println("table: " + table);
                                    System.out.println("tableSchema: " + tableSchema);
                                    stringMapMap.put(table, tableSchema);
                                    tableMap = stringMapMap;

                                    List<String> keys = new ArrayList<>(tableSchema.keySet());
                                    keys.add("TABLE_CODE");
                                    broadcastState.put(table, keys.toString());
                                }
                            }
                        });

        // 使用自定义 sink 写入 Oracle 数据库

        process.addSink(
                new RichSinkFunction<JSONObject>() {

                    Connection connection;
                    PreparedStatement stmt;

                    String s1;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        connection = JdbcUtil.getOracleConnection(params).getConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        // 关闭数据库连接
                        if (stmt != null) {
                            stmt.close();
                        }
                        if (connection != null) {
                            connection.close();
                        }
                    }

                    @Override
                    public void invoke(JSONObject value, Context context) throws Exception {
                        super.invoke(value, context);
                        try {
                            String tableMap = value.getString("tableMap");
                            //                            System.out.println("tableMap: " +
                            // tableMap);
                            String tableInfoMap = value.getString("tableInfoMap");
                            JSONObject tableInfoMapJson = JSONObject.parseObject(tableInfoMap);
                            JSONObject tableMapJson = JSONObject.parseObject(tableMap);
                            String s2 = tableMapJson.toJSONString();
                            Map<String, Object> tableMaps = JSON.parseObject(s2);

                            String tableCode = value.getString("TABLE_CODE");
                            System.out.println("tableCode: " + tableCode);
                            value.remove("tableMap");
                            value.remove("tableInfoMap");
                            System.out.println("value - " + value);
                            value.remove("TABLE_CODE");

                            String s = value.toJSONString();
                            Map<String, Object> map = JSON.parseObject(s);

                            String keysString = String.join(",", map.keySet());
                            String valueString =
                                    String.join(
                                            ",",
                                            map.values().stream()
                                                    .map(Object::toString)
                                                    .toArray(String[]::new));
                            ;

                            // 解析数据
                            String[] fieldNames = keysString.split(",");

                            String[] fieldValues = valueString.split(",");
                            int numFields = fieldNames.length;
                            String primaryKey = tableInfoMapJson.getString(tableCode);

                            Object o = tableMaps.get(tableCode);

                            Map map1 =
                                    JSONObject.parseObject(JSONObject.toJSONString(o), Map.class);

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
                                result.append("t.")
                                        .append(field)
                                        .append("=s.")
                                        .append(field)
                                        .append(" AND ");
                            }
                            result = new StringBuilder(result.substring(0, result.length() - 5));

                            String updateField = String.join(",", updateFieldList);
                            String[] updateFieldSet = updateField.split(",");

                            // 构造 SQL 语句
                            StringBuilder sqlBuilder = new StringBuilder("MERGE INTO ");
                            sqlBuilder.append(tableCode.toUpperCase()).append(" t USING (SELECT ");
                            for (int i = 0; i < numFields; i++) {
                                if (map1.get(fieldNames[i]).equals("\"DATE\"")) {
                                    System.out.println("111 ");
                                    sqlBuilder
                                            .append("TO_DATE(SUBSTR(")
                                            .append("'")
                                            .append(fieldValues[i])
                                            .append("',1,19),'YYYY-MM-DD HH24:MI:SS') AS ")
                                            .append(fieldNames[i]);
                                } else if (map1.get(fieldNames[i])
                                        .toString()
                                        .startsWith("TIMESTAMP", 1)) {
                                    sqlBuilder
                                            .append("TO_TIMESTAMP(SUBSTR(")
                                            .append("'")
                                            .append(fieldValues[i])
                                            .append("',1,19),'YYYY-MM-DD HH24:MI:SS') AS ")
                                            .append(fieldNames[i]);
                                } else {
                                    sqlBuilder
                                            .append("'")
                                            .append(fieldValues[i])
                                            .append("' AS ")
                                            .append(fieldNames[i]);
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

                            s1 = String.format(sqlBuilder.toString(), result);

                            System.out.println(s1);
                            stmt = connection.prepareStatement(s1);
                            stmt.execute();

                        } catch (Exception e) {
                            LOG.error("  异常： " + e + " - " + s1);
                        }
                    }
                });

        env.execute("KafkaToOracle");
    }

    private static SingleOutputStreamOperator<String> filter(
            SingleOutputStreamOperator<String> stringDataStreamSource) {

        return stringDataStreamSource.filter(
                (FilterFunction<String>)
                        value -> {
                            try {
                                if (value.equals("null")) {
                                    return false;
                                }
                                JSONObject.parse(value);
                                return true;
                            } catch (Exception e) {

                                return false;
                            }
                        });
    }

    private static SingleOutputStreamOperator<JSONObject> map(
            SingleOutputStreamOperator<String> filter) {
        return filter.map(
                (MapFunction<String, JSONObject>)
                        value -> {
                            String replaceAll = value.replaceAll("null", "\"0\"");
                            return JSONObject.parseObject(replaceAll);
                        });
    }
}
