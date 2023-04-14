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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.sophiadata.flink.sync2.sink.CreateOracleSinkTable;
import io.sophiadata.flink.sync2.source.MyKafkaSource;
import io.sophiadata.flink.sync2.utils.ConvertUtil;
import io.sophiadata.flink.sync2.utils.JdbcUtil;
import io.sophiadata.flink.sync2.utils.RedisUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** (@SophiaData) (@date 2023/4/11 17:22). */
public class KafkaToOracle2 {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaToOracle2.class);
    private static final HashMap<String, String> tableSchemaMap = new HashMap<>();

    static Connection connection = null;

    public void kafkaToOracle2(
            String[] args,
            StreamExecutionEnvironment env,
            StreamTableEnvironment tEnv,
            ParameterTool params)
            throws Exception {

        env.setParallelism(1); // 测试并行度为 1
        //        env.registerCachedFile("hdfs://cdh81:8020/user/hdfs/", "cacheFile");

        Map<String, String> tableLists = RedisUtil.getRedisClient(params).hgetAll("tableList");
        String tableListsString = JSON.toJSONString(tableLists);
        JSONObject tableListJson = JSONObject.parseObject(tableListsString);
        Set<String> tableListSets = tableListJson.keySet();
        List<String> tables = new ArrayList<>(tableListSets);

        HashSet<String> dataBases = new HashSet<>();

        // 基于内存的 catalog 信息
        GenericInMemoryCatalog myCatalog = new GenericInMemoryCatalog("myCatalog");
        tEnv.registerCatalog("myCatalog", myCatalog);
        tEnv.useCatalog("myCatalog");

        for (String dataBase : tables) {
            String[] split = dataBase.split("\\.");
            dataBases.add(split[0]);
        }

        for (String database : dataBases) {
            myCatalog.createDatabase(database, new CatalogDatabaseImpl(new HashMap<>(), ""), true);
        }

        Map<String, RowTypeInfo> tableTypeInformationMap = Maps.newConcurrentMap();
        Map<String, DataType[]> tableDataTypesMap = Maps.newConcurrentMap();
        Map<String, RowType> tableRowTypeMap = Maps.newConcurrentMap();

        for (String table : tables) {
            Map<String, String> tableSchema = RedisUtil.getRedisClient(params).hgetAll(table);
            String tableSchemaString = JSON.toJSONString(tableSchema);
            tableSchemaMap.put(table, tableSchemaString);
            JSONObject tableSchemaJson = JSON.parseObject(tableSchemaString);

            TypeInformation<?>[] filedTypes = new TypeInformation[tableSchemaJson.size()];

            String[] fieldNames = new String[tableSchemaJson.size()];
            // 获取DataType
            DataType[] fieldDataTypes = new DataType[tableSchemaJson.size()];
            LogicalType[] logicalTypes = new LogicalType[tableSchemaJson.size()];

            for (int i = 0; i < tableSchemaJson.size(); i++) {

                Set<String> tableSchemaKeySet = tableSchemaJson.keySet();
                Collection<Object> tableSchemaValues = tableSchemaJson.values();

                ArrayList<String> tableSchemaKeySetList = new ArrayList<>(tableSchemaKeySet);
                ArrayList<Object> valuesList = new ArrayList<>(tableSchemaValues);

                fieldNames[i] = tableSchemaKeySetList.get(i);
                fieldDataTypes[i] = ConvertUtil.convertFromColumn((String) valuesList.get(i));
                filedTypes[i] =
                        InternalTypeInfo.of(
                                ConvertUtil.convertFromColumn((String) valuesList.get(i))
                                        .getLogicalType());

                logicalTypes[i] =
                        ConvertUtil.convertFromColumn((String) valuesList.get(i)).getLogicalType();
            }

            RowType rowType = RowType.of(logicalTypes, fieldNames);

            List<String> primaryKeys;
            primaryKeys = Collections.singletonList(tableListJson.getString(table));

            tableRowTypeMap.put(table, rowType);
            // 拼接 Flink Sink 表 DDL sql
            StringBuilder stmt = new StringBuilder();
            String[] split = table.split("\\.");
            String sinkTableName = String.format(table);
            String sinkTableName2 = split[1];
            stmt.append("create table if not exists ").append(sinkTableName).append("(\n");

            for (int i = 0; i < fieldNames.length; i++) {
                String column = fieldNames[i];
                String fieldDataType = fieldDataTypes[i].toString();
                stmt.append("\t`").append(column).append("` ").append(fieldDataType).append(",\n");
            }
            //            int lastIndexOf = stmt.lastIndexOf(",");
            //            stmt.replace(lastIndexOf, lastIndexOf + 1, ")");
            stmt.append(
                    String.format(
                            "PRIMARY KEY (%s) NOT ENFORCED\n)",
                            StringUtils.join(primaryKeys, ",")));

            String connectorWithBody = CreateOracleSinkTable.connectorWithBody(params);

            String formatJdbcSinkWithBody =
                    connectorWithBody.replace("${sinkTableName}", sinkTableName2);
            String createSinkTableDdl = stmt + formatJdbcSinkWithBody;

            // 创建 Flink Sink 表
            LOG.info("createSinkTableDdl: \r {}", createSinkTableDdl);
            tEnv.executeSql(createSinkTableDdl);
            tableDataTypesMap.put(table, fieldDataTypes);
            tableTypeInformationMap.put(table, new RowTypeInfo(filedTypes, fieldNames));
        }

        RedisUtil.closeConnection();

        StatementSet statementSet = tEnv.createStatementSet();

        SingleOutputStreamOperator<String> kafkaSourceStream =
                new MyKafkaSource()
                        .singleOutputStreamOperator(params, env, "DEFAULT_GROUP")
                        .uid("KafkaToOracle-kafkaSourceStream")
                        .name("KafkaToOracle-kafkaSourceStream");

        SingleOutputStreamOperator<String> filterDS =
                filter(kafkaSourceStream).uid("KafkaToOracle-filter").name("KafkaToOracle-filter");

        connection = JdbcUtil.getMysqlConnection(params);

        SingleOutputStreamOperator<JSONObject> mapDS =
                map(filterDS).uid("KafkaToOracle-map").name("KafkaToOracle-map");

        SingleOutputStreamOperator<Tuple2<String, Row>> process =
                keyby(mapDS)
                        .uid("KafkaToOracle-filter-process")
                        .name("KafkaToOracle-filter-process");

        for (Map.Entry<String, RowTypeInfo> entry : tableTypeInformationMap.entrySet()) {
            String tableName = entry.getKey();
            RowTypeInfo rowTypeInfo = entry.getValue();
            SingleOutputStreamOperator<Row> mapStream =
                    process.filter(data -> data.f0.equals(tableName))
                            .map(data -> data.f1, rowTypeInfo);

            Table table = tEnv.fromChangelogStream(mapStream);
            String temporaryViewName = String.format("t_%s", tableName);
            tEnv.createTemporaryView(temporaryViewName, table);

            String sinkTableName = String.format(tableName);
            String insertSql =
                    String.format(
                            "insert into %s select * from %s", sinkTableName, temporaryViewName);
            LOG.info("add insertSql for {}, sql: {}", tableName, insertSql);
            statementSet.addInsertSql(insertSql);
        }
        statementSet.execute();

        env.execute();
    }

    private SingleOutputStreamOperator<String> filter(
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

    private SingleOutputStreamOperator<JSONObject> map(SingleOutputStreamOperator<String> filter) {
        return filter.map(
                (MapFunction<String, JSONObject>)
                        value -> {
                            String replaceAll = value.replaceAll("null", "\"\"");
                            return JSONObject.parseObject(replaceAll);
                        });
    }

    private SingleOutputStreamOperator<Tuple2<String, Row>> keyby(
            SingleOutputStreamOperator<JSONObject> map) {
        return map.keyBy(r -> r.getString("PRO_MGT_ORG_CODE") + r.getString("TASK_ID"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, Tuple2<String, Row>>() {
                            private transient MapState<String, String> tableSchemaState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);

                                tableSchemaState =
                                        getRuntimeContext()
                                                .getMapState(
                                                        new MapStateDescriptor<>(
                                                                "tableSchemaState",
                                                                String.class,
                                                                String.class));
                            }

                            @Override
                            public void processElement(
                                    JSONObject value,
                                    KeyedProcessFunction<String, JSONObject, Tuple2<String, Row>>
                                                    .Context
                                            ctx,
                                    Collector<Tuple2<String, Row>> out)
                                    throws Exception {

                                String tableCode = value.getString("TABLE_CODE");

                                tableSchemaState.putAll(tableSchemaMap);

                                String tableSchema = tableSchemaState.get(tableCode);
                                Set<String> tableSchemaKeySets =
                                        JSONObject.parseObject(tableSchema).keySet();

                                JSONObject originalTableSchema = new JSONObject();

                                for (String tableSchemaKey : tableSchemaKeySets) {
                                    String valueString = value.getString(tableSchemaKey);
                                    originalTableSchema.put(tableSchemaKey, valueString);
                                }

                                Collection<Object> originalTableSchemaValues =
                                        originalTableSchema.values();

                                ArrayList<Object> originalTableSchemaList =
                                        new ArrayList<>(originalTableSchemaValues);

                                Row row = new Row(originalTableSchemaList.size());

                                for (int i = 0; i < originalTableSchemaList.size(); i++) {
                                    row.setField(
                                            i,
                                            StringData.fromString(
                                                    originalTableSchemaList.get(i).toString()));
                                }

                                out.collect(new Tuple2<>(tableCode, row));
                            }
                        });
    }
}
