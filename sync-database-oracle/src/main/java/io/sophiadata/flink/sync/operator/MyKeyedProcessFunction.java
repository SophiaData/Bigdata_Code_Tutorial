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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSONObject;
import io.sophiadata.flink.sync.Constant;
import io.sophiadata.flink.sync.utils.DateUtil;
import io.sophiadata.flink.sync.utils.JdbcUtil;
import io.sophiadata.flink.sync.utils.KafkaUtils;
import io.sophiadata.flink.sync.utils.UpDataDataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/** (@SophiaData) (@date 2023/5/14 17:39). */
public class MyKeyedProcessFunction
        extends KeyedProcessFunction<String, JSONObject, ArrayList<JSONObject>> {
    private static final Logger logger = LoggerFactory.getLogger(MyKeyedProcessFunction.class);

    ParameterTool params;
    static Connection connection;

    HashMap<String, ArrayList<JSONObject>> bufferMap = new HashMap<>();

    private transient ValueState<Tuple3<Long, Long, Long>> statisticsState;

    private String taskId;

    ArrayList<JSONObject> threeList = new ArrayList<>();
    static ConcurrentHashMap<String, String> proMgtOrgCodeMap = new ConcurrentHashMap<>();

    private Tuple3<Long, Long, Long> tuple3;

    static ConcurrentHashMap<String, Long> countMap = new ConcurrentHashMap<>();

    static ConcurrentHashMap<String, String> kafkaCountMap1 = new ConcurrentHashMap<>();

    ConcurrentHashMap<String, Long> batchSizeMap = new ConcurrentHashMap<>();

    private Long f2;

    private String receType;

    private String tableCode;

    private Long batchSize;

    public MyKeyedProcessFunction(ParameterTool params) {
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) {

        ValueStateDescriptor<Tuple3<Long, Long, Long>> StatisticsDescriptor =
                new ValueStateDescriptor<>(
                        "taskIdTimeStamp",
                        TypeInformation.of(new TypeHint<Tuple3<Long, Long, Long>>() {}),
                        Tuple3.of(0L, 0L, 0L));

        statisticsState = getRuntimeContext().getState(StatisticsDescriptor);

        try {
            connection = JdbcUtil.getOracleConnection(params).getConnection();
        } catch (SQLException e) {
            logger.error("SQLException -> {}", e.getMessage());
        }
    }

    @Override
    public void processElement(
            JSONObject value,
            KeyedProcessFunction<String, JSONObject, ArrayList<JSONObject>>.Context context,
            Collector<ArrayList<JSONObject>> out) {

        try {
            tuple3 = statisticsState.value();
        } catch (IOException e) {
            logger.error(" statisticsState IOException {}", e.getMessage());
        }

        taskId = value.getString(Constant.TASK_ID);
        String proMgtOrgCode = value.getString(Constant.PRO_MGT_ORG_CODE);
        proMgtOrgCodeMap.put(taskId, proMgtOrgCode);
        receType = value.getString(Constant.RECE_TYPE);
        String batchSize1 = value.getString(Constant.BATCH_SIZE);
        tableCode = value.getString(Constant.TABLE_CODE);
        String kafkaCount = value.getString("kafkaCount");
        if (kafkaCount == null) {
            kafkaCountMap1.put(taskId, "null");
        } else {
            kafkaCountMap1.put(taskId, kafkaCount);
        }

        if (receType != null) {
            final String RECE_TYPE1 = "01";
            final String RECE_TYPE2 = "02";
            final String RECE_TYPE3 = "03";
            switch (receType) {
                case RECE_TYPE1:
                case RECE_TYPE2:
                    if (batchSize1 != null) {
                        tuple3.f0 = Long.valueOf(batchSize1);
                        batchSize = Long.valueOf(batchSize1);
                        batchSizeMap.put(taskId, Long.valueOf(batchSize1));
                    }
                    try {
                        f2 = tuple3.f2;
                        if (f2 == 0L) {
                            f2 = System.currentTimeMillis() + 2 * 60 * 1000;
                            tuple3.f2 = f2;
                            batchSize = tuple3.f0;
                            batchSizeMap.put(taskId, batchSize);
                            statisticsState.update(tuple3);
                            System.out.println(" 注册六小时定时器: " + context.getCurrentKey());
                            context.timerService()
                                    .registerProcessingTimeTimer(f2 + 6 * 60 * 60 * 1000);
                        }
                    } catch (Exception e) {
                        logger.error(" taskIdTimeStampState exception -> {}", e.getMessage());
                    }

                    if (bufferMap.get(taskId) == null) {
                        Long f0 = tuple3.f0;
                        System.out.println("f0: " + f0);
                        batchSizeMap.put(taskId, f0);
                        ArrayList<JSONObject> arrayList1 = new ArrayList<>();
                        arrayList1.add(value);
                        countMap.put(taskId, 1L);
                        bufferMap.put(taskId, arrayList1);
                        new KafkaUtils(params)
                                .updateKafkaTaskStatus(taskId, proMgtOrgCode, "03", "");

                        UpDataDataUtil.updateTaskStatus(
                                kafkaCount, connection, taskId, proMgtOrgCode, "03");
                        try {
                            ArrayList<JSONObject> arrayList = bufferMap.get(taskId);
                            comparison(
                                    context,
                                    out,
                                    receType,
                                    tableCode,
                                    arrayList,
                                    batchSizeMap.get(taskId));
                        } catch (Exception e) {
                            logger.error(
                                    " arrayList isBatchComplete exception -> {}", e.getMessage());
                        }
                        System.out.println(" 当前批次初始化 " + taskId);
                        context.timerService()
                                .registerProcessingTimeTimer(
                                        System.currentTimeMillis() + 60 * 1000L);
                    } else {
                        ArrayList<JSONObject> arrayList = bufferMap.get(taskId);
                        arrayList.add(value);
                        bufferMap.put(taskId, arrayList);
                        countMap.putIfAbsent(taskId, 0L);
                        Long aLong = countMap.get(taskId);
                        aLong = aLong + 1L;
                        countMap.put(taskId, aLong);
                    }
                    if (batchSizeMap.get(taskId) == 0) {

                        // Query the Oracle database to get the batch size
                        long batchSize2 = 0L;
                        try {
                            batchSize2 = getBatchSizeFromOracle(taskId, proMgtOrgCode, connection);
                            System.out.println(" 查询 batchSize " + context.getCurrentKey());
                        } catch (Exception e) {
                            logger.error(" getBatchSizeFromOracle exception -> {}", e.getMessage());
                        }
                        if (batchSize2 == -1) {
                            tuple3.f0 = batchSize2;
                            batchSize = tuple3.f0;
                            batchSizeMap.put(taskId, batchSize2);
                            System.out.println(" batchSize = -1 注册定时器 " + taskId);
                            context.timerService()
                                    .registerProcessingTimeTimer(
                                            System.currentTimeMillis() + 60 * 1000);
                        } else {
                            tuple3.f0 = batchSize2;
                            batchSize = tuple3.f0;
                            batchSizeMap.put(taskId, batchSize2);
                            System.out.println(" 成功获取 batchSize " + batchSize2 + " -- " + taskId);
                            try {
                                ArrayList<JSONObject> arrayList = bufferMap.get(taskId);
                                comparison(
                                        context,
                                        out,
                                        receType,
                                        tableCode,
                                        arrayList,
                                        batchSizeMap.get(taskId));
                            } catch (Exception e) {
                                logger.error(
                                        " 成功获取 batchSize2 isBatchComplete exception -> {}",
                                        e.getMessage());
                            }
                        }
                    } else {
                        try {
                            ArrayList<JSONObject> arrayList = bufferMap.get(taskId);
                            comparison(
                                    context,
                                    out,
                                    receType,
                                    tableCode,
                                    arrayList,
                                    batchSizeMap.get(taskId));
                        } catch (Exception e) {
                            logger.error(
                                    " batchSize2 isBatchComplete exception -> {}", e.getMessage());
                        }
                    }

                    break;
                case RECE_TYPE3:
                    threeList.add(value);
                    out.collect(threeList);
                    threeList.clear();
                default:
                    logger.error(
                            String.format(
                                    params.get("jobName") + " The type caught is incorrect : %s ",
                                    receType));
                    break;
            }
        }
    }

    private void comparison(
            KeyedProcessFunction<String, JSONObject, ArrayList<JSONObject>>.Context context,
            Collector<ArrayList<JSONObject>> out,
            String receType,
            String tableCode,
            ArrayList<JSONObject> arrayList,
            Long batchSize2)
            throws IOException {
        if (isBatchComplete(arrayList, batchSize2, taskId)) {
            UpDataDataUtil.updateReadKafka(
                    kafkaCountMap1.get(taskId),
                    connection,
                    taskId,
                    proMgtOrgCodeMap.get(taskId),
                    countMap.get(taskId));
            statisticsState.update(tuple3);
            new KafkaUtils(params)
                    .updateKafkaTaskStatus(taskId, proMgtOrgCodeMap.get(taskId), "04", "");
            UpDataDataUtil.updateTaskStatus(
                    kafkaCountMap1.get(taskId),
                    connection,
                    taskId,
                    proMgtOrgCodeMap.get(taskId),
                    "04");
            System.out.println(" 数据总量 " + arrayList.size() + " -- " + taskId);
            logger.info(" all data has arrived: {} ", taskId);
            System.out.println(taskId + " all data has arrived: %s %n");
            delTableData(proMgtOrgCodeMap.get(taskId), tableCode, receType, connection);
            System.out.println(
                    taskId + " 目标表数据删除成功 " + DateUtil.toDate(System.currentTimeMillis()));
            logger.info(" data deleted completed ");
            // Emit the data downstream
            System.out.println(taskId + " 数据开始下发 " + DateUtil.toDate(System.currentTimeMillis()));

            System.out.println(taskId + " 数据下发总量 " + arrayList.size() + " --- " + taskId);
            ArrayList<JSONObject> arrayList1 = new ArrayList<>();
            for (JSONObject data : arrayList) {
                data.put("batchSize", batchSize);
                arrayList1.add(data);
            }
            out.collect(arrayList1);
            System.out.println(" 数据清理 " + taskId);
            arrayList1.clear();
            arrayList.clear();
            bufferMap.remove(taskId);
            System.out.println(taskId + " 数据下发完成 " + DateUtil.toDate(System.currentTimeMillis()));
            if (arrayList.isEmpty()) {
                System.out.println(" 数据清理完成 ");
            }
            batchSize = 0L;
            tuple3.f0 = batchSize;
            tuple3.f1 = 0L;
            context.timerService().deleteProcessingTimeTimer(f2 + 6 * 60 * 60 * 1000);
            System.out.println("定时器： 删除六小时定时器 ");
            f2 = 0L;
            tuple3.f2 = f2;
            statisticsState.update(tuple3);
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<String, JSONObject, ArrayList<JSONObject>>.OnTimerContext context,
            Collector<ArrayList<JSONObject>> out) {
        System.out.println(" 进入定时器 " + taskId);
        long timestamps;
        try {
            if (context.timestamp() == f2 + 6 * 60 * 60 * 1000L) {
                System.out.println(
                        context.getCurrentKey()
                                + " 数据未到齐超过 6 小时 "
                                + DateUtil.toDate(System.currentTimeMillis()));
                logger.warn(" current taskId data overtime {}  data clear !!! ", taskId);
                bufferMap.remove(taskId);
                tuple3.f0 = 0L;
                tuple3.f1 = 0L;
                tuple3.f2 = 0L;
                statisticsState.update(tuple3);
                System.out.println(" 数据超时清除 ");
            } else {

                if (batchSizeMap.get(taskId) == -1 || batchSizeMap.get(taskId) == 0) {
                    System.out.println(
                            taskId
                                    + " -- 定时器 batchSize 查询 "
                                    + DateUtil.toDate(System.currentTimeMillis()));
                    logger.info(" 定时器查询批次 batchSize -> " + taskId);
                    // Query the Oracle database again to get the batch size
                    long batchSize3 =
                            getBatchSizeFromOracle(
                                    taskId, proMgtOrgCodeMap.get(taskId), connection);
                    if (batchSize3 == -1) {
                        tuple3.f0 = batchSize3;
                        batchSize = tuple3.f0;
                        batchSizeMap.put(taskId, batchSize3);
                        System.out.println(
                                " 定时查询 batchSize 仍为 -1 注册一分钟定时器 "
                                        + taskId
                                        + " -- "
                                        + DateUtil.toDate(System.currentTimeMillis()));
                        timestamps = System.currentTimeMillis() + 60 * 1000;
                        context.timerService().registerProcessingTimeTimer(timestamps);
                    } else {
                        batchSize = batchSize3;
                        tuple3.f0 = batchSize;
                        System.out.println(" 定时器查询 batchSize 完成 " + batchSize3 + " --- " + taskId);
                        ArrayList<JSONObject> arrayList = bufferMap.get(taskId);
                        batchSizeMap.put(taskId, batchSize3);
                        System.out.println(" 定时器 comparison ");
                        comparison(
                                context,
                                out,
                                receType,
                                tableCode,
                                arrayList,
                                batchSizeMap.get(taskId));
                    }
                } else {
                    if (bufferMap.get(taskId) != null) {
                        ArrayList<JSONObject> arrayList = bufferMap.get(taskId);
                        comparison(
                                context,
                                out,
                                receType,
                                tableCode,
                                arrayList,
                                batchSizeMap.get(taskId));
                    }
                }
            }

        } catch (Exception e) {
            logger.error("taskId {} error -> {}", context.getCurrentKey(), e.getMessage());
        }
    }

    private static boolean isBatchComplete(
            ArrayList<JSONObject> sum, long batchSize, String taskId) {
        int size = 0;
        try {
            size = sum.size();
            if (size % 1000 == 0) {
                System.out.println(" size  " + size + " - " + batchSize + " - " + taskId);
            }
        } catch (Exception e) {
            logger.error(" error -> {}, {}", e.getMessage(), sum);
        }
        //        if (size > batchSize && batchSize != -1L && batchSize != 0) {
        //            System.out.println(" 数据异常 " + taskId + " - " + size + " - " + batchSize);
        //        }
        if (size == batchSize || size > batchSize && batchSize != 0 && batchSize != -1) {
            System.out.println(" size 大小 " + size + " -- " + batchSize);
        }
        return size >= batchSize && batchSize != 0 && batchSize != -1;
    }

    private static long getBatchSizeFromOracle(
            String taskId, String proMgtOrgCode, Connection connection) throws Exception {
        String dataNumSql =
                "select TAC_DATA_NUM from SGAMI_HEAD_OPERATION.A_DG_ORG_TASK_MONITOR where TASK_ID = ? and PRO_MGT_ORG_CODE = ?";
        try (PreparedStatement statement = connection.prepareStatement(dataNumSql)) {
            statement.setString(1, taskId);
            statement.setString(2, proMgtOrgCode);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    String tacDataNum = resultSet.getString("TAC_DATA_NUM");
                    if (tacDataNum != null) {
                        return Long.parseLong(tacDataNum);
                    }
                }
            }
        }
        logger.warn("The result of this tacDataNum query failed and was null!!!");
        return -1;
    }

    @Override
    public void close() {

        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            logger.error(" close SQLException -> {}", e.getMessage());
        }
    }

    private static void delTableData(
            String proMgtOrgCode, String tableCode, String receType, Connection connection) {
        switch (receType) {
            case "01":
                String delSql01 = "delete from " + tableCode + " where PRO_MGT_ORG_CODE = ?";
                try {
                    PreparedStatement statement = connection.prepareStatement(delSql01);
                    statement.setString(1, proMgtOrgCode);
                    statement.executeUpdate();
                } catch (SQLException e) {
                    logger.error(" delTableData SQLException -> {}", e.getMessage());
                }
                break;
            case "02":
                String[] split = tableCode.split("\\.");
                String delColumSql =
                        "select DEL_INFO from SGAMI_SUPPORT.S_TABLE_LISTENER where ORG_CODE = ? and USER_NAME = ? and TAB_NAME = ?";
                try {
                    PreparedStatement statement = connection.prepareStatement(delColumSql);
                    statement.setString(1, proMgtOrgCode);
                    statement.setString(2, split[0]);
                    statement.setString(3, split[1]);
                    ResultSet resultSet = statement.executeQuery();
                    if (resultSet.next()) {
                        String delInfo = resultSet.getString("DEL_INFO");
                        if (delInfo != null) {
                            String delSql02 =
                                    "delete from "
                                            + tableCode
                                            + " where "
                                            + delInfo
                                            + " and PRO_MGT_ORG_CODE = ?";
                            PreparedStatement delStatement = connection.prepareStatement(delSql02);
                            delStatement.setString(1, proMgtOrgCode);
                            delStatement.executeUpdate();

                        } else {
                            logger.error("{} delInfo is null", proMgtOrgCode);
                            System.out.println("delInfo is null");
                        }
                    } else {
                        logger.error("delInfo is null");
                    }
                } catch (SQLException e) {
                    logger.error(" delColumSql SQLException -> {} ", e.getMessage());
                }
                break;
            default:
                logger.warn(" Invalid receType: {}", receType);
                break;
        }
    }
}
