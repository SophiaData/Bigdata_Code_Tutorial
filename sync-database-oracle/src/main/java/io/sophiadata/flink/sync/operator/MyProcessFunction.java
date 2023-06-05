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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.alibaba.fastjson.JSONObject;
import io.sophiadata.flink.sync.Constant;
import io.sophiadata.flink.sync.utils.JdbcUtil;
import lombok.Cleanup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** (@SophiaData) (@date 2023/5/4 20:29). */
public class MyProcessFunction extends ProcessFunction<JSONObject, JSONObject> {

    private static final Logger LOG = LoggerFactory.getLogger(MyProcessFunction.class);

    private final ParameterTool params;
    private final OutputTag<JSONObject> hbaseOutputTag;

    private final OutputTag<JSONObject> receType3OutputTag;

    private transient HashMap<String, String> sinkDatabaseType;

    private Connection connection;

    private static final String HBASE = "HBASE";

    private static final String ORACLE = "ORACLE";

    private static final String TABLE_CODE = "TABLE_CODE";

    private static final String DB_NAME = "DB_NAME";

    public MyProcessFunction(
            ParameterTool params,
            OutputTag<JSONObject> hbaseOutputTag,
            OutputTag<JSONObject> receType3OutputTag) {
        this.params = params;
        this.hbaseOutputTag = hbaseOutputTag;
        this.receType3OutputTag = receType3OutputTag;
    }

    @Override
    public void open(Configuration parameters) {
        try {
            connection = JdbcUtil.getOracleConnection(params).getConnection();
        } catch (SQLException e) {
            LOG.error("SQLException -> {} ", e.getMessage());
        }
    }

    @Override
    public void processElement(
            JSONObject value,
            ProcessFunction<JSONObject, JSONObject>.Context ctx,
            Collector<JSONObject> out) {
        String dbName = null;
        try {
            if (sinkDatabaseType == null) {
                sinkDatabaseType = new HashMap<>();
            }
            String tableCode = value.getString(TABLE_CODE);

            if (!sinkDatabaseType.containsKey(tableCode)) {
                sinkDatabaseType.put(tableCode, getDbName(tableCode, value));
            }
            dbName = sinkDatabaseType.get(tableCode);
            String receType = value.getString(Constant.RECE_TYPE);

            if (dbName.equals(HBASE)) {
                // hbaseOutputTag 分流
                // ctx.output(hbaseOutputTag,value);
                LOG.warn(" 暂不支持 ");
            } else if (dbName.equals(ORACLE)) {
                final String RECE_TYPE1 = "01";
                final String RECE_TYPE2 = "02";
                final String RECE_TYPE3 = "03";
                switch (receType) {
                    case "00":
                        LOG.warn(" 数据入库类型不正确 ");
                        break;
                    case RECE_TYPE1:
                    case RECE_TYPE2:
                    case RECE_TYPE3:
                        out.collect(value);
                        break;
                }

                //                LOG.info(" oracle stream ");
            }
        } catch (Exception e) {
            LOG.error(
                    " sinkDatabaseType error -> {}, error -> {}, data -> {} ",
                    dbName,
                    e.getMessage(),
                    value);
        }
    }

    private String getDbName(String tableCode, JSONObject value) {
        LOG.info(" tableCode -> " + tableCode);

        String[] split = new String[0];
        try {
            split = tableCode.split("\\.");
        } catch (Exception e) {
            LOG.error(" split -> {} data -> {} reason {}", split, value, e.getMessage());
        }

        String tableSql =
                " select DB_NAME from SGAMI_HEAD_OPERATION.A_BMDM_ENTITY_INFO where TABLE_CODE = ? and ACCOUNT_ID = ?";
        List<JSONObject> queryResult1 = new ArrayList<>();
        try {
            @Cleanup PreparedStatement statement1 = connection.prepareStatement(tableSql);
            statement1.setString(1, split[1]);
            statement1.setString(2, split[0]);
            ResultSet resultSet1 = statement1.executeQuery();
            while (resultSet1.next()) {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put(DB_NAME, resultSet1.getString(DB_NAME));
                System.out.println(" put db_name success ");
                queryResult1.add(jsonObject);
            }

        } catch (Exception e) {
            LOG.error(" exception -> {} ", e.getMessage());
        }

        try {
            if (!queryResult1.isEmpty()) {
                String dbName = queryResult1.get(0).getString(DB_NAME);
                if (dbName != null) {
                    sinkDatabaseType.put(tableCode, dbName);
                    return dbName;
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        LOG.error(" dbName is null ");
        return null;
    }
}
