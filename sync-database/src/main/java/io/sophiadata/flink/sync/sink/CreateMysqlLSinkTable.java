/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.flink.table.types.DataType;

import io.sophiadata.flink.sync.util.MysqlUtil;
import io.sophiadata.flink.sync.util.ParameterUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/** (@SophiaData) (@date 2023/5/31 19:08). */
public class CreateMysqlLSinkTable {

    public void createMysqlSinkTable(
            ParameterTool params,
            String sinkTableName,
            String[] fieldNames,
            DataType[] fieldDataTypes,
            List<String> primaryKeys)
            throws SQLException, ClassNotFoundException {
        String createSql =
                MysqlUtil.createTable(sinkTableName, fieldNames, fieldDataTypes, primaryKeys);
        Connection connection =
                MysqlUtil.getConnection(
                        ParameterUtil.sinkUrl(params),
                        ParameterUtil.sinkUsername(params),
                        ParameterUtil.sinkPassword(params));
        MysqlUtil.executeSql(connection, createSql);
    }

    public static String connectorWithBody(ParameterTool params) {
        String connectorWithBody =
                " with (\n"
                        + " 'connector' = '${sinkType}',\n"
                        + " 'url' = '${sinkUrl}',\n"
                        + " 'username' = '${sinkUsername}',\n"
                        + " 'password' = '${sinkPassword}',\n"
                        + " 'table-name' = '${sinkTableName}'\n"
                        + ")";

        connectorWithBody =
                connectorWithBody
                        .replace("${sinkType}", "jdbc")
                        .replace("${sinkUrl}", ParameterUtil.sinkUrl(params))
                        .replace("${sinkUsername}", ParameterUtil.sinkUsername(params))
                        .replace("${sinkPassword}", ParameterUtil.sinkPassword(params));

        return connectorWithBody;
    }
}
