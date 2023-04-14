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
package io.sophiadata.flink.sync2.sink;

import org.apache.flink.api.java.utils.ParameterTool;

import com.alibaba.nacos.api.exception.NacosException;
import io.sophiadata.flink.sync2.utils.NacosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/** (@SophiaData) (@date 2023/4/11 17:23). */
public class CreateMySQLSinkTable {
    private static final Logger LOG = LoggerFactory.getLogger(CreateMySQLSinkTable.class);

    public static String connectorWithBody(ParameterTool params)
            throws IOException, NacosException {
        Properties mysqlConfig =
                NacosUtil.getFromNacosConfig("new-sync-mysql", params, "DEFAULT_GROUP");
        String connectorWithBody =
                " with (\n"
                        + " 'connector' = '${sinkType}',\n"
                        + " 'url' = '${sinkUrl}',\n"
                        + " 'username' = '${sinkUsername}',\n"
                        + " 'password' = '${sinkPassword}',\n"
                        + " 'table-name' = '${sinkTableName}',\n"
                        + " 'sink.buffer-flush.max-rows' = '1000',\n"
                        + " 'sink.buffer-flush.interval' = '3s',\n"
                        + " 'sink.parallelism' = '1'\n"
                        + ")";

        connectorWithBody =
                connectorWithBody
                        .replace("${sinkType}", "jdbc")
                        .replace("${sinkUrl}", mysqlConfig.get("sinkMysqlUrl").toString())
                        .replace("${sinkUsername}", mysqlConfig.get("sinkMysqlUsername").toString())
                        .replace(
                                "${sinkPassword}", mysqlConfig.get("sinkMysqlPassword").toString());

        return connectorWithBody;
    }
}
