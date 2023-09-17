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

package io.sophiadata.flink.sync.util;

import org.apache.flink.api.java.utils.ParameterTool;

import io.sophiadata.flink.sync.common.Constants;

/** (@SophiaData) (@date 2023/5/31 19:05). */
public class ParameterUtil {
    // 这里你也可以使用 nacos 等工具来进行配置的私有化
    public static String sinkUrl(ParameterTool params) {
        return params.get("sinkUrl", Constants.sinkUrl);
    }

    public static String sinkUsername(ParameterTool params) {
        return params.get("sinkUsername", Constants.sinkUsername);
    }

    public static String sinkPassword(ParameterTool params) {
        return params.get("sinkPassword", Constants.sinkPassword);
    }

    public static String hostname(ParameterTool params) {
        return params.get("hostname", Constants.hostname);
    }

    public static Integer port(ParameterTool params) {
        return params.getInt("port", Constants.port);
    }

    public static String username(ParameterTool params) {
        return params.get("username", Constants.username);
    }

    public static String password(ParameterTool params) {
        return params.get("password", Constants.password);
    }

    public static String databaseName(ParameterTool params) {
        return params.get("databaseName", Constants.databaseName);
    }

    public static String tableList(ParameterTool params) {
        return params.get("tableList", Constants.tableList);
    }

    public static Integer setParallelism(ParameterTool params) {
        return params.getInt("setParallelism", Constants.setParallelism);
    }

    public static String cdcSourceName(ParameterTool params) {
        return params.get("cdcSourceName", Constants.cdcSourceName);
    }
}
