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

package io.sophiadata.flink.sync.util;

import org.apache.flink.api.java.utils.ParameterTool;

/** (@SophiaData) (@date 2023/5/31 19:05). */
public class ParameterUtil {
    public static String sinkUrl(ParameterTool params) {
        return params.get(
                "sinkUrl",
                "jdbc:mysql://localhost:3306/test2?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai");
    }

    public static String sinkUsername(ParameterTool params) {
        return params.get("sinkUsername", "root");
    }

    public static String sinkPassword(ParameterTool params) {
        return params.get("sinkPassword", "123456");
    }

    public static String hostname(ParameterTool params) {
        return params.get("hostname", "localhost");
    }

    public static Integer port(ParameterTool params) {
        return params.getInt("port", 3306);
    }

    public static String username(ParameterTool params) {
        return params.get("username", "root");
    }

    public static String password(ParameterTool params) {
        return params.get("password", "123456");
    }

    public static String databaseName(ParameterTool params) {
        return params.get("databaseName", "test");
    }

    public static String tableList(ParameterTool params) {
        return params.get("tableList", ".*");
    }

    public static Integer setParallelism(ParameterTool params) {
        return params.getInt("setParallelism", 2);
    }

    public static String cdcSourceName(ParameterTool params) {
        return params.get("cdcSourceName", "mysql-cdc-1");
    }
}
