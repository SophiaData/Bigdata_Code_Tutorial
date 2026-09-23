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

import io.sophiadata.flink.compat.ParameterTool;
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

    /**
     * Sink table name pattern. Must contain {@code %s}, which is replaced with the source table
     * name. The same value is used when creating the sink table and when inserting into it.
     */
    public static String sinkPrefix(ParameterTool params) {
        String prefix = params.get("sinkPrefix", Constants.sinkPrefix);
        if (!prefix.contains("%s")) {
            throw new IllegalArgumentException(
                    "sinkPrefix must contain '%s' as the table-name placeholder, but was: "
                            + prefix);
        }
        return prefix;
    }

    /**
     * Normalises a table list into the {@code database.table} form the CDC connector requires.
     *
     * <p>A bare table name is qualified with the database, {@code .*} becomes {@code database.*},
     * and already-qualified entries pass through unchanged.
     *
     * @param databaseName the source database
     * @param tableList comma-separated table list, or {@code .*}
     * @return a connector-ready table list
     */
    public static String normalizeTableList(String databaseName, String tableList) {
        if (tableList == null || tableList.trim().isEmpty() || ".*".equals(tableList.trim())) {
            return databaseName + ".*";
        }
        StringBuilder sb = new StringBuilder();
        for (String raw : tableList.split(",")) {
            String table = raw.trim();
            if (table.isEmpty()) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(table.contains(".") ? table : databaseName + "." + table);
        }
        return sb.length() == 0 ? databaseName + ".*" : sb.toString();
    }
}
