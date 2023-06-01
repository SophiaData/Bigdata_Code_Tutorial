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
import org.apache.flink.connector.jdbc.catalog.MySqlCatalog;
import org.apache.flink.table.types.DataType;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/** (@SophiaData) (@date 2023/5/31 19:02). */
public class MysqlUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlUtil.class);
    private static final String DRIVER_NAME = "com.mysql.cj.jdbc.Driver";

    public static Connection getConnection(String sinkUrl, String sinkUsername, String sinkPassword)
            throws ClassNotFoundException, SQLException {
        Connection connection;
        try {
            Class.forName(DRIVER_NAME);
            connection = DriverManager.getConnection(sinkUrl, sinkUsername, sinkPassword);
        } catch (ClassNotFoundException e) {
            LOG.error("驱动未加载，请检查: ", e);
            throw e;
        } catch (SQLException e) {
            LOG.error("sql 异常: ", e);
            throw e;
        }
        return connection;
    }

    public static void executeSql(Connection connection, String createTable) throws SQLException {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(createTable);
            preparedStatement.execute();
        } catch (SQLException e) {
            LOG.error("建表异常: ", e);
            throw e;
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    LOG.error("sql 资源关闭异常: ", e);
                }
            }
        }
    }

    public static String createTable(
            String sinkTableName,
            String[] fieldNames,
            DataType[] fieldDataTypes,
            List<String> primaryKeys) {
        StringBuilder stmt = new StringBuilder();
        stmt.append("create table if not exists ").append(sinkTableName).append("(\n");
        for (int i = 0; i < fieldNames.length; i++) {
            String column = fieldNames[i];
            String fieldDataType = fieldDataTypes[i].toString();
            stmt.append("\t`").append(column).append("` ").append(fieldDataType).append(",\n");
        }
        stmt.append(String.format("PRIMARY KEY (%s)\n)", StringUtils.join(primaryKeys, ",")));
        String[] split = stmt.toString().split(","); // mysql timestamp 类型需要默认值设置为 1970
        StringBuilder stringBuilder = new StringBuilder();
        for (String value : split) {
            int index = value.indexOf("TIMESTAMP");
            if (index != -1) {
                stringBuilder.append(value).append(" default '1970-01-01 09:00:00',");
            } else {
                stringBuilder.append(value).append(",");
            }
        }
        int lastIndexOf = stringBuilder.lastIndexOf(",");
        stringBuilder.replace(lastIndexOf, lastIndexOf + 1, " ");

        String createSql = stringBuilder.toString();
        System.out.println(createSql);

        return createSql;
    }

    public static MySqlCatalog useMysqlCatalog(ParameterTool params) {
        return new MySqlCatalog(
                Thread.currentThread().getContextClassLoader(), // MySQL 8 驱动
                // Class.forName("com.mysql.cj.jdbc.Driver").getClassLoader()
                "mysql-catalog",
                ParameterUtil.databaseName(params),
                ParameterUtil.username(params),
                ParameterUtil.password(params),
                String.format(
                        "jdbc:mysql://%s:%d",
                        ParameterUtil.hostname(params), ParameterUtil.port(params)));
    }
}
