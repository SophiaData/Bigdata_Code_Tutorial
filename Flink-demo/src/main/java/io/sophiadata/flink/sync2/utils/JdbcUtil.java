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
package io.sophiadata.flink.sync2.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.shaded.guava30.com.google.common.base.CaseFormat;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.exception.NacosException;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/** (@SophiaData) (@date 2023/4/11 17:10). */
public class JdbcUtil {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtil.class);

    private static final BasicDataSource dataSource = new BasicDataSource();

    private static final String DRIVER_NAME = "com.mysql.cj.jdbc.Driver";

    public static BasicDataSource getOracleConnection(ParameterTool params)
            throws IOException, NacosException {
        Properties oracleConfig =
                NacosUtil.getFromNacosConfig("new-sync-oracle", params, "DEFAULT_GROUP");

        try {
            dataSource.setDriverClassName("oracle.jdbc.OracleDriver");
            dataSource.setUrl(oracleConfig.get("sinkOracleUrl").toString());
            dataSource.setUsername(oracleConfig.get("sinkOracleUsername").toString());
            dataSource.setPassword(oracleConfig.get("sinkOraclePassword").toString());
            dataSource.setInitialSize(1);
            dataSource.setMaxTotal(50);
            dataSource.setMaxWaitMillis(10 * 1000);
            dataSource.setMinEvictableIdleTimeMillis(30 * 60 * 1000);

        } catch (Exception e) {
            LOG.error(String.format("建立连接错误, 原因 %s", e));
        }

        return dataSource;
    }

    public static Connection getMysqlConnection(ParameterTool params)
            throws ClassNotFoundException, SQLException, IOException, NacosException {
        Properties mysqlConfig =
                NacosUtil.getFromNacosConfig("new-sync-mysql", params, "DEFAULT_GROUP");
        Connection connection;
        try {
            Class.forName(DRIVER_NAME);
            connection =
                    DriverManager.getConnection(
                            mysqlConfig.get("sinkMysqlUrl").toString(),
                            mysqlConfig.get("sinkMysqlUsername").toString(),
                            mysqlConfig.get("sinkMysqlPassword").toString());
        } catch (ClassNotFoundException e) {
            LOG.error("驱动未加载，请检查: ", e);
            throw e;
        } catch (SQLException e) {
            LOG.error("sql 异常: ", e);
            throw e;
        }
        return connection;
    }

    public static <T> List<T> queryResult(
            Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel)
            throws Exception {

        // 创建集合用于存放查询结果
        ArrayList<T> result = new ArrayList<>();

        // 编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        // 执行查询
        ResultSet resultSet = preparedStatement.executeQuery();

        // 获取列名信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        // 遍历resultSet,将每行查询到的数据封装为  T  对象
        while (resultSet.next()) {

            // 构建T对象
            T t = clz.newInstance();

            for (int i = 1; i < columnCount + 1; i++) {

                String columnName = metaData.getColumnName(i);
                Object value = resultSet.getObject(columnName);

                if (underScoreToCamel) {
                    columnName =
                            CaseFormat.LOWER_UNDERSCORE.to(
                                    CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                // 给T对象进行属性赋值
                BeanUtils.setProperty(t, columnName, value);
            }

            // 将T对象添加至集合
            result.add(t);
        }

        // 关闭资源对象
        resultSet.close();
        preparedStatement.close();

        // 返回结果
        return result;
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        Connection connection = JdbcUtil.getMysqlConnection(params);
        String sql = "select * from SGAMI_STAT.A_BA_IND_ENERGY_DAY";
        connection.prepareStatement(sql);

        List<JSONObject> jsonObjects = queryResult(connection, sql, JSONObject.class, false);

        System.out.println(jsonObjects);
        connection.close();
    }
}
