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

package io.sophiadata.flink.sync.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/** (@SophiaData) (@date 2023/3/22 10:40). */
public class JdbcUtil {

    private static final String DEFAULT_GROUP = "DEFAULT_GROUP";

    private static final String NEW_SYNC_ORACLE = "new-sync-oracle";

    private static final String NEW_SYNC_MYSQL = "new-sync-mysql";

    private static final String NEW_SYNC_HBASE = "new-sync-hbase";

    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtil.class);

    private static final BasicDataSource dataSource = new BasicDataSource();

    private static final BasicDataSource dataSource2 = new BasicDataSource();

    private static final String DRIVER_NAME = "com.mysql.cj.jdbc.Driver";

    private static org.apache.hadoop.hbase.client.Connection hbaseConnection;

    public static BasicDataSource getOracleConnection(ParameterTool params) {
        try {
            Properties oracleConfig =
                    NacosUtil.getFromNacosConfig(NEW_SYNC_ORACLE, params, DEFAULT_GROUP);

            dataSource.setDriverClassName("oracle.jdbc.OracleDriver");
            dataSource.setUrl(oracleConfig.get("sinkOracleUrl").toString());
            dataSource.setUsername(oracleConfig.get("sinkOracleUsername").toString());
            dataSource.setPassword(oracleConfig.get("sinkOraclePassword").toString());
            dataSource.setInitialSize(10);
            dataSource.setMaxTotal(50);
            dataSource.setMinIdle(10);
            dataSource.setMaxWaitMillis(60 * 1000);
            dataSource.setMinEvictableIdleTimeMillis(10 * 60 * 1000);
            dataSource.setValidationQuery("SELECT 1 FROM DUAL");

        } catch (Exception e) {
            LOG.error(String.format(" oracle connection exception, reason %s", e.getMessage()));
        }

        return dataSource;
    }

    public static BasicDataSource getMysqlConnection(ParameterTool params) {
        try {
            Properties mysqlConfig =
                    NacosUtil.getFromNacosConfig(NEW_SYNC_MYSQL, params, DEFAULT_GROUP);

            dataSource2.setDriverClassName(DRIVER_NAME);
            dataSource2.setUrl(mysqlConfig.get("sinkMySQLUrl").toString());
            dataSource2.setUsername(mysqlConfig.get("sinkMySQLUsername").toString());
            dataSource2.setPassword(mysqlConfig.get("sinkMySQLPassword").toString());
            dataSource2.setInitialSize(10);
            dataSource2.setMaxTotal(50);
            dataSource2.setMinIdle(10);
            dataSource2.setMaxWaitMillis(60 * 1000);
            dataSource2.setMinEvictableIdleTimeMillis(10 * 60 * 1000);
            dataSource2.setValidationQuery("select NOW()");
        } catch (Exception e) {
            LOG.error(" mysql connection exception :{} ", e.getMessage());
        }
        return dataSource2;
    }

    public static org.apache.hadoop.hbase.client.Connection getMyHbaseConnection(
            ParameterTool params) {
        try {
            Properties hbaseConfig =
                    NacosUtil.getFromNacosConfig(NEW_SYNC_HBASE, params, DEFAULT_GROUP);
            org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
            config.set(
                    "hbase.zookeeper.property.clientPort",
                    hbaseConfig.get("hbase.sink.zookeeper.client.port").toString());
            config.set(
                    "hbase.zookeeper.quorum",
                    hbaseConfig.get("hbase.sink.zookeeper.quorum").toString());

            // 安全认证
            config.set(
                    "hadoop.security.authentication",
                    hbaseConfig.getOrDefault("hadoop.security.authentication", "").toString());
            config.set(
                    "java.security.krb5.conf",
                    hbaseConfig.getOrDefault("java.security.krb5.conf", "").toString());
            config.set(
                    "hbase.sink.user.keytab",
                    hbaseConfig.getOrDefault("hbase.sink.user.keytab.default", "").toString());
            hbaseConnection = ConnectionFactory.createConnection(config);
        } catch (Exception e) {
            LOG.error(" hbase connection exception :{}", e.getMessage());
        }
        return hbaseConnection;
    }

    public static <T> List<T> queryResult(
            Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel) {

        // 创建集合用于存放查询结果
        ArrayList<T> result = new ArrayList<>();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            // 编译SQL
            preparedStatement = connection.prepareStatement(querySql);

            // 执行查询
            resultSet = preparedStatement.executeQuery();
        } catch (SQLException e) {
            LOG.error(" queryResult exception -> {}", e.getMessage());
        }

        // 获取列名信息
        ResultSetMetaData metaData = null;
        int columnCount = 0;
        try {
            if (resultSet != null) {
                metaData = resultSet.getMetaData();
                columnCount = metaData.getColumnCount();
            } else {
                LOG.error(" resultSet is null ");
            }
        } catch (SQLException e) {
            LOG.error(" resultSet exception -> {}", e.getMessage());
        }

        // 遍历resultSet,将每行查询到的数据封装为  T  对象
        try {
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
        } catch (SQLException
                | InstantiationException
                | IllegalAccessException
                | InvocationTargetException e) {
            LOG.error(" exception -> {}", e.getMessage());
        }

        // 关闭资源对象
        try {
            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            LOG.error(" SQLException -> {}", e.getMessage());
        }

        // 返回结果
        return result;
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        Connection connection = JdbcUtil.getOracleConnection(params).getConnection();
    }
}
