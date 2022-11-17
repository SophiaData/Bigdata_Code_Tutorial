package io.sophiadata.flink.cdc;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import io.sophiadata.flink.base.BaseSql;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** (@SophiaData) (@date 2022/10/25 10:56). */
public class FlinkSqlTest extends BaseSql {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlTest.class);

    public static void main(String[] args) {
        new FlinkSqlTest().init(args, "flink_sql_job_test", true, true);
        LOG.info(" init 方法正常 ");
    }

    @Override
    public void handle(
            StreamExecutionEnvironment env, StreamTableEnvironment tEnv, ParameterTool params) {
        String hostname = params.get("hostname", "localhost");
        int port = params.getInt("port", 3306);
        String username = params.get("username", "root");
        String password = params.get("password", "123456");
        String databaseName = params.get("databaseName", "test");
        String tableName = params.get("tableName", "test2");

        tEnv.executeSql(
                "CREATE TABLE mysql_binlog (\n"
                        + " id INT NOT NULL,\n"
                        + " student STRING,\n"
                        + " sex STRING,\n"
                        + " PRIMARY KEY(id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + " 'connector' = 'mysql-cdc',\n"
                        + " 'hostname' = '"
                        + hostname
                        + "',\n"
                        + " 'port' = '"
                        + port
                        + "',\n"
                        + " 'username' = '"
                        + username
                        + "',\n"
                        + " 'password' = '"
                        + password
                        + "',\n"
                        + " 'database-name' = '"
                        + databaseName
                        + "',\n"
                        + " 'table-name' = '"
                        + tableName
                        + "', \n"
                        + " 'debezium.decimal.handling.mode' = 'string'," +
                        " 'scan.startup.specific-offset.file' = 'mysql-bin.000009'," +
                        "'scan.startup.specific-offset.pos' = '5767')" +
                        "");

        tEnv.executeSql(
                "CREATE TABLE mysql_binlog2 (\n"
                        + " id INT NOT NULL,\n"
                        + " student STRING,\n"
                        + " sex STRING,\n"
                        + " PRIMARY KEY(id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + " 'connector' = 'mysql-cdc',\n"
                        + " 'hostname' = '"
                        + hostname
                        + "',\n"
                        + " 'port' = '"
                        + port
                        + "',\n"
                        + " 'username' = '"
                        + username
                        + "',\n"
                        + " 'password' = '"
                        + password
                        + "',\n"
                        + " 'database-name' = '"
                        + databaseName
                        + "',\n"
                        + " 'table-name' = '"
                        + tableName
                        + "', \n"
                        + " 'debezium.decimal.handling.mode' = 'string'" +
                        " )" +
                        "");

        Table sqlQuery = tEnv.sqlQuery("select id, student, sex from mysql_binlog2");
        tEnv.toChangelogStream(sqlQuery).print();
        try {
            env.execute();
        } catch (Exception e) {
            LOG.error("异常信息输出：", e);
        }
    }
}
