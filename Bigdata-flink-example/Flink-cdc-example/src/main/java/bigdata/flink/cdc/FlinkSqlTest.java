package bigdata.flink.cdc;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** (@SophiaData) (@date 2022/10/25 10:56). */
public class FlinkSqlTest extends BaseSql {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlTest.class);

    public static void main(String[] args) {
        new FlinkSqlTest().init("flink_sql_test_job", args, "hashMap");
        LOG.info(" 方法初始化完成 ");
    }

    @Override
    protected void handle(
            StreamExecutionEnvironment env, StreamTableEnvironment tEnv, ParameterTool params) {
        tEnv.executeSql(
                "CREATE TABLE mysql_binlog (\n"
                        + " id INT NOT NULL,\n"
                        + " student STRING,\n"
                        + " sex STRING,\n"
                        + " PRIMARY KEY(id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + " 'connector' = 'mysql-cdc',\n"
                        + " 'hostname' = '"
                        + params.get("hostname")
                        + "' ,\n"
                        + " 'port' = '"
                        + params.getInt("port")
                        + "',\n"
                        + " 'username' = '"
                        + params.get("username")
                        + "',\n"
                        + " 'password' = '"
                        + params.get("password")
                        + "',\n"
                        + " 'database-name' = '"
                        + params.get("databaseName")
                        + "',\n"
                        + " 'table-name' = '"
                        + params.get("tableName")
                        + "' \n"
                        + ")");

        Table sqlQuery = tEnv.sqlQuery("select id, student, sex from mysql_binlog");
        tEnv.toChangelogStream(sqlQuery).print();
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(String.format(" 运行异常 %s", e));
        }
    }
}
