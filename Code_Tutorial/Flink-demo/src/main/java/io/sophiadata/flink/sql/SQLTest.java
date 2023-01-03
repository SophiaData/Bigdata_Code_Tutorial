package io.sophiadata.flink.sql;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import io.sophiadata.flink.base.BaseSql;

/**
 * (@gtk) (@date 2022/12/22 18:35).
 */
public class SQLTest extends BaseSql {
    public static void main(String[] args) throws Exception {
        new SQLTest().init(args,"SQLTest");
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, ParameterTool params) throws Exception {
         tEnv.executeSql("CREATE TABLE test2 (\n" +
                "  id STRING,\n" +
                "  op STRING,\n" +
                "  sex STRING,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/test',\n" +
                "   'table-name' = 'test2',\n" +
                 " 'username' = 'root'," +
                 "'password' = '123456'" +
                ");");

        Table table = tEnv.sqlQuery("select * from test2");

        table.execute().print();
    }
}
