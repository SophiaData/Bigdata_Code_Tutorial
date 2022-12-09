package io.sophiadata.flink.cdc2.sink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.types.DataType;

import io.sophiadata.flink.cdc2.util.MySQLUtil;
import io.sophiadata.flink.cdc2.util.ParameterUtil;

import java.sql.Connection;
import java.util.List;

/** (@SophiaData) (@date 2022/12/9 12:21). */
public class CreateMySQLSinkTable {

    public void createMySQLSinkTable(
            ParameterTool params,
            String sinkTableName,
            String[] fieldNames,
            DataType[] fieldDataTypes,
            List<String> primaryKeys) {
        String createSql =
                MySQLUtil.createTable(sinkTableName, fieldNames, fieldDataTypes, primaryKeys);
        Connection connection =
                MySQLUtil.getConnection(
                        ParameterUtil.sinkUrl(params),
                        ParameterUtil.sinkUsername(params),
                        ParameterUtil.sinkPassword(params));
        MySQLUtil.executeSql(connection, createSql);
    }

    public static String connectorWithBody(ParameterTool params) {
        String connectorWithBody =
                " with (\n"
                        + " 'connector' = '${sinkType}',\n"
                        + " 'url' = '${sinkUrl}',\n"
                        + " 'username' = '${sinkUsername}',\n"
                        + " 'password' = '${sinkPassword}',\n"
                        + " 'table-name' = '${sinkTableName}'\n"
                        + ")";

        connectorWithBody =
                connectorWithBody
                        .replace("${sinkType}", "jdbc")
                        .replace("${sinkUrl}", ParameterUtil.sinkUrl(params))
                        .replace("${sinkUsername}", ParameterUtil.sinkUsername(params))
                        .replace("${sinkPassword}", ParameterUtil.sinkPassword(params));

        return connectorWithBody;
    }
}
