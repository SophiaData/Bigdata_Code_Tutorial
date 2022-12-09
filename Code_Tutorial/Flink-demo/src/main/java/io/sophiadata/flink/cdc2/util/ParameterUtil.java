package io.sophiadata.flink.cdc2.util;

import org.apache.flink.api.java.utils.ParameterTool;

/** (@SophiaData) (@date 2022/12/9 13:50). */
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
}
