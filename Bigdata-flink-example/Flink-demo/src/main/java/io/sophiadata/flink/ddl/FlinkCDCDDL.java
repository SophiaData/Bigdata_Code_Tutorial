package io.sophiadata.flink.ddl;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import io.sophiadata.flink.base.BaseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.flink.shaded.guava30.com.google.common.base.Preconditions.checkNotNull;

/** (@SophiaData) (@date 2022/11/13 10:20). */
public class FlinkCDCDDL extends BaseCode {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkCDCDDL.class);

    public static void main(String[] args) {
        // 测试解析 mysql schema change
        // true 为 更新，插入语句 false 为删除语句
        new FlinkCDCDDL().init(args, "flink_cdc_ddl_job_test", true, true);
        LOG.info(" init 方法正常 ");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, ParameterTool params) {

        String hostname = checkNotNull(params.get("hostname"));
        int port = checkNotNull(params.getInt("port"));
        String username = checkNotNull(params.get("username"));
        String password = checkNotNull(params.get("password"));
        String databaseList = checkNotNull(params.get("databaseList"));
        String tableList = checkNotNull(params.get("tableList"));

        Properties properties = new Properties();
        // decimal 设置为 string 避免转换异常
        properties.put("decimal.handling.mode", "string");

        MySqlSource<Tuple2<Boolean, String>> sourceFunction =
                MySqlSource.<Tuple2<Boolean, String>>builder()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .databaseList(databaseList)
                        .tableList(tableList)
                        .deserializer(new JsonStringDebeziumDeserializationSchema())
                        .includeSchemaChanges(true)
                        // 由于发生了 schema change 2.3 新增的 earliest，specificOffset，timestamp 可能不可用
                        // 详情参照 flink cdc 官网启动模式章节
                        .startupOptions(StartupOptions.initial())
                        .debeziumProperties(properties)
                        .build();

        DataStreamSource<Tuple2<Boolean, String>> mysql =
                env.fromSource(sourceFunction, WatermarkStrategy.noWatermarks(), "mysql")
                        .setParallelism(1);

        mysql.print().setParallelism(1);
    }
}
