package io.sophiadata.flink.doris;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import io.sophiadata.flink.base.BaseSql;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.flink.shaded.guava30.com.google.common.base.Preconditions.checkNotNull;

/** (@SophiaData) (@date 2022/10/25 10:56). */
public class FlinkToDoris extends BaseSql {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkToDoris.class);

    public static void main(String[] args) {
        new FlinkToDoris().init(args, "flink_doris_test", true, true);
        LOG.info(" init 方法正常 ");
    }

    @Override
    public void handle(
            StreamExecutionEnvironment env, StreamTableEnvironment tEnv, ParameterTool params) {
        String hostname = checkNotNull(params.get("hostname"));
        int port = checkNotNull(params.getInt("port"));
        String username = checkNotNull(params.get("username"));
        String password = checkNotNull(params.get("password"));
        String databaseList = checkNotNull(params.get("databaseName"));
        String tableList = checkNotNull(params.get("tableName"));

        // 这是一个未经测试的伪代码
        Properties properties = new Properties();
        // decimal 设置为 string 避免转换异常
        properties.put("decimal.handling.mode", "string");

        MySqlSource<String> sourceFunction =
                MySqlSource.<String>builder()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .databaseList(databaseList)
                        .tableList(tableList)
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true) // 接收 ddl
                        .startupOptions(StartupOptions.initial())
                        .debeziumProperties(properties)
                        .build();

        DataStreamSource<String> mysql =
                env.fromSource(sourceFunction, WatermarkStrategy.noWatermarks(), "mysql")
                        .setParallelism(1);

        mysql.print().setParallelism(1);

        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes("***")
                .setTableIdentifier("***")
                .setUsername("***")
                .setPassword("***");

        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-doris");

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new SimpleStringSerializer()) // serialize according to string
                .setDorisOptions(dorisBuilder.build());

        mysql.sinkTo(builder.build());
    }
}
