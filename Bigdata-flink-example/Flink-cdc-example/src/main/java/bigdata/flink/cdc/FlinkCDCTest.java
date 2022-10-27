package bigdata.flink.cdc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import bigdata.flink.base.BaseCode;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.curator.shaded.com.google.common.base.Preconditions.checkNotNull;

/** (@SophiaData) (@date 2022/5/7 14:17). */
public class FlinkCDCTest extends BaseCode {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkCDCTest.class);

    public static void main(String[] args) {
        // 参数信息通过 args 传递
        new FlinkCDCTest().init(args, "flink_cdc_job_test", true, false);
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

        MySqlSource<String> sourceFunction =
                MySqlSource.<String>builder()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .databaseList(databaseList)
                        .tableList(tableList)
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true)
                        .startupOptions(StartupOptions.initial())
                        .debeziumProperties(properties)
                        .build();

        DataStreamSource<String> mysql =
                env.fromSource(sourceFunction, WatermarkStrategy.noWatermarks(), "mysql")
                        .setParallelism(1);

        mysql.print().setParallelism(1);
    }
}
