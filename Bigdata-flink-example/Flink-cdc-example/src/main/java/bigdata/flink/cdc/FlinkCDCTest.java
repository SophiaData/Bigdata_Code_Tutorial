package bigdata.flink.cdc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

import java.util.Properties;

import static org.apache.curator.shaded.com.google.common.base.Preconditions.checkNotNull;

/**
 * (@xxx) (@date 2022/5/7 14:17).
 */
public class FlinkCDCTest {
    private static String hostname;
    private static int port;
    private static String username;
    private static String password;
    private static String databaseList;
    private static String tableList;

    public static void main(String[] args) throws Exception {
        // 参数信息通过 args 传递
        final ParameterTool params = ParameterTool.fromArgs(args);
        try {
            hostname = checkNotNull(params.get("hostname"));
            port = checkNotNull(params.getInt("port"));
            username = checkNotNull(params.get("username"));
            password = checkNotNull(params.get("password"));
            databaseList = checkNotNull(params.get("databaseList"));
            tableList = checkNotNull(params.get("tableList"));
        } catch (Exception e) {
           throw  new RuntimeException(
                    String.format("传参失败，当前已接受传参: \n"  +
                            "--hostname %s \n" +
                            "--port %s \n" +
                            "--username %s \n" +
                            "--password %s \n" +
                            "--databaseList %s \n" +
                            "--tableList %s \n" +
                            "异常原因: %s",
                            hostname,
                            port,
                            username,
                            password,
                            databaseList,
                            tableList,
                            e)
            );
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        // decimal 设置为 string 避免转换异常
        properties.put("decimal.handling.mode", "string");
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/flink/FlinkCDCTest");
        env.enableCheckpointing(30 * 1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

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
                env.fromSource(sourceFunction, WatermarkStrategy.noWatermarks(), "mysql");

        mysql.print().setParallelism(1);

        env.execute();
    }
}
