package bigdata.flink.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * (@xxx) (@date 2022/5/7 14:17)
 */
public class FlinkCDCTest {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.put("decimal.handling.mode", "string");
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/xxx/flink/");
        env.enableCheckpointing(10000);
        MySqlSource<String> sourceFunction =
                MySqlSource.<String>builder()
                        .hostname("localhost")
                        .port(3306)
                        .username("root")
                        .password("123456")
                        .databaseList("test")
                        .tableList("test.test2")
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
