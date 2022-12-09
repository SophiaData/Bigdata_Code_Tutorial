package io.sophiadata.flink.cdc2.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import io.sophiadata.flink.cdc2.table.CustomDebeziumDeserializer;
import io.sophiadata.flink.cdc2.util.ParameterUtil;

import java.util.Map;

/** (@SophiaData) (@date 2022/12/9 14:47). */
public class MySQLCDCSource {

    public SingleOutputStreamOperator<Tuple2<String, Row>> singleOutputStreamOperator(
            ParameterTool params,
            StreamExecutionEnvironment env,
            Map<String, RowType> tableRowTypeMap) {

        MySqlSource<Tuple2<String, Row>> mySqlSource =
                MySqlSource.<Tuple2<String, Row>>builder()
                        .hostname(ParameterUtil.hostname(params))
                        .port(ParameterUtil.port(params))
                        .databaseList(ParameterUtil.databaseName(params))
                        .tableList(ParameterUtil.tableList(params))
                        .username(ParameterUtil.username(params))
                        .password(ParameterUtil.password(params))
                        .deserializer(new CustomDebeziumDeserializer(tableRowTypeMap))
                        .startupOptions(StartupOptions.initial())
                        .build();

        return env.fromSource(
                        mySqlSource,
                        WatermarkStrategy.noWatermarks(),
                        params.get("sourceName", "mysql cdc"))
                .disableChaining()
                .setParallelism(params.getInt("parallelism", 2));
    }
}
