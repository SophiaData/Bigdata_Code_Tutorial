package io.sophiadata.flink.streming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.sophiadata.flink.base.BaseCode;

import java.util.ArrayList;

/**
 * (@SophiaData) (@date 2023/6/6 14:42).
 */
public class IncrementMapFunction extends BaseCode {
    public static void main(String[] args) throws Exception {
        new IncrementMapFunction().init(args,"MapFunction");
    }

    @Override
    public void handle(String[] args, StreamExecutionEnvironment env) throws Exception {

        ArrayList<Long> testData = new ArrayList<>();
        testData.add(1L);
        testData.add(2L);
        DataStreamSource<Long> testDataStream = env.fromCollection(testData);
        SingleOutputStreamOperator<Long> map = testDataStream.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value + 1L;
            }
        });
        map.print();
        env.execute("MapFunction");

    }
}
