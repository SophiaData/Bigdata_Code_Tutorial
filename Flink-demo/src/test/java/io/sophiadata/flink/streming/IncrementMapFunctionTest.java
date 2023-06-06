package io.sophiadata.flink.streming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.*;

public class IncrementMapFunctionTest {

    @Test
    public void testIncrementMapFunction() throws Exception {
        // 创建测试数据
        List<Long> testData = new ArrayList<>();
        testData.add(1L);
        testData.add(2L);

        // 设置 StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建数据源
        DataStreamSource<Long> testDataStream = env.fromCollection(testData);

        // 应用 MapFunction
        SingleOutputStreamOperator<Long> map = testDataStream.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value + 1L;
            }
        });

        // 添加自定义的测试 Sink
        map.addSink(new TestSink()).setParallelism(1);

        // 执行测试
        env.execute();

        // 验证结果
        List<Long> expectedOutput = new ArrayList<>();
        expectedOutput.add(2L);
        expectedOutput.add(3L);

        assertEquals(expectedOutput, TestSink.OUTPUT);
    }

    // 自定义测试 Sink
    public static class TestSink implements SinkFunction<Long> {
        public static final List<Long> OUTPUT = new ArrayList<>();

        @Override
        public void invoke(Long value, Context context) throws Exception {
            OUTPUT.add(value);
        }
    }
}