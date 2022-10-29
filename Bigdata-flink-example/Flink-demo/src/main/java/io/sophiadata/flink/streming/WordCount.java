package io.sophiadata.flink.streming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.sophiadata.flink.base.BaseCode;

/** (@SophiaData) (@date 2022/10/29 13:50). */
public class WordCount extends BaseCode {

    public static void main(String[] args) {
        //
        new WordCount().init(args, "WordCount", true, true);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, ParameterTool params) {
        env.fromElements(WORDS)
                .flatMap(
                        (FlatMapFunction<String, Tuple2<String, Integer>>)
                                (value, out) -> {
                                    String[] splits = value.toLowerCase().split(",");

                                    for (String split : splits) {
                                        if (split.length() > 0) {
                                            out.collect(new Tuple2<>(split, 1));
                                        }
                                    }
                                })
                .keyBy(value -> value.f0)
                .reduce(
                        (ReduceFunction<Tuple2<String, Integer>>)
                                (value1, value2) -> new Tuple2<>(value1.f0, value1.f1 + value2.f1))
                .print()
                .setParallelism(1)
                .name("WordCount_Print");
    }

    private static final String[] WORDS =
            new String[] {"hello,nihao,nihao,world,bigdata,hadoop,hive,hive,hello,big"};
}
