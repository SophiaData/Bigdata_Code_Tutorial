package io.sophiadata.flink.streming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import io.sophiadata.flink.base.BaseCode;

/** (@SophiaData) (@date 2022/10/29 19:12). */
public class Sideout extends BaseCode {

    public static void main(String[] args) throws Exception {
        //
        new Sideout().init(args, "sideout");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, ParameterTool params) {
        env.setParallelism(1);
        SingleOutputStreamOperator<String> sideout =
                env.fromElements(STREAM)
                        .flatMap(
                                (FlatMapFunction<String, String>)
                                        (value, out) -> {
                                            String[] splits = value.toLowerCase().split(",");
                                            for (String split : splits) {
                                                if (split.length() > 0) {
                                                    out.collect(split);
                                                }
                                            }
                                        })
                        .process(
                                new ProcessFunction<String, String>() {
                                    @Override
                                    public void processElement(
                                            String value,
                                            ProcessFunction<String, String>.Context ctx,
                                            Collector<String> out) {
                                        if (value.equals("hello")) {
                                            ctx.output(new OutputTag<String>("hello") {}, value);
                                        } else {
                                            out.collect(value);
                                        }
                                    }
                                })
                        .name("sideout");

        sideout.print("stream >>>  ").name("stream");
        sideout.getSideOutput(new OutputTag<String>("hello") {})
                .print(" hello stream >>> ")
                .name("hello");
    }

    private static final String[] STREAM =
            new String[] {"hello,nihao,nihao,world,bigdata,hadoop,hive,hive,hello,big"};
}
