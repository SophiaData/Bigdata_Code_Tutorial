package io.sophiadata.flink.base;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** (@SophiaData) (@date 2022/10/25 10:58). */
public abstract class BaseSql {
    public void init(String[] args, String ckPathAndJobId, Boolean hashMap, Boolean localpath) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // set sql job name
        tEnv.getConfig().getConfiguration().setString("pipeline.name", ckPathAndJobId);

        checkpoint(env, ckPathAndJobId, hashMap, localpath);

        restartTask(env);

        handle(env, tEnv, params);
    }

    public void init(String[] args, String ckPathAndJobId) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // set sql job name
        tEnv.getConfig().getConfiguration().setString("pipeline.name", ckPathAndJobId);

        handle(env, tEnv, params);
    }

    public abstract void handle(
            StreamExecutionEnvironment env, StreamTableEnvironment tEnv, ParameterTool params);

    public void checkpoint(
            StreamExecutionEnvironment env,
            String ckPathAndJobId,
            Boolean hashMap,
            Boolean localpath) {
        if (hashMap) {
            env.setStateBackend(new HashMapStateBackend());
        } else {
            env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        }
        if (localpath) {
            env.getCheckpointConfig().setCheckpointStorage("file:///Users/flink/" + ckPathAndJobId);
        } else {
            env.getCheckpointConfig()
                    .setCheckpointStorage("hdfs://hadoop1:8020/flink/" + ckPathAndJobId);
        }
        env.enableCheckpointing(60 * 1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(3 * 60 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    public void restartTask(StreamExecutionEnvironment env) {
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(10, Time.minutes(5), Time.seconds(10)));
    }
}
