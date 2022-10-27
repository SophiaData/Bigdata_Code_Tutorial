package bigdata.flink.base;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** (@SophiaData) (@date 2022/10/27 13:26). */
public abstract class BaseCode implements BaseInit {
    @Override
    public void init(String[] args, String ckPathAndJobId, Boolean hashMap, Boolean local) {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (local) {
            return;
        } else {
            checkpoint(env, ckPathAndJobId, hashMap);
        }
        restartTask(env);

        handle(env, params);

        try {
            env.execute(ckPathAndJobId); // 传入一个job的名字
        } catch (Exception e) {
            throw new RuntimeException(String.format("任务运行异常，异常原因: %s", e));
        }
    }

    public abstract void handle(StreamExecutionEnvironment env, ParameterTool params);

    @Override
    public void checkpoint(StreamExecutionEnvironment env, String ckPathAndJobId, Boolean hashMap) {
        if (hashMap) {
            env.setStateBackend(new HashMapStateBackend());
        } else {
            env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        }
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/flink/" + ckPathAndJobId);
        env.enableCheckpointing(60 * 1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(3 * 60 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    @Override
    public void restartTask(StreamExecutionEnvironment env) {
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(10)));
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(10, Time.seconds(100), Time.seconds(500)));
    }
}
