package bigdata.flink.base;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

/** (@SophiaData) (@date 2022/10/27 12:10). */
public interface BaseInit extends Serializable {

    void init(String[] args, String ckPathAndJobId, Boolean hashMap, Boolean local);

    void checkpoint(StreamExecutionEnvironment env, String ckPathAndJobId, Boolean hashMap);

    void restartTask(StreamExecutionEnvironment env);
}
