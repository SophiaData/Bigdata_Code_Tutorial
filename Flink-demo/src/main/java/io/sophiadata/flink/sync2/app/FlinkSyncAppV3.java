package io.sophiadata.flink.sync2.app;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import io.sophiadata.flink.base.BaseSql;
import io.sophiadata.flink.sync2.executor.KafkaToOracle2;
import io.sophiadata.flink.sync2.executor.KafkaToOracle4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * (@SophiaData) (@date 2023/4/11 17:16).
 */
public class FlinkSyncAppV3 extends BaseSql {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSyncAppV3.class);
    private static ParameterTool params = null;

    public static void main(String[] args) throws Exception {
        params = ParameterTool.fromArgs(args);

        new FlinkSyncAppV3().init(args, params.get("ckPathAndJobId"));
        //        new FlinkSyncAppV2().init(args, params.get("ckPathAndJobId"),false,false);
        LOG.info(params.get("ckPathAndJobId") + " job 初始化正常 ！！！ ");
    }

    @Override
    public void handle(String[] args, StreamExecutionEnvironment env, StreamTableEnvironment tEnv)
            throws Exception {
        env.getConfig().setGlobalJobParameters(params);
        new KafkaToOracle4().kafkaToOracle4(env, params);
//        new KafkaToOracle2().kafkaToOracle2(args, env, tEnv,params);
    }
}
