package io.sophiadata.flink.nacos;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import io.sophiadata.flink.base.BaseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** (@SophiaData) (@date 2022/11/27 21:22). */
public class FlinkNacosConfig extends BaseCode {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkNacosConfig.class);

    public static void main(String[] args) {
        new FlinkNacosConfig().init(args, "FlinkNacosConfig");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, ParameterTool params) {
        env.setParallelism(1);
        String serverAddr = "localhost";
        String dataId = "data1";
        String group = "group_1";

        Properties properties = new Properties();
        properties.put("serverAddr", serverAddr);
        try {
            ConfigService configService = NacosFactory.createConfigService(properties); // 服务配置中心
            String content = configService.getConfig(dataId, group, 3000); // 获取配置
            System.out.println("content: " + content); // 测试打印结果
        } catch (Exception e) {
            LOG.error("异常信息输出：", e);
        }

        env.addSource(new FlinkNacosSource()).print().setParallelism(1);
    }
}
