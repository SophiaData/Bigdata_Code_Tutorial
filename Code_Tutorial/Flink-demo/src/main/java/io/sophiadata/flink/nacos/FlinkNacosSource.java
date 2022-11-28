package io.sophiadata.flink.nacos;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Executor;

/** (@SophiaData) (@date 2022/11/27 21:32). */
public class FlinkNacosSource extends RichSourceFunction<String> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkNacosSource.class);

    Properties properties = new Properties();
    ConfigService configService;
    String config;

    String serverAddr = "localhost";
    String dataId = "data1";
    String group = "group_1";

    @Override
    public void open(Configuration parameters) {
        try {
            super.open(parameters);

            properties.put("serverAddr", serverAddr);
            configService = NacosFactory.createConfigService(properties);
            config = configService.getConfig(dataId, group, 5000);

            configService.addListener(
                    dataId,
                    group,
                    new Listener() {
                        @Override
                        public Executor getExecutor() {
                            return null;
                        }

                        @Override
                        public void receiveConfigInfo(String msg) {
                            config = msg;
                            System.out.println("开启监听器" + msg);
                        }
                    });
        } catch (Exception e) {
            LOG.error("open 方法异常： " + e);
        }
    }

    @Override
    public void run(SourceContext<String> sourceContext) {
        try {
            while (true) {
                Thread.sleep(500);
                System.out.println("获得配置信息: " + config);
                sourceContext.collect(String.valueOf(System.currentTimeMillis()));
            }
        } catch (Exception e) {
            LOG.error("线程异常" + e);
        }
    }

    @Override
    public void cancel() {
        // 集成报警信息
    }
}
