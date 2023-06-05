/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sophiadata.flink.sync.source;

/** (@SophiaData) (@date 2023/3/8 21:41). */
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Executor;

public class MyNacosSource extends RichSourceFunction<String> {
    // 本项目未使用，仅做测试

    private static final Logger LOG = LoggerFactory.getLogger(MyNacosSource.class);

    Properties properties = new Properties();
    ConfigService configService;
    String config;

    String serverAddr = "localhost";
    String dataId = "flink_test";
    String group = "DEFAULT_GROUP";

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
                sourceContext.collect("监听时间: " + System.currentTimeMillis());
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
