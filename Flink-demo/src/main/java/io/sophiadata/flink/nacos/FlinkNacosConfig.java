/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sophiadata.flink.nacos;

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

    public static void main(String[] args) throws Exception {
        new FlinkNacosConfig().init(args, "FlinkNacosConfig");
    }

    @Override
    public void handle(String[] args, StreamExecutionEnvironment env) {
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
