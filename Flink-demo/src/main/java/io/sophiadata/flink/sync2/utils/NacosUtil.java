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
package io.sophiadata.flink.sync2.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/** (@SophiaData) (@date 2023/4/11 17:11). */
public class NacosUtil {

    private static final Logger LOG = LoggerFactory.getLogger(NacosUtil.class);

    /**
     * @param assembly data_id
     * @param params 具体配置
     * @param group 配置组
     * @throws IOException io 异常
     * @throws NacosException nacos 异常
     */
    public static Properties getFromNacosConfig(String assembly, ParameterTool params, String group)
            throws IOException, NacosException {

        Properties properties = new Properties();

        properties.setProperty("serverAddr", params.get("nacos_server", ""));
        properties.setProperty("username", params.get("nacos_username", ""));
        properties.setProperty("password", params.get("nacos_pd", ""));
        properties.setProperty("namespace", params.get("nacos_namespace", ""));

        ConfigService service = NacosFactory.createConfigService(properties);
        String content = service.getConfig(assembly + ".properties", group, 5000L);
        //        String content2 = service.getConfig(assembly + ".yaml", group, 5000L);

        Properties load = PropertiesUtil.load(content);
        LOG.info("nacos 成功配置获取 ");
        return load;
    }

    public static void main(String[] args) throws IOException, NacosException {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        Properties defaultGroup =
                NacosUtil.getFromNacosConfig("sync-kafka", parameterTool, "DEFAULT_GROUP");
        System.out.println(defaultGroup);
    }
}
