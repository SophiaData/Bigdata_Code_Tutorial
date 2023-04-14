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
package io.sophiadata.flink.sync2.source;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import io.sophiadata.flink.sync2.utils.NacosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** (@SophiaData) (@date 2023/4/11 17:13). */
public class RedisSource implements SourceFunction<Map<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSource.class);

    private boolean running = true;

    private final ParameterTool params;

    private static JedisCluster redisCluster;

    public RedisSource(ParameterTool params) {
        this.params = params;
    }

    @Override
    public void run(SourceContext<Map<String, String>> ctx) throws Exception {
        Properties redisConfig =
                NacosUtil.getFromNacosConfig("new-sync-redis", params, "DEFAULT_GROUP");
        JedisPoolConfig config = new JedisPoolConfig();
        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        config.setMaxTotal(10);
        config.setMaxIdle(5);
        config.setMinIdle(2);
        config.setMaxWaitMillis(10000);
        try {
            Set<HostAndPort> hostAndPortSet = new HashSet<>();
            String redisHostAndPort = redisConfig.get("RedisHostAndPort").toString();
            for (String s : redisHostAndPort.split(",")) {
                HostAndPort hostAndPort =
                        new HostAndPort(s.split(":")[0], Integer.parseInt(s.split(":")[1]));
                hostAndPortSet.add(hostAndPort);
            }

            redisCluster =
                    new JedisCluster(
                            hostAndPortSet,
                            10000,
                            10000,
                            100,
                            redisConfig.get("RedisPd").toString(),
                            config);

        } catch (Exception e) {
            LOG.error("redis 连接异常 ");
        }

        Map<String, String> tableInfoMap;
        while (running) {
            tableInfoMap = redisCluster.hgetAll("NEW-SYNC-TABLELIST");
            if (tableInfoMap != null) {
                ctx.collect(tableInfoMap);
            }
            // 十分钟查询一次 redis 检查是否有新表 该查询有性能损耗
            Thread.sleep(10 * 60 * 1000);
        }

        redisCluster.close();
    }

    @Override
    public void cancel() {
        running = false;
    }
}
