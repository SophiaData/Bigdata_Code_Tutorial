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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.alibaba.nacos.api.exception.NacosException;
import io.sophiadata.flink.sync.utils.NacosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class MyRedisSource implements SourceFunction<Map<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(MyRedisSource.class);

    private boolean running = true;

    private final ParameterTool params;

    private static JedisCluster redisCluster;

    public MyRedisSource(ParameterTool params) {
        this.params = params;
    }

    @Override
    public void run(SourceContext<Map<String, String>> ctx) {
        Properties redisConfig = null;
        try {
            redisConfig = NacosUtil.getFromNacosConfig("new-sync-redis", params, "DEFAULT_GROUP");
        } catch (IOException e) {
            LOG.error("IOException -> {} ", e.getMessage());
        } catch (NacosException e) {
            LOG.error("NacosException -> {} ", e.getMessage());
        }
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

            if (redisConfig.getProperty("RedisPd").isEmpty()) {
                redisCluster = new JedisCluster(hostAndPortSet);
            } else {
                redisCluster =
                        new JedisCluster(
                                hostAndPortSet,
                                10000,
                                10000,
                                100,
                                redisConfig.get("RedisPd").toString(),
                                config);
            }

        } catch (Exception e) {
            LOG.error("redis 连接异常 ");
        }

        Map<String, String> tableInfoMap;
        while (running) {
            LOG.info("redisCluster : {}", redisCluster);

            tableInfoMap = redisCluster.hgetAll("NEW-SYNC-TABLELIST");
            if (tableInfoMap != null) {
                String[] split =
                        tableInfoMap
                                .toString()
                                .replaceAll("\"", "")
                                .replaceAll("\\{", "")
                                .replaceAll("\\}", "")
                                .split(", ");
                Map<String, String> map = new HashMap<>();
                for (String s : split) {
                    String[] pair = s.split("=");
                    String key = pair[0];
                    String value = pair.length > 1 ? pair[1] : "";
                    map.put(key, value);
                }
                ctx.collect(map);
            }
            try {
                Thread.sleep(60 * 60 * 1000);
            } catch (InterruptedException e) {
                LOG.error(" Thread InterruptedException -> {}", e.getMessage());
            }
        }

        redisCluster.close();
    }

    @Override
    public void cancel() {
        running = false;
    }
}
