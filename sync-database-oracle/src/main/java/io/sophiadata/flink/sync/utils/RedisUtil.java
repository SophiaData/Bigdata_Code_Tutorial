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

package io.sophiadata.flink.sync.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** (@SophiaData) (@date 2023/3/8 21:08). */
public class RedisUtil {
    private static final Logger LOG = LoggerFactory.getLogger(RedisUtil.class);

    private static final String DEFAULT_GROUP = "DEFAULT_GROUP";

    private static JedisCluster redisCluster;

    public static JedisCluster getRedisClient(ParameterTool params) throws Exception {

        Properties redisConfig =
                NacosUtil.getFromNacosConfig("new-sync-redis", params, DEFAULT_GROUP);
        JedisPoolConfig config = new JedisPoolConfig();
        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        config.setMaxTotal(500);
        config.setMaxIdle(50);
        config.setMinIdle(20);
        config.setMaxWaitMillis(10000);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
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
            LOG.error(" redis abnormal connection ");
        }
        return redisCluster;
    }

    public static void closeConnection() {
        if (redisCluster != null) {
            redisCluster.close();
            LOG.info(" jedis successfully connected and closed ");
        }
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        JedisCluster redisClient = RedisUtil.getRedisClient(params);
        Map<String, String> stringStringMap = redisClient.hgetAll("NEW-SYNC-TABLELISTs");
        System.out.println(stringStringMap);
        if (stringStringMap.isEmpty()) {
            System.out.println(111);
        }

        String[] split =
                stringStringMap
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

        System.out.println(map);

        //        String jsonString = JSON.toJSONString(stringStringMap);
        //
        //        JSONObject tableListJson = JSONObject.parseObject(jsonString);
        //        Set<String> tableListSets = tableListJson.keySet();
        //        List<String> tables = new ArrayList<>(tableListSets);
        //
        //        System.out.println(tables);
        RedisUtil.closeConnection();
    }
}
