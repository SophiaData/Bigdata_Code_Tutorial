package io.sophiadata.flink.sync2.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * (@SophiaData) (@date 2023/4/11 17:12).
 */
public class RedisUtil {
    private static final Logger LOG = LoggerFactory.getLogger(RedisUtil.class);

    private static JedisCluster redisCluster;

    public static JedisCluster getRedisClient(ParameterTool params) throws Exception {

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
        return redisCluster;
    }

    public static void closeConnection() {
        if (redisCluster != null) {
            redisCluster.close();
            LOG.info(" jedis 成功连接关闭 ");
        }
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
    }
}
