package io.sophiadata.flink.sync2.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import com.alibaba.nacos.api.exception.NacosException;
import io.sophiadata.flink.sync2.utils.NacosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * (@gtk) (@SophiaData 2023/4/11 17:08).
 */
public class MyKafkaSource {
    private static final Logger LOG = LoggerFactory.getLogger(MyKafkaSource.class);

    public SingleOutputStreamOperator<String> singleOutputStreamOperator(
            ParameterTool params, StreamExecutionEnvironment env, String group)
            throws IOException, NacosException {
        Properties kafkaNacosConfig = NacosUtil.getFromNacosConfig("new-sync-kafka", params, group);

        if (kafkaNacosConfig.getProperty("kafkaUser").isEmpty()
                && kafkaNacosConfig.getProperty("kafkaPd").isEmpty()) {
            LOG.warn(" 当前 Kafka 集群没有安全认证 ");
            Properties properties = new Properties();
            properties.put(
                    "bootstrap.servers", kafkaNacosConfig.get("bootstrapServers").toString());
            properties.put("group.id", kafkaNacosConfig.get("groupId").toString());
            properties.put("auto.offset.reset", "earliest");

            ArrayList<String> topics = new ArrayList<>();

            String topic = kafkaNacosConfig.get("topic").toString();
            if (topic.contains(",")) {
                String[] splits = topic.split(",");
                topics.addAll(Arrays.asList(splits));
            } else {
                topics.add(topic);
            }

            return env.addSource(
                    new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), properties)
                            .setStartFromEarliest());

        } else {

            Properties properties = new Properties();
            properties.put(
                    "bootstrap.servers", kafkaNacosConfig.get("bootstrapServers").toString());
            properties.put("group.id", kafkaNacosConfig.get("groupId").toString());
            properties.put("security.protocol", "SASL_PLAINTEXT");
            properties.put("sasl.mechanism", "PLAIN");
            properties.put(
                    "sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username="
                            + kafkaNacosConfig.get("kafkaUser")
                            + "  password="
                            + kafkaNacosConfig.get("kafkaPd")
                            + ";");
            properties.put("auto.offset.reset", "earliest");

            ArrayList<String> topics =
                    new ArrayList<>(Arrays.asList(kafkaNacosConfig.get("topic").toString()));
            return env.addSource(
                    new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), properties)
                            .setStartFromEarliest());
        }
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        new MyKafkaSource().singleOutputStreamOperator(params, env, "DEFAULT_GROUP").print();
        env.execute();
    }
}
