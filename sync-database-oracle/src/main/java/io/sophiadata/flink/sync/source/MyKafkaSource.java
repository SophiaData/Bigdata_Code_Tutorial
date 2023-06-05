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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import com.alibaba.nacos.api.exception.NacosException;
import io.sophiadata.flink.sync.utils.NacosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/** (@SophiaData) (@date 2023/4/21 11:16). */
public class MyKafkaSource {

    private static final String DEFAULT_GROUP = "DEFAULT_GROUP";

    private static final String NEW_SYNC_KAFKA = "new-sync-kafka";
    private static final Logger LOG = LoggerFactory.getLogger(MyKafkaSource.class);

    public SingleOutputStreamOperator<String> singleOutputStreamOperator(
            ParameterTool params, StreamExecutionEnvironment env) {
        Properties kafkaNacosConfig = null;
        try {
            kafkaNacosConfig = NacosUtil.getFromNacosConfig(NEW_SYNC_KAFKA, params, DEFAULT_GROUP);
        } catch (IOException e) {
            LOG.error(" IOException -> {} ", e.getMessage());
        } catch (NacosException e) {
            LOG.error(" NacosException -> {} ", e.getMessage());
        }

        if (kafkaNacosConfig.getProperty("kafkaUser." + params.get("province.code")).isEmpty()
                && kafkaNacosConfig
                        .getProperty("kafkaPd." + params.get("province.code"))
                        .isEmpty()) {
            LOG.warn(" 当前 Kafka 集群没有安全认证 ");
            Properties properties = new Properties();
            properties.put(
                    "bootstrap.servers",
                    kafkaNacosConfig
                            .get("share.bootstrap.servers." + params.get("province.code"))
                            .toString());
            properties.put(
                    "group.id",
                    kafkaNacosConfig.get("group.id." + params.get("province.code")).toString());
            properties.put("auto.offset.reset", "earliest");
            //            properties.put("auto.offset.reset", "latest");

            ArrayList<String> topics = new ArrayList<>();

            String topic =
                    kafkaNacosConfig.get("input.topic." + params.get("province.code")).toString();
            if (topic.contains(",")) {
                String[] splits = topic.split(",");
                topics.addAll(Arrays.asList(splits));
            } else {
                topics.add(topic);
            }

            return env.addSource(
                    new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), properties)
                            //                            .setStartFromTimestamp(
                            //                                    params.getLong("kafkaTimestamp",
                            // System.currentTimeMillis()))
                            .setCommitOffsetsOnCheckpoints(false)
                            .setStartFromGroupOffsets());

        } else {

            Properties properties = new Properties();
            properties.put(
                    "bootstrap.servers",
                    kafkaNacosConfig
                            .get("share.bootstrap.servers." + params.get("province.code"))
                            .toString());
            properties.put(
                    "group.id",
                    kafkaNacosConfig.get("group.id." + params.get("province.code")).toString());
            properties.put("security.protocol", "SASL_PLAINTEXT");
            properties.put("sasl.mechanism", "PLAIN");
            properties.put(
                    "sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required\nusername=\""
                            + kafkaNacosConfig.get("kafkaUser." + params.get("province.code"))
                            + "\"\npassword=\""
                            + kafkaNacosConfig.get("kafkaPd." + params.get("province.code"))
                            + "\";");
            properties.put("auto.offset.reset", params.get("KafkaOffset", "latest"));

            ArrayList<String> topics = new ArrayList<>();

            String topic =
                    kafkaNacosConfig.get("input.topic." + params.get("province.code")).toString();
            if (topic.contains(",")) {
                String[] splits = topic.split(",");
                topics.addAll(Arrays.asList(splits));
            } else {
                topics.add(topic);
            }
            return env.addSource(
                    new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), properties)
                            .setCommitOffsetsOnCheckpoints(false)
                            .setStartFromGroupOffsets());
        }
    }
}
