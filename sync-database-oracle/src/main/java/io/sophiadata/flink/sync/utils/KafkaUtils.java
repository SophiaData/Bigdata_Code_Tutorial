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

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/** (@SophiaData) (@date 2023/4/7 09:35). */
public class KafkaUtils {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);

    static ParameterTool params;

    public KafkaUtils(ParameterTool params) {
        KafkaUtils.params = params;
    }

    public static KafkaProducer<String, String> createProducer(ParameterTool params) {
        Properties props = new Properties();
        props.put(
                "bootstrap.servers",
                params.get("KafkaServers", "192.16.20.83:9092,192.16.20.84:9092"));
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

    public void updateKafkaTaskStatus(
            String taskId, String areaCode, String statusCode, String errorExp) {
        HashMap<String, String> hashMap = new HashMap<>();
        hashMap.put("taskId", taskId);
        hashMap.put("areaCode", areaCode);
        hashMap.put("statusCode", statusCode);
        hashMap.put("errorExp", errorExp);
        String jsonString = JSON.toJSONString(hashMap);
        sendKafkaMS("O_ORG_TASK_MONITOR", jsonString);
    }

    private static void sendKafkaMS(String topic, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        try (KafkaProducer<String, String> producer = createProducer(params)) {
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf(
                    "Sent message to topic %s at offset %d%n", metadata.topic(), metadata.offset());
            LOG.info(
                    " Sent message to topic {} at offset {} ", metadata.topic(), metadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Failed to send message: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        String topic = "O_ORG_TASK_MONITOR";
        String message = "Hello, World!";
        sendKafkaMS(topic, message);
    }
}
