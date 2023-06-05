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

package io.sophiadata.flink.sync.app;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import io.sophiadata.flink.sync.base.BaseCode;
import io.sophiadata.flink.sync.executor.KafkaToOracle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** (@SophiaData) (@date 2023/6/5 10:16). */
public class FlinkSyncApp extends BaseCode {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSyncApp.class);
    private static ParameterTool params = null;

    public static void main(String[] args) throws Exception {
        params = ParameterTool.fromArgs(args);

        new FlinkSyncApp().init(args, params.get("jobName"));
        LOG.info(params.get("jobName") + " job initialization is normal ！！！ ");
    }

    @Override
    public void handle(String[] args, StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        env.getConfig().setGlobalJobParameters(params);
        // 入口
        new KafkaToOracle().kafkaToOracle(env, params);
    }
}
