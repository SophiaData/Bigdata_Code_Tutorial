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

package io.sophiadata.flink.sync.operator;

import org.apache.flink.api.common.functions.FilterFunction;

import com.alibaba.fastjson.JSONObject;
import io.sophiadata.flink.sync.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** (@SophiaData) (@date 2023/4/20 15:32). */
public class MyFilterFunction implements FilterFunction<String> {

    private static final Logger logger = LoggerFactory.getLogger(MyFilterFunction.class);

    @Override
    public boolean filter(String value) {
        try {
            if (value == null
                    || JSONObject.parseObject(value).getString(Constant.TASK_ID) == null) {
                logger.warn(" value is null" + value);
                return false;
            } else {
                String string = JSONObject.parseObject(value).getString(Constant.TASK_ID);
                if (string.isEmpty()) {
                    return false;
                } else {
                    return true;
                }
            }
        } catch (Exception e) {
            logger.error(" Exception {} data {}", e.getMessage(), value);
            return false;
        }
    }
}
