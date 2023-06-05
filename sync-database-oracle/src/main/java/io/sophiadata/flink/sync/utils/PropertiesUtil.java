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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

/** (@SophiaData) (@date 2023/3/9 14:18). */
public class PropertiesUtil {

    private static final Logger LOG = LoggerFactory.getLogger(PropertiesUtil.class);

    public static Properties load(String content) throws IOException {
        Properties properties = new Properties();

        if (content != null) {
            properties.load(new ByteArrayInputStream(content.getBytes()));
            LOG.debug(" 成功 load 配置文件，内容为: " + properties);
        } else {
            throw new NullPointerException(" 配置文件为 null: " + properties);
        }
        return properties;
    }
}
