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

package io.sophiadata.flink.sync.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/** (@SophiaData) (@date 2023/7/20 09:52). */
public final class PropertiesUtil {

    private PropertiesUtil() {}

    private static final Logger LOG = LoggerFactory.getLogger(PropertiesUtil.class);

    public static Properties load(String content) {
        if (content == null) {
            throw new IllegalArgumentException("content is null");
        }

        try (InputStream input =
                new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))) {
            Properties props = new Properties();
            props.load(input);
            return props;

        } catch (IOException e) {
            LOG.error(" load properties failed {}", e.getMessage());
            throw new RuntimeException("load properties failed", e);
        }
    }
}
