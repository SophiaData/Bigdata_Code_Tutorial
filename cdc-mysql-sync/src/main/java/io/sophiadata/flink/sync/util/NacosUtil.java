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

import org.apache.flink.api.java.utils.ParameterTool;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/** (@SophiaData) (@date 2023/7/20 09:45). */
public final class NacosUtil {

    private NacosUtil() {}

    private static final Logger LOG = LoggerFactory.getLogger(NacosUtil.class);
    public static final String DEFAULT_GROUP = "DEFAULT_GROUP";

    public static final String NACOS_SERVER_KEY = "nacos_server";
    public static final String NACOS_DATA_ID_KEY = "nacos_data_id";
    public static final String NACOS_GROUP_KEY = "nacos_group";
    public static final String NACOS_USERNAME_KEY = "nacos_username";
    public static final String NACOS_PASSWORD_KEY = "nacos_pd";
    public static final String NACOS_NAMESPACE_KEY = "nacos_namespace";

    /** Marker key: if present, load config from a local .properties file. */
    public static final String CONFIG_KEY = "config";

    /**
     * Merge config from Nacos or local file into command-line args. Priority: CLI > Nacos/file.
     *
     * <p>Supports:
     *
     * <ul>
     *   <li>{@code --config=/path/to/config.properties} — local file
     *   <li>{@code --nacos_server=host:port --nacos_data_id=xxx} — Nacos remote
     * </ul>
     */
    public static ParameterTool mergeInto(final ParameterTool args) {
        if (args.has(CONFIG_KEY)) {
            return mergeLocalFile(args);
        }
        if (args.has(NACOS_SERVER_KEY)) {
            return mergeNacos(args);
        }
        return args;
    }

    // ==================== Local file ====================

    private static ParameterTool mergeLocalFile(final ParameterTool args) {
        final String configPath = args.get(CONFIG_KEY);
        if (configPath == null || configPath.isEmpty()) {
            throw new IllegalArgumentException("--config path is empty");
        }
        try {
            final Properties fileProps = loadPropertiesFile(configPath);
            final ParameterTool fileTool = ParameterTool.fromMap(toStringMap(fileProps));
            final ParameterTool merged = fileTool.mergeWith(args);
            LOG.info("merged {} keys from {} (CLI wins on conflict)", fileProps.size(), configPath);
            return merged;
        } catch (Exception e) {
            throw new IllegalStateException("failed to load config: " + configPath, e);
        }
    }

    private static Properties loadPropertiesFile(final String path) throws IOException {
        if (path.startsWith("classpath:")) {
            final String resource = path.substring("classpath:".length());
            try (InputStream is = NacosUtil.class.getResourceAsStream("/" + resource)) {
                if (is == null) {
                    throw new IOException("classpath resource not found: " + resource);
                }
                final Properties props = new Properties();
                props.load(is);
                return props;
            }
        }
        try (FileInputStream fis = new FileInputStream(path)) {
            final Properties props = new Properties();
            props.load(fis);
            return props;
        }
    }

    // ==================== Nacos ====================

    private static ParameterTool mergeNacos(final ParameterTool args) {
        final String dataId = args.get(NACOS_DATA_ID_KEY, "flink-sync");
        final String group = args.get(NACOS_GROUP_KEY, DEFAULT_GROUP);
        try {
            final Properties remote = getFromNacosConfig(dataId, args, group);
            final ParameterTool remoteTool = ParameterTool.fromMap(toStringMap(remote));
            final ParameterTool merged = remoteTool.mergeWith(args);
            LOG.info(
                    "merged {} keys from Nacos dataId={} group={} (CLI wins on conflict)",
                    remote.size(),
                    dataId,
                    group);
            return merged;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "failed to load Nacos config dataId=" + dataId + " group=" + group, e);
        }
    }

    static final Properties getFromNacosConfig(
            final String assembly, final ParameterTool params, final String group)
            throws IOException, NacosException {
        final Properties properties = new Properties();
        properties.setProperty("serverAddr", params.get(NACOS_SERVER_KEY, ""));
        properties.setProperty("username", params.get(NACOS_USERNAME_KEY, ""));
        properties.setProperty("password", params.get(NACOS_PASSWORD_KEY, ""));
        properties.setProperty("namespace", params.get(NACOS_NAMESPACE_KEY, ""));

        final ConfigService service = NacosFactory.createConfigService(properties);
        final String content = service.getConfig(assembly + ".properties", group, 5000L);
        final Properties load = PropertiesUtil.load(content);
        LOG.info("nacos successfully configured acquisition");
        return load;
    }

    // ==================== utils ====================

    private static Map<String, String> toStringMap(final Properties p) {
        final java.util.Map<String, String> out = new java.util.HashMap<>();
        for (final String name : p.stringPropertyNames()) {
            out.put(name, p.getProperty(name));
        }
        return out;
    }
}
