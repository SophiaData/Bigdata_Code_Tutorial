package io.sophiadata.flink.sync2.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * (@SophiaData) (@date 2023/4/11 17:11).
 */
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
