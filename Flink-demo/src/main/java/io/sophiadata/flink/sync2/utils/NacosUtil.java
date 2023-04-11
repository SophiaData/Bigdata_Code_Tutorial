package io.sophiadata.flink.sync2.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * (@SophiaData) (@date 2023/4/11 17:11).
 */
public class NacosUtil {

    private static final Logger LOG = LoggerFactory.getLogger(NacosUtil.class);

    /**
     * @param assembly data_id
     * @param params 具体配置
     * @param group 配置组
     * @throws IOException io 异常
     * @throws NacosException nacos 异常
     */
    public static Properties getFromNacosConfig(String assembly, ParameterTool params, String group)
            throws IOException, NacosException {

        Properties properties = new Properties();

        properties.setProperty("serverAddr", params.get("nacos_server", ""));
        properties.setProperty("username", params.get("nacos_username", ""));
        properties.setProperty("password", params.get("nacos_pd", ""));
        properties.setProperty("namespace", params.get("nacos_namespace", ""));

        ConfigService service = NacosFactory.createConfigService(properties);
        String content = service.getConfig(assembly + ".properties", group, 5000L);
        //        String content2 = service.getConfig(assembly + ".yaml", group, 5000L);

        Properties load = PropertiesUtil.load(content);
        LOG.info("nacos 成功配置获取 ");
        return load;
    }

    public static void main(String[] args) throws IOException, NacosException {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        Properties defaultGroup =
                NacosUtil.getFromNacosConfig("sync-kafka", parameterTool, "DEFAULT_GROUP");
        System.out.println(defaultGroup);
    }
}
