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

package io.sophiadata.flink.sink;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 端到端测试：验证数据能写入 Elasticsearch 并被查询。
 *
 * <p>需要 Docker 环境。无 Docker 时自动跳过。
 */
public class ElasticsearchSinkExampleE2E {

    static ElasticsearchContainer es;

    @ClassRule
    public static final org.apache.flink.test.util.MiniClusterWithClientResource MINI_CLUSTER =
            new org.apache.flink.test.util.MiniClusterWithClientResource(
                    new org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
                                    .Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    @BeforeClass
    public static void startEs() {
        Assume.assumeTrue("Docker not available, skipping E2E test", isDockerAvailable());
        es =
                new ElasticsearchContainer(DockerImageName.parse("elasticsearch:7.17.9"))
                        .withEnv("discovery.type", "single-node")
                        .withEnv("xpack.security.enabled", "false");
        es.start();
    }

    @AfterClass
    public static void stopEs() {
        if (es != null) {
            es.stop();
        }
    }

    @Test
    public void testWriteAndQueryElasticsearch() throws Exception {
        final String indexName = "test-flink-es-output";
        final String httpAddress = es.getHttpHostAddress();
        final String[] parts = httpAddress.replace("http://", "").split(":");
        final String host = parts[0];
        final int port = Integer.parseInt(parts[1]);

        try (RestClient client = RestClient.builder(new HttpHost(host, port, "http")).build()) {
            for (final String doc :
                    new String[] {
                        "{\"sensor\":\"sensor_1\",\"value\":10.5}",
                        "{\"sensor\":\"sensor_2\",\"value\":20.3}",
                        "{\"sensor\":\"sensor_1\",\"value\":15.7}"
                    }) {
                final Request indexReq = new Request("POST", "/" + indexName + "/_doc");
                indexReq.setJsonEntity(doc);
                client.performRequest(indexReq);
            }
            client.performRequest(new Request("POST", "/" + indexName + "/_refresh"));

            final Request searchReq = new Request("GET", "/" + indexName + "/_search");
            searchReq.setJsonEntity("{\"query\":{\"match_all\":{}}}");
            final Response searchResp = client.performRequest(searchReq);
            final String body = EntityUtils.toString(searchResp.getEntity());

            assertThat(body).contains("\"total\":{\"value\":3");
            assertThat(body).contains("sensor_1");
            assertThat(body).contains("sensor_2");
        }
    }

    private static boolean isDockerAvailable() {
        try {
            final Process p = Runtime.getRuntime().exec(new String[] {"docker", "info"});
            return p.waitFor() == 0;
        } catch (final IOException | InterruptedException e) {
            return false;
        }
    }
}
