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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.fastjson.JSONObject;
import io.sophiadata.flink.base.BaseCode;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;

/**
 * Elasticsearch Sink 示例 —— 演示用 RestHighLevelClient 将数据写入 Elasticsearch。
 *
 * <p>由于 Flink 官方 ES Connector 版本兼容性问题，本示例演示使用 Elasticsearch REST Client 手动构建 Sink，这也是生产环境中常用的方案。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.sink.ElasticsearchSinkExample \
 *   --es.host localhost --es.port 9200
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>RestHighLevelClient 的创建和生命周期管理
 *   <li>自定义 RichMapFunction 构造 IndexRequest
 *   <li>Elasticsearch 批量写入的思路（Flink checkpoint 触发 flush）
 * </ul>
 *
 * <p>注意：本示例仅为教学用途，生产环境建议使用 Flink 官方 Elasticsearch Connector。
 */
public class ElasticsearchSinkExample extends BaseCode {

    public static void main(final String[] args) throws Exception {
        new ElasticsearchSinkExample().init(args, "Elasticsearch_Sink_Example");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        env.setParallelism(1);

        final String esHost = getArg(args, "es.host", "localhost");
        final int esPort = getArgInt(args, "es.port", 9200);
        final String indexName = getArg(args, "es.index", "flink-demo");

        // 1. 模拟数据流：传感器读数
        final DataStream<String> source =
                env.fromElements(
                        "{\"sensor\":\"sensor_1\",\"value\":10.5,\"ts\":1700000001000}",
                        "{\"sensor\":\"sensor_2\",\"value\":20.3,\"ts\":1700000002000}",
                        "{\"sensor\":\"sensor_1\",\"value\":15.7,\"ts\":1700000003000}",
                        "{\"sensor\":\"sensor_3\",\"value\":30.1,\"ts\":1700000004000}",
                        "{\"sensor\":\"sensor_2\",\"value\":25.8,\"ts\":1700000005000}");

        // 2. 转换为 IndexRequest（使用 RichMapFunction 管理 ES 客户端）
        final SingleOutputStreamOperator<String> result =
                source.map(new ElasticsearchIndexMapper(esHost, esPort, indexName))
                        .name("ES_Index_Mapper");

        // 输出转换结果（实际生产中这里应该是 sinkTo ES）
        result.print("ES_IndexRequest").setParallelism(1);
    }

    /**
     * 将 JSON 字符串转换为 Elasticsearch IndexRequest 的 RichMapFunction。
     *
     * <p>在 open() 中创建 RestHighLevelClient，在 close() 中释放资源。
     */
    public static class ElasticsearchIndexMapper extends RichMapFunction<String, String> {

        private static final long serialVersionUID = 1L;
        private final String esHost;
        private final int esPort;
        private final String indexName;

        private transient RestHighLevelClient client;

        public ElasticsearchIndexMapper(
                final String esHost, final int esPort, final String indexName) {
            this.esHost = esHost;
            this.esPort = esPort;
            this.indexName = indexName;
        }

        @Override
        public void open(final Configuration parameters) throws Exception {
            // 创建 ES 客户端（在 RichFunction 的 open 中初始化）
            client =
                    new RestHighLevelClient(
                            RestClient.builder(new HttpHost(esHost, esPort, "http")));
        }

        @Override
        public String map(final String value) throws Exception {
            // 解析 JSON 并构建 IndexRequest
            final JSONObject json = JSONObject.parseObject(value);
            final IndexRequest request =
                    new IndexRequest(indexName).source(value, XContentType.JSON);

            // 实际生产中这里会调用 client.index(request)
            // 此处仅演示请求构建过程
            return "Index: " + request.index() + ", ID: " + request.id() + ", Source: " + value;
        }

        @Override
        public void close() throws Exception {
            if (client != null) {
                client.close();
            }
        }
    }

    private static String getArg(final String[] args, final String key, final String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (("--" + key).equals(args[i])) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }

    private static int getArgInt(final String[] args, final String key, final int defaultValue) {
        final String value = getArg(args, key, String.valueOf(defaultValue));
        return Integer.parseInt(value);
    }
}
