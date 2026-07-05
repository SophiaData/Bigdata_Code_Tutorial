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

package io.sophiadata.flink.streaming.advanced;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import io.sophiadata.flink.base.BaseCode;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Async I/O 示例 —— 异步查询外部系统（模拟 HTTP/Redis 查询）。
 *
 * <p>演示 RichAsyncFunction 的生命周期、超时处理、容量控制。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.streaming.advanced.AsyncIoExample
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>RichAsyncFunction 的 open / asyncInvoke / timeout / close
 *   <li>orderedWait vs unorderedWait 的区别
 *   <li>超时处理和容量控制（capacity 参数）
 * </ul>
 */
public class AsyncIoExample extends BaseCode {

    public static void main(final String[] args) throws Exception {
        new AsyncIoExample().init(args, "Async_IO_Example");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        // 1. 模拟数据流
        final DataStream<String> stream =
                env.fromElements("user_1", "user_2", "user_3", "user_4", "user_5");

        // 2. 异步查询（模拟 HTTP/Redis 查询）
        final DataStream<String> result =
                AsyncDataStream.unorderedWait(
                        stream, new LookupAsyncFunction(), 30, TimeUnit.SECONDS, 100);

        // 3. 输出结果
        result.print("Async_Result").setParallelism(1);
    }

    /** 模拟异步查询外部系统（如 HTTP API / Redis）。 */
    public static class LookupAsyncFunction extends RichAsyncFunction<String, String> {

        private static final long serialVersionUID = 1L;
        private transient ExecutorService executor;

        @Override
        public void open(final Configuration parameters) {
            executor = Executors.newFixedThreadPool(10);
        }

        @Override
        public void asyncInvoke(final String value, final ResultFuture<String> resultFuture) {
            CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    // 模拟外部查询延迟
                                    Thread.sleep(100);
                                } catch (final InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return "enriched:" + value;
                            },
                            executor)
                    .whenComplete(
                            (result, throwable) -> {
                                if (throwable != null) {
                                    resultFuture.completeExceptionally(throwable);
                                } else {
                                    resultFuture.complete(Collections.singleton(result));
                                }
                            });
        }

        @Override
        public void timeout(final String input, final ResultFuture<String> resultFuture) {
            resultFuture.complete(Collections.singleton("timeout:" + input));
        }

        @Override
        public void close() {
            if (executor != null) {
                executor.shutdown();
            }
        }
    }
}
