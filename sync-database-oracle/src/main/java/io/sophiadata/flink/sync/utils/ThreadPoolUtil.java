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

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** (@SophiaData) (@date 2023/3/20 19:59). */
public final class ThreadPoolUtil {
    // 本项目未使用，仅做测试

    private static final Logger LOG = LoggerFactory.getLogger(ThreadPoolUtil.class);
    /** ThreadPoolExecutor. */
    private static volatile ThreadPoolExecutor threadPoolExecutor;

    private ThreadPoolUtil() {}

    /**
     * getThreadPoolExecutor.
     *
     * @return doc
     */
    public static ThreadPoolExecutor getThreadPoolExecutor() {

        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor =
                            new ThreadPoolExecutor(
                                    1, 1, 1, TimeUnit.MINUTES, new LinkedBlockingDeque<>());
                }
            }
        }

        return threadPoolExecutor;
    }

    /**
     * main.
     *
     * @param args args
     */
    public static void main(String[] args) {

        threadPoolExecutor = getThreadPoolExecutor();

        for (int i = 0; i < 10; i++) {
            threadPoolExecutor.execute(
                    () -> {
                        System.out.println(Thread.currentThread().getName());
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
        }
    }
}
