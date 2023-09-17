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

package io.sophiadata.flink.sync.common;

/** (@sophiadata) (@date 2023/9/17 10:25). */
public class Constants {
    public static final String sinkUrl =
            "jdbc:mysql://localhost:3306/test2?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";

    public static final String sinkUsername = "root";

    public static final String sinkPassword = "123456";

    public static final String hostname = "localhost";

    public static final Integer port = 3306;

    public static final String username = "root";

    public static final String password = "123456";

    public static final String databaseName = "test";

    public static final String tableList = ".*";

    public static final Integer setParallelism = 2;

    public static final String cdcSourceName = "mysql-cdc-1";
}
