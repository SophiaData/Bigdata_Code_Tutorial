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

    public static final String SINK_URL =
            "jdbc:mysql://localhost:3306/test2?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";

    public static final String SINK_USERNAME = System.getenv("MYSQL_SINK_USERNAME");

    public static final String SINK_PASSWORD = System.getenv("MYSQL_SINK_PASSWORD");

    public static final String HOSTNAME = "localhost";

    public static final Integer PORT = 3306;

    public static final String USERNAME = System.getenv("MYSQL_USERNAME");

    public static final String PASSWORD = System.getenv("MYSQL_PASSWORD");

    public static final String DATABASE_NAME = "test";

    public static final String TABLE_LIST = ".*";

    public static final Integer SET_PARALLELISM = 2;

    public static final String CDC_SOURCE_NAME = "mysql-cdc-1";
}
