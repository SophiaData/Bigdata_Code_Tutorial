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

package io.sophiadata.flink.source.bean;

import io.sophiadata.flink.source.utils.RandomNum;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.net.ConnectException;

/** (@sophiadata) (@date 2023/8/2 11:08). */
@Data
@AllArgsConstructor
public class AppError {

    Integer error_code;

    String msg;

    public static AppError build() {
        int errorCode = RandomNum.getRandInt(1001, 4001);
        String msg =
                " Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.log.bean.AppError.main(AppError.java:xxxxxx)";
        return new AppError(errorCode, msg);
    }

    public static void main(String[] args) throws Exception {
        throw new ConnectException();
    }

    //

}
