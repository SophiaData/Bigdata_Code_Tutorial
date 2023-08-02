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

import com.alibaba.fastjson.JSON;
import io.sophiadata.flink.source.config.AppConfig;
import io.sophiadata.flink.source.utils.RandomOptionGroup;
import lombok.Builder;
import lombok.Data;

import java.util.List;

/** (@sophiadata) (@date 2023/8/2 11:09). */
@Data
@Builder
public class AppMain {

    private Long ts; // (String) 客户端日志产生时的时间

    private AppCommon common;

    private AppPage page;

    private AppError err;

    private AppNotice notice;

    private AppStart start;

    private List<AppDisplay> displays;
    private List<AppAction> actions;

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    public static class AppMainBuilder {

        public void checkError() {
            Integer errorRate = AppConfig.error_rate;
            Boolean ifError =
                    RandomOptionGroup.builder()
                            .add(true, errorRate)
                            .add(false, 100 - errorRate)
                            .build()
                            .getRandBoolValue();
            if (ifError) {
                AppError appError = AppError.build();
                this.err = appError;
            }
        }
    }
}
