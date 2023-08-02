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

import io.sophiadata.flink.source.utils.RanOpt;
import io.sophiadata.flink.source.utils.RandomNum;
import io.sophiadata.flink.source.utils.RandomOptionGroup;
import lombok.AllArgsConstructor;
import lombok.Data;

/** (@sophiadata) (@date 2023/8/2 11:12). */
@Data
@AllArgsConstructor
public class AppStart {

    private String entry; // 入口：  安装后进入=install，  点击图标= icon，  点击通知= notice
    private Long open_ad_id; // 开屏广告Id
    private Integer open_ad_ms; // 开屏广告持续时间
    private Integer open_ad_skip_ms; // 开屏广告点击掉过的时间  未点击为0
    private Integer loading_time; // 加载时长：计算下拉开始到接口返回数据的时间，（开始加载报0，加载成功或加载失败才上报时间）

    public static class Builder {
        private String entry; // 入口： 安装后进入=install，  点击图标= icon，  点击通知= notice
        private Long open_ad_id; // 开屏广告Id
        private Integer open_ad_ms; // 开屏广告持续时间
        private Integer open_ad_skip_ms; // 开屏广告持续多长时间，点击跳过 未点击为0
        private Integer loading_time; // 加载时长：计算下拉开始到接口返回数据的时间，（开始加载报0，加载成功或加载失败才上报时间）
        private Integer first_open;

        public Builder() {
            entry =
                    new RandomOptionGroup<String>(
                                    new RanOpt<String>("install", 5),
                                    new RanOpt<String>("icon", 75),
                                    new RanOpt<String>("notice", 20))
                            .getRandStringValue();
            open_ad_id = RandomNum.getRandInt(1, 20) + 0L;
            open_ad_ms = RandomNum.getRandInt(1000, 10000);
            open_ad_skip_ms =
                    RandomOptionGroup.builder()
                            .add(0, 50)
                            .add(RandomNum.getRandInt(1000, open_ad_ms), 50)
                            .build()
                            .getRandIntValue();
            loading_time = RandomNum.getRandInt(1000, 20000);
            first_open = RandomNum.getRandInt(0, 1);
        }

        public AppStart build() {
            return new AppStart(entry, open_ad_id, open_ad_ms, open_ad_skip_ms, loading_time);
        }
    }
}
