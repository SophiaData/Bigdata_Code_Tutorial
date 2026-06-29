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

package io.sophiadata.flink.source.config;

/** (@sophiadata) (@date 2023/8/2 11:13). */
public final class AppConfig {

    public static final String MOCK_DATE = "2021-11-11";

    public static final int MOCK_COUNT = Integer.MAX_VALUE;

    // 是否产生倾斜数据: 1是、0否;目前是 mid、skuid(商品明细) 产生倾斜
    public static final String MOCK_SKEW = "1";

    // 倾斜比率，百分比
    public static final String MOCK_SKEW_RATE = "80";

    // 设备最大值
    public static final int MAX_MID = 1000000;

    // 会员最大值
    public static final int MAX_UID = 20000000;

    // 购物券最大id
    public static final int MAX_COUPON_ID = 30;

    // 商品最大值
    public static final int MAX_SKU_ID = 100000;

    // 页面最大访问时间
    public static final int PAGE_DURING_MAX_MS = 20000;

    // 错误概率 百分比
    public static final int ERROR_RATE = 3;

    // 每条日志发送延迟 ms
    public static final int LOG_SLEEP = 0;

    // 添加收藏率 百分比
    public static final int IF_FAVOR_RATE = 30;

    // 取消收藏率 百分比
    public static final int IF_FAVOR_CANCEL_RATE = 10;

    // 添加购物车率 百分比
    public static final int IF_CART_RATE = 10;

    // 增加购物车商品数量率 百分比
    public static final int IF_CART_ADD_NUM_RATE = 10;

    // 减少购物车商品数量率 百分比
    public static final int IF_CART_MINUS_NUM_RATE = 10;

    // 删除购物车率 百分比
    public static final int IF_CART_RM_RATE = 10;

    // 增加收货地址率 百分比
    public static final int IF_ADD_ADDRESS = 15;

    // 领取优惠券率 百分比
    public static final int IF_GET_COUPON = 25;

    // 最大曝光数
    public static final int MAX_DISPLAY_COUNT = 10;

    // 最小曝光数
    public static final int MIN_DISPLAY_COUNT = 4;

    // 最大事件数
    public static final int MAX_ACTIVITY_COUNT = 2;

    public static final int MAX_POS_ID = 5;

    // 商品详情来源的占比： 用户查询，商品推广，智能推荐, 促销活动
    public static final Integer[] SOURCE_TYPE_RATE = new Integer[] {40, 25, 15, 20};

    // 搜索的关键词
    public static final String[] SEARCH_KEYWORDS =
            new String[] {"图书", "小米", "iphone11", "电视", "口红", "ps5", "苹果手机", "小米盒子"};

    private AppConfig() {
        // Private constructor to prevent instantiation
    }
}
