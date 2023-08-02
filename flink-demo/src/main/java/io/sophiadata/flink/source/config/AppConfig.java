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

import io.sophiadata.flink.source.utils.ParamUtil;

/** (@sophiadata) (@date 2023/8/2 11:13). */
public class AppConfig {

    public static String mock_date = "2021-11-11";

    public static Integer mock_count = Integer.MAX_VALUE;

    // 是否产生倾斜数据: 1是、0否;目前是 mid、skuid(商品明细) 产生倾斜
    public static String mock_skew = "1";

    // 倾斜比率，百分比
    public static String mock_skew_rate = "80";

    // 设备最大值
    public static Integer max_mid = 1000000;

    // 会员最大值
    public static Integer max_uid = 20000000;

    // 购物券最大id
    public static Integer max_coupon_id = 30;

    // 商品最大值
    public static Integer max_sku_id = 100000;

    // 页面最大访问时间
    public static Integer page_during_max_ms = 20000;

    // 错误概率 百分比
    public static Integer error_rate = 3;

    // 每条日志发送延迟 ms
    public static Integer log_sleep = 0;

    // 添加收藏率 百分比
    public static Integer if_favor_rate = 30;

    // 取消收藏率 百分比
    public static Integer if_favor_cancel_rate = 10;

    // 添加购物车率 百分比
    public static Integer if_cart_rate = 10;

    // 增加购物车商品数量率 百分比
    public static Integer if_cart_add_num_rate = 10;

    // 减少购物车商品数量率 百分比
    public static Integer if_cart_minus_num_rate = 10;

    // 删除购物车率 百分比
    public static Integer if_cart_rm_rate = 10;

    // 增加收货地址率 百分比
    public static Integer if_add_address = 15;

    // 领取优惠券率 百分比
    public static Integer if_get_coupon = 25;

    // 最大曝光数
    public static Integer max_display_count = 10;

    // 最小曝光数
    public static Integer min_display_count = 4;

    // 最大事件数
    public static Integer max_activity_count = 2;

    public static Integer max_pos_id = 5;

    // 商品详情来源的占比： 用户查询，商品推广，智能推荐, 促销活动
    public static Integer[] sourceTypeRate = new Integer[] {40, 25, 15, 20};

    // 搜索的关键词
    public static String[] searchKeywords =
            new String[] {"图书", "小米", "iphone11", "电视", "口红", "ps5", "苹果手机", "小米盒子"};

    public void setMock_skew(String mock_skew) {
        AppConfig.mock_skew = mock_skew;
    }

    public void setMock_skew_rate(String mock_skew_rate) {
        AppConfig.mock_skew_rate = mock_skew_rate;
    }

    public void setMock_count(String mock_count) {
        AppConfig.mock_count = ParamUtil.checkCount(mock_count);
    }

    public void setMax_mid(String max_mid) {
        AppConfig.max_mid = ParamUtil.checkCount(max_mid);
    }

    public void setMax_uid(String max_uid) {
        AppConfig.max_uid = ParamUtil.checkCount(max_uid);
    }

    public void setMax_sku_id(String max_sku_id) {
        AppConfig.max_sku_id = ParamUtil.checkCount(max_sku_id);
    }

    public void setPage_during_max_ms(String page_during_max_ms) {
        AppConfig.page_during_max_ms = ParamUtil.checkCount(page_during_max_ms);
    }

    public void setError_rate(String error_rate) {
        AppConfig.error_rate = ParamUtil.checkRatioNum(error_rate);
    }

    public void setLog_sleep(String log_sleep) {
        AppConfig.log_sleep = ParamUtil.checkCount(log_sleep);
    }

    public static void setIf_favor_rate(Integer if_favor_rate) {
        AppConfig.if_favor_rate = if_favor_rate;
    }

    public static void setIf_favor_cancel_rate(Integer if_favor_cancel_rate) {
        AppConfig.if_favor_cancel_rate = if_favor_cancel_rate;
    }

    public static void setIf_cart_rate(Integer if_cart_rate) {
        AppConfig.if_cart_rate = if_cart_rate;
    }

    public static void setIf_cart_add_num_rate(Integer if_cart_add_num_rate) {
        AppConfig.if_cart_add_num_rate = if_cart_add_num_rate;
    }

    public static void setIf_cart_minus_num_rate(Integer if_cart_minus_num_rate) {
        AppConfig.if_cart_minus_num_rate = if_cart_minus_num_rate;
    }

    public static void setIf_cart_rm_rate(Integer if_cart_rm_rate) {
        AppConfig.if_cart_rm_rate = if_cart_rm_rate;
    }

    public static void setIf_add_address(Integer if_add_address) {
        AppConfig.if_add_address = if_add_address;
    }

    public static void setMax_display_count(Integer max_display_count) {
        AppConfig.max_display_count = max_display_count;
    }

    public static void setMin_display_count(Integer min_display_count) {
        AppConfig.min_display_count = min_display_count;
    }

    public static void setMax_activity_count(Integer max_activity_count) {
        AppConfig.max_activity_count = max_activity_count;
    }

    public void setMockDate(String mockDate) {
        AppConfig.mock_date = mockDate;
    }

    public void setSourceType(String sourceTypeRate) {
        Integer[] sourceTypeRateArray = ParamUtil.checkRate(sourceTypeRate, 4);
        AppConfig.sourceTypeRate = sourceTypeRateArray;
    }

    public void setSearchKeywords(String keywords) {
        AppConfig.searchKeywords = ParamUtil.checkArray(keywords);
    }

    public void setIf_get_coupon(String if_get_coupon_ratio) {
        AppConfig.if_get_coupon = ParamUtil.checkRatioNum(if_get_coupon_ratio);
    }

    public void setMaxCouponId(String couponId) {
        AppConfig.max_coupon_id = ParamUtil.checkCount(couponId);
    }
}
