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

package io.sophiadata.flink.source.enums;

/** (@sophiadata) (@date 2023/8/2 11:23). */
public enum PageId {
    home("首页"),
    category("分类页"),
    discovery("发现页"),
    top_n("热门排行"),
    favor("收藏页"),
    search("搜索页"),
    good_list("商品列表页"),
    good_detail("商品详情"),
    good_spec("商品规格"),
    comment("评价"),
    comment_done("评价完成"),
    comment_list("评价列表"),
    cart("购物车"),
    trade("下单结算"),
    payment("支付页面"),
    payment_done("支付完成"),
    orders_all("全部订单"),
    orders_unpaid("订单待支付"),
    orders_undelivered("订单待发货"),
    orders_unreceipted("订单待收货"),
    orders_wait_comment("订单待评价"),
    mine("我的"),
    activity("活动"),
    login("登录"),
    register("注册");

    private String desc;

    PageId(String desc) {
        this.desc = desc;
    }
}
