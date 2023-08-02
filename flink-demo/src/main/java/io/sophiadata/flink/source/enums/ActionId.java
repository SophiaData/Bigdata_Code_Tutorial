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

/** (@sophiadata) (@date 2023/8/2 11:20). */
public enum ActionId {
    favor_add("添加收藏"),
    favor_canel("取消收藏"),
    cart_add("添加购物车"),
    cart_remove("删除购物车"),
    cart_add_num("增加购物车商品数量"),
    cart_minus_num("减少购物车商品数量"),
    trade_add_address("增加收货地址"),
    get_coupon("领取优惠券");

    private String desc;

    ActionId(String desc) {
        this.desc = desc;
    }
}
