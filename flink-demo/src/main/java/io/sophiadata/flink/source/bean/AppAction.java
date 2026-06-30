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

import io.sophiadata.flink.source.config.AppConfig;
import io.sophiadata.flink.source.enums.ActionId;
import io.sophiadata.flink.source.enums.ItemType;
import io.sophiadata.flink.source.enums.PageId;
import io.sophiadata.flink.source.utils.RandomNum;
import io.sophiadata.flink.source.utils.RandomOptionGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** (@sophiadata) (@date 2023/8/2 11:05). */
public class AppAction {

    private ActionId actionId;
    private ItemType itemType;
    private String item;
    private String extend1;
    private String extend2;
    private long ts;

    public AppAction(ActionId actionId, ItemType itemType, String item) {
        this.actionId = actionId;
        this.itemType = itemType;
        this.item = item;
    }

    public ActionId getActionId() {
        return actionId;
    }

    public void setActionId(ActionId actionId) {
        this.actionId = actionId;
    }

    public ItemType getItemType() {
        return itemType;
    }

    public void setItemType(ItemType itemType) {
        this.itemType = itemType;
    }

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public String getExtend1() {
        return extend1;
    }

    public void setExtend1(String extend1) {
        this.extend1 = extend1;
    }

    public String getExtend2() {
        return extend2;
    }

    public void setExtend2(String extend2) {
        this.extend2 = extend2;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AppAction that = (AppAction) o;
        return actionId == that.actionId
                && itemType == that.itemType
                && Objects.equals(item, that.item)
                && Objects.equals(extend1, that.extend1)
                && Objects.equals(extend2, that.extend2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actionId, itemType, item, extend1, extend2);
    }

    @Override
    public String toString() {
        return "AppAction{actionId="
                + actionId
                + ", itemType="
                + itemType
                + ", item='"
                + item
                + "'}";
    }

    public static List<AppAction> buildList(AppPage appPage, Long startTs, Integer duringTime) {

        List<AppAction> actionList = new ArrayList<>();
        Boolean ifFavor =
                RandomOptionGroup.builder()
                        .add(true, AppConfig.IF_FAVOR_RATE)
                        .add(false, 100 - AppConfig.IF_FAVOR_RATE)
                        .build()
                        .getRandBoolValue();
        Boolean ifCart =
                RandomOptionGroup.builder()
                        .add(true, AppConfig.IF_CART_RATE)
                        .add(false, 100 - AppConfig.IF_CART_RATE)
                        .build()
                        .getRandBoolValue();
        Boolean ifCartAddNum =
                RandomOptionGroup.builder()
                        .add(true, AppConfig.IF_CART_ADD_NUM_RATE)
                        .add(false, 100 - AppConfig.IF_CART_ADD_NUM_RATE)
                        .build()
                        .getRandBoolValue();
        Boolean ifCartMinusNum =
                RandomOptionGroup.builder()
                        .add(true, AppConfig.IF_CART_MINUS_NUM_RATE)
                        .add(false, 100 - AppConfig.IF_CART_MINUS_NUM_RATE)
                        .build()
                        .getRandBoolValue();
        Boolean ifCartRm =
                RandomOptionGroup.builder()
                        .add(true, AppConfig.IF_CART_RM_RATE)
                        .add(false, 100 - AppConfig.IF_CART_RM_RATE)
                        .build()
                        .getRandBoolValue();
        Boolean ifGetCouponRm =
                RandomOptionGroup.builder()
                        .add(true, AppConfig.IF_GET_COUPON)
                        .add(false, 100 - AppConfig.IF_GET_COUPON)
                        .build()
                        .getRandBoolValue();
        if (appPage.getPageId() == PageId.good_detail) {

            if (ifFavor) {
                AppAction favorAction =
                        new AppAction(ActionId.favor_add, appPage.getItemType(), appPage.getItem());
                actionList.add(favorAction);
            }
            if (ifCart) {
                AppAction cartAction =
                        new AppAction(ActionId.cart_add, appPage.getItemType(), appPage.getItem());
                actionList.add(cartAction);
            }
            if (ifGetCouponRm) {
                int couponId = RandomNum.getRandInt(1, AppConfig.MAX_COUPON_ID);
                AppAction couponAction =
                        new AppAction(
                                ActionId.get_coupon, ItemType.coupon_id, String.valueOf(couponId));
                actionList.add(couponAction);
            }

        } else if (appPage.getPageId() == PageId.cart) {

            if (ifCartAddNum) {
                int skuId = RandomNum.getRandInt(1, AppConfig.MAX_SKU_ID);
                AppAction favorAction =
                        new AppAction(ActionId.cart_add_num, ItemType.sku_id, skuId + "");
                actionList.add(favorAction);
            }
            if (ifCartMinusNum) {
                int skuId = RandomNum.getRandInt(1, AppConfig.MAX_SKU_ID);
                AppAction favorAction =
                        new AppAction(ActionId.cart_minus_num, ItemType.sku_id, skuId + "");
                actionList.add(favorAction);
            }
            if (ifCartRm) {
                int skuId = RandomNum.getRandInt(1, AppConfig.MAX_SKU_ID);
                AppAction favorAction =
                        new AppAction(ActionId.cart_remove, ItemType.sku_id, skuId + "");
                actionList.add(favorAction);
            }

        } else if (appPage.getPageId() == PageId.trade) {
            Boolean ifAddAddress =
                    RandomOptionGroup.builder()
                            .add(true, AppConfig.IF_ADD_ADDRESS)
                            .add(false, 100 - AppConfig.IF_ADD_ADDRESS)
                            .build()
                            .getRandBoolValue();
            if (ifAddAddress) {
                AppAction appAction = new AppAction(ActionId.trade_add_address, null, null);
                actionList.add(appAction);
            }

        } else if (appPage.getPageId() == PageId.favor) {
            Boolean ifFavorCancel =
                    RandomOptionGroup.builder()
                            .add(true, AppConfig.IF_FAVOR_CANCEL_RATE)
                            .add(false, 100 - AppConfig.IF_FAVOR_CANCEL_RATE)
                            .build()
                            .getRandBoolValue();
            int skuId = RandomNum.getRandInt(1, AppConfig.MAX_SKU_ID);
            for (int i = 0; i < 3; i++) {
                if (ifFavorCancel) {
                    AppAction appAction =
                            new AppAction(ActionId.favor_canel, ItemType.sku_id, skuId + i + "");
                    actionList.add(appAction);
                }
            }
        }

        int size = actionList.size();
        long avgActionTime = duringTime / (size + 1);
        for (int i = 1; i <= actionList.size(); i++) {
            AppAction appAction = actionList.get(i - 1);
            appAction.setTs(startTs + i * avgActionTime);
        }
        return actionList;
    }
}
