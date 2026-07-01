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
import io.sophiadata.flink.source.enums.DisplayType;
import io.sophiadata.flink.source.enums.ItemType;
import io.sophiadata.flink.source.enums.PageId;
import io.sophiadata.flink.source.utils.ParamUtil;
import io.sophiadata.flink.source.utils.RandomNum;
import io.sophiadata.flink.source.utils.RandomOptionGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.sophiadata.flink.source.config.AppConfig.MAX_ACTIVITY_COUNT;
import static io.sophiadata.flink.source.config.AppConfig.MAX_DISPLAY_COUNT;
import static io.sophiadata.flink.source.config.AppConfig.MAX_POS_ID;
import static io.sophiadata.flink.source.config.AppConfig.MAX_SKU_ID;
import static io.sophiadata.flink.source.config.AppConfig.MIN_DISPLAY_COUNT;

/** (@gtk) (@date 2023/8/2 11:08). */
public class AppDisplay {

    private ItemType itemType;
    private String item;
    private DisplayType displayType;
    private Integer order;
    private Integer posId;

    public AppDisplay(
            ItemType itemType, String item, DisplayType displayType, Integer order, Integer posId) {
        this.itemType = itemType;
        this.item = item;
        this.displayType = displayType;
        this.order = order;
        this.posId = posId;
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

    public DisplayType getDisplayType() {
        return displayType;
    }

    public void setDisplayType(DisplayType displayType) {
        this.displayType = displayType;
    }

    public Integer getOrder() {
        return order;
    }

    public void setOrder(Integer order) {
        this.order = order;
    }

    public Integer getPosId() {
        return posId;
    }

    public void setPosId(Integer posId) {
        this.posId = posId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AppDisplay that = (AppDisplay) o;
        return Objects.equals(itemType, that.itemType)
                && Objects.equals(item, that.item)
                && Objects.equals(displayType, that.displayType)
                && Objects.equals(order, that.order)
                && Objects.equals(posId, that.posId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemType, item, displayType, order, posId);
    }

    @Override
    public String toString() {
        return "AppDisplay{itemType="
                + itemType
                + ", item='"
                + item
                + "', displayType="
                + displayType
                + ", order="
                + order
                + ", posId="
                + posId
                + '}';
    }

    public static List<AppDisplay> buildList(AppPage appPage) {

        List<AppDisplay> displayList = new ArrayList<>();
        Boolean isSkew = ParamUtil.checkBoolean(AppConfig.MOCK_SKEW);
        RandomOptionGroup isSkewRandom =
                RandomOptionGroup.builder().add(true, 80).add(false, 20).build();

        appendActivities(appPage, displayList);
        appendSkus(appPage, displayList, isSkew, isSkewRandom);

        return displayList;
    }

    /** 促销活动：首页、发现页、分类页 */
    private static void appendActivities(AppPage appPage, List<AppDisplay> displayList) {
        if (appPage.getPageId() != PageId.home
                && appPage.getPageId() != PageId.discovery
                && appPage.getPageId() != PageId.category) {
            return;
        }
        int displayCount = RandomNum.getRandInt(1, MAX_ACTIVITY_COUNT);
        int posId = RandomNum.getRandInt(1, MAX_POS_ID);
        for (int i = 1; i <= displayCount; i++) {
            int actId = RandomNum.getRandInt(1, MAX_ACTIVITY_COUNT);
            displayList.add(
                    new AppDisplay(
                            ItemType.activity_id, actId + "", DisplayType.activity, i, posId));
        }
    }

    /** 非促销活动曝光：商品列表 */
    private static void appendSkus(
            AppPage appPage,
            List<AppDisplay> displayList,
            Boolean isSkew,
            RandomOptionGroup isSkewRandom) {
        if (!isSkuPage(appPage)) {
            return;
        }
        int displayCount = RandomNum.getRandInt(MIN_DISPLAY_COUNT, MAX_DISPLAY_COUNT);
        int offset = displayList.size();
        for (int i = 1 + offset; i <= displayCount + offset; i++) {
            int skuId = resolveSkuId(appPage, isSkew, isSkewRandom);
            int posId = RandomNum.getRandInt(1, MAX_POS_ID);
            RandomOptionGroup<DisplayType> dispTypeGroup =
                    RandomOptionGroup.<DisplayType>builder()
                            .add(DisplayType.promotion, 30)
                            .add(DisplayType.query, 60)
                            .add(DisplayType.recommend, 10)
                            .build();
            displayList.add(
                    new AppDisplay(
                            ItemType.sku_id, skuId + "", dispTypeGroup.getValue(), i, posId));
        }
    }

    private static boolean isSkuPage(AppPage appPage) {
        return appPage.getPageId() == PageId.good_detail
                || appPage.getPageId() == PageId.home
                || appPage.getPageId() == PageId.category
                || appPage.getPageId() == PageId.activity
                || appPage.getPageId() == PageId.good_spec
                || appPage.getPageId() == PageId.good_list
                || appPage.getPageId() == PageId.discovery;
    }

    private static int resolveSkuId(
            AppPage appPage, Boolean isSkew, RandomOptionGroup isSkewRandom) {
        if (appPage.getPageId() == PageId.good_detail
                && isSkew
                && isSkewRandom.getRandBoolValue()) {
            return MAX_SKU_ID / 2;
        }
        return RandomNum.getRandInt(1, MAX_SKU_ID);
    }
}
