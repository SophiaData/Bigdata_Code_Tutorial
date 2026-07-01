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
import io.sophiadata.flink.source.utils.RandomNum;
import io.sophiadata.flink.source.utils.RandomNumString;
import io.sophiadata.flink.source.utils.RandomOptionGroup;

import java.util.Objects;

/** (@sophiadata) (@date 2023/8/2 11:11). */
public class AppPage {

    private PageId lastPageId;
    private PageId pageId;
    private ItemType itemType;
    private String item;
    private Integer duringTime;
    private String extend1;
    private String extend2;
    private DisplayType sourceType;

    public AppPage(
            final PageId lastPageId,
            final PageId pageId,
            final ItemType itemType,
            final String item,
            final Integer duringTime,
            final String extend1,
            final String extend2,
            final DisplayType sourceType) {
        this.lastPageId = lastPageId;
        this.pageId = pageId;
        this.itemType = itemType;
        this.item = item;
        this.duringTime = duringTime;
        this.extend1 = extend1;
        this.extend2 = extend2;
        this.sourceType = sourceType;
    }

    public PageId getLastPageId() {
        return lastPageId;
    }

    public void setLastPageId(final PageId lastPageId) {
        this.lastPageId = lastPageId;
    }

    public PageId getPageId() {
        return pageId;
    }

    public void setPageId(final PageId pageId) {
        this.pageId = pageId;
    }

    public ItemType getItemType() {
        return itemType;
    }

    public void setItemType(final ItemType itemType) {
        this.itemType = itemType;
    }

    public String getItem() {
        return item;
    }

    public void setItem(final String item) {
        this.item = item;
    }

    public Integer getDuringTime() {
        return duringTime;
    }

    public void setDuringTime(final Integer duringTime) {
        this.duringTime = duringTime;
    }

    public String getExtend1() {
        return extend1;
    }

    public void setExtend1(final String extend1) {
        this.extend1 = extend1;
    }

    public String getExtend2() {
        return extend2;
    }

    public void setExtend2(final String extend2) {
        this.extend2 = extend2;
    }

    public DisplayType getSourceType() {
        return sourceType;
    }

    public void setSourceType(final DisplayType sourceType) {
        this.sourceType = sourceType;
    }

    public static AppPage build(
            final PageId pageId, final PageId lastPageId, final Integer duringTime) {

        ItemType itemType = null;
        String item = null;
        final String extend1 = null;
        final String extend2 = null;
        DisplayType sourceType = null;

        final RandomOptionGroup<DisplayType> sourceTypeGroup =
                RandomOptionGroup.<DisplayType>builder()
                        .add(DisplayType.query, AppConfig.SOURCE_TYPE_RATE[0])
                        .add(DisplayType.promotion, AppConfig.SOURCE_TYPE_RATE[1])
                        .add(DisplayType.recommend, AppConfig.SOURCE_TYPE_RATE[2])
                        .add(DisplayType.activity, AppConfig.SOURCE_TYPE_RATE[3])
                        .build();

        if (pageId == PageId.good_detail
                || pageId == PageId.good_spec
                || pageId == PageId.comment
                || pageId == PageId.comment_list) {

            sourceType = sourceTypeGroup.getValue();

            itemType = ItemType.sku_id;
            item = RandomNum.getRandInt(1, AppConfig.MAX_SKU_ID) + "";
        } else if (pageId == PageId.good_list) {
            itemType = ItemType.keyword;
            item = new RandomOptionGroup(AppConfig.SEARCH_KEYWORDS).getRandStringValue();
        } else if (pageId == PageId.trade
                || pageId == PageId.payment
                || pageId == PageId.payment_done) {
            itemType = ItemType.sku_ids;
            item =
                    RandomNumString.getRandNumString(
                            1, AppConfig.MAX_SKU_ID, RandomNum.getRandInt(1, 3), ",", false);
        }
        return new AppPage(
                lastPageId, pageId, itemType, item, duringTime, extend1, extend2, sourceType);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AppPage that = (AppPage) o;
        return lastPageId == that.lastPageId
                && pageId == that.pageId
                && itemType == that.itemType
                && Objects.equals(item, that.item)
                && Objects.equals(duringTime, that.duringTime)
                && Objects.equals(extend1, that.extend1)
                && Objects.equals(extend2, that.extend2)
                && sourceType == that.sourceType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                lastPageId, pageId, itemType, item, duringTime, extend1, extend2, sourceType);
    }

    @Override
    public String toString() {
        return "AppPage{lastPageId="
                + lastPageId
                + ", pageId="
                + pageId
                + ", itemType="
                + itemType
                + ", item='"
                + item
                + "', duringTime="
                + duringTime
                + '}';
    }
}
