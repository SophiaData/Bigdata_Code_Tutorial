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
import lombok.AllArgsConstructor;
import lombok.Data;

/** (@sophiadata) (@date 2023/8/2 11:11). */
@Data
@AllArgsConstructor
public class AppPage {

    PageId last_page_id;

    PageId page_id;

    ItemType item_type;

    String item;

    Integer during_time;

    String extend1;

    String extend2;

    DisplayType source_type;

    public static AppPage build(PageId pageId, PageId lastPageId, Integer duringTime) {

        ItemType itemType = null;
        String item = null;
        String extend1 = null;
        String extend2 = null;
        DisplayType sourceType = null;

        RandomOptionGroup<DisplayType> sourceTypeGroup =
                RandomOptionGroup.<DisplayType>builder()
                        .add(DisplayType.query, AppConfig.sourceTypeRate[0])
                        .add(DisplayType.promotion, AppConfig.sourceTypeRate[1])
                        .add(DisplayType.recommend, AppConfig.sourceTypeRate[2])
                        .add(DisplayType.activity, AppConfig.sourceTypeRate[3])
                        .build();

        if (pageId == PageId.good_detail
                || pageId == PageId.good_spec
                || pageId == PageId.comment
                || pageId == PageId.comment_list) {

            sourceType = sourceTypeGroup.getValue();

            itemType = ItemType.sku_id;
            item = RandomNum.getRandInt(1, AppConfig.max_sku_id) + "";
        } else if (pageId == PageId.good_list) {
            itemType = ItemType.keyword;
            item = new RandomOptionGroup(AppConfig.searchKeywords).getRandStringValue();
        } else if (pageId == PageId.trade
                || pageId == PageId.payment
                || pageId == PageId.payment_done) {
            itemType = ItemType.sku_ids;
            item =
                    RandomNumString.getRandNumString(
                            1, AppConfig.max_sku_id, RandomNum.getRandInt(1, 3), ",", false);
        }
        return new AppPage(
                lastPageId, pageId, itemType, item, duringTime, extend1, extend2, sourceType);
    }
}
