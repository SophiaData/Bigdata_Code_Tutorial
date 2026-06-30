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

package io.sophiadata.flink.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import com.alibaba.fastjson.JSON;
import io.sophiadata.flink.source.bean.AppAction;
import io.sophiadata.flink.source.bean.AppCommon;
import io.sophiadata.flink.source.bean.AppDisplay;
import io.sophiadata.flink.source.bean.AppMain;
import io.sophiadata.flink.source.bean.AppPage;
import io.sophiadata.flink.source.bean.AppStart;
import io.sophiadata.flink.source.config.AppConfig;
import io.sophiadata.flink.source.enums.PageId;
import io.sophiadata.flink.source.utils.ParamUtil;
import io.sophiadata.flink.source.utils.RandomNum;
import io.sophiadata.flink.source.utils.RandomOptionGroup;
import org.apache.commons.lang3.EnumUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** (@sophiadata) (@date 2023/8/2 11:23). */
@SuppressWarnings("deprecation")
public class MockSourceFunction implements ParallelSourceFunction<String> {

    private static final Logger log = LoggerFactory.getLogger(MockSourceFunction.class);
    private volatile Long ts;
    private volatile int mockCount;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        for (; mockCount < AppConfig.MOCK_COUNT; mockCount++) {
            List<AppMain> appMainList = doAppMock();
            for (AppMain appMain : appMainList) {
                ctx.collect(appMain.toString());
                Thread.sleep(AppConfig.LOG_SLEEP);
            }
        }
    }

    @Override
    public void cancel() {
        mockCount = AppConfig.MOCK_COUNT;
    }

    public List<AppMain> doAppMock() {
        List<AppMain> logList = new ArrayList<>();

        LocalDateTime curDate = ParamUtil.checkDateTime(AppConfig.MOCK_DATE);
        ts = curDate.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        AppMain.AppMainBuilder appMainBuilder = AppMain.builder();

        // 启动 数据
        AppCommon appCommon = AppCommon.build();
        appMainBuilder.common(appCommon);
        appMainBuilder.checkError();
        AppStart appStart = new AppStart.Builder().build();
        appMainBuilder.start(appStart);
        appMainBuilder.ts(ts);

        logList.add(appMainBuilder.build());

        String jsonFile =
                "[\n"
                        + "  {\"path\":[\"home\",\"good_list\",\"good_detail\",\"cart\",\"trade\",\"payment\"],\"rate\":20 },\n"
                        + "  {\"path\":[\"home\",\"search\",\"good_list\",\"good_detail\",\"login\",\"good_detail\",\"cart\",\"trade\",\"payment\"],\"rate\":50 },\n"
                        + "  {\"path\":[\"home\",\"mine\",\"orders_unpaid\",\"trade\",\"payment\"],\"rate\":10 },\n"
                        + "  {\"path\":[\"home\",\"mine\",\"orders_unpaid\",\"good_detail\",\"good_spec\",\"comment\",\"trade\",\"payment\"],\"rate\":5 },\n"
                        + "  {\"path\":[\"home\",\"mine\",\"orders_unpaid\",\"good_detail\",\"good_spec\",\"comment\",\"home\"],\"rate\":5 },\n"
                        + "  {\"path\":[\"home\",\"good_detail\"],\"rate\":70 },\n"
                        + "  {\"path\":[\"home\"  ],\"rate\":10 }\n"
                        + "]";
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> pathList =
                (List<Map<String, Object>>) (List<?>) JSON.parseArray(jsonFile);
        RandomOptionGroup.Builder<List<String>> builder = RandomOptionGroup.builder();

        // 抽取一个访问路径
        for (Map<String, Object> map : pathList) {
            List<String> path = (List<String>) map.get("path");
            Integer rate = (Integer) map.get("rate");
            builder.add(path, rate);
        }
        List<String> chosenPath = builder.build().getRandomOpt().getValue();
        // ts+=appStart.getLoading_time() ;

        // 逐个输入日志
        // 每条日志  1 主行为  2 曝光  3 错误
        PageId lastPageId = null;
        for (Object o : chosenPath) {
            // common字段
            AppMain.AppMainBuilder pageBuilder = AppMain.builder().common(appCommon);

            String path = (String) o;

            int pageDuringTime = RandomNum.getRandInt(1000, AppConfig.PAGE_DURING_MAX_MS);
            // 添加页面
            PageId pageId = EnumUtils.getEnum(PageId.class, path);
            AppPage page = AppPage.build(pageId, lastPageId, pageDuringTime);
            if (pageId == null) {
                log.warn("Unknown page id: {}", path);
            }
            pageBuilder.page(page);
            // 置入上一个页面
            lastPageId = page.getPageId();

            // 页面中的动作
            List<AppAction> appActionList = AppAction.buildList(page, ts, pageDuringTime);
            if (!appActionList.isEmpty()) {
                pageBuilder.actions(appActionList);
            }
            // 曝光
            List<AppDisplay> displayList = AppDisplay.buildList(page);
            if (!displayList.isEmpty()) {
                pageBuilder.displays(displayList);
            }
            pageBuilder.ts(ts);
            pageBuilder.checkError();
            logList.add(pageBuilder.build());
            // ts+= pageDuringTime ;
        }

        //  随机发送通知日志
        //        System.out.println(logList);

        return logList;
    }

    public static void main(String[] args) throws InterruptedException {
        // System.out.println(RandomStringUtils.random(16,true,true));
        new MockSourceFunction().doAppMock();
    }
}
