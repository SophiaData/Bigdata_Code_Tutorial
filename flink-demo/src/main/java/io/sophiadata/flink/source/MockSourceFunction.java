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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.EnumUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/** (@sophiadata) (@date 2023/8/2 11:23). */
@Slf4j
public class MockSourceFunction implements ParallelSourceFunction<String> {

    private volatile Long ts;
    private volatile int mockCount;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        for (; mockCount < AppConfig.mock_count; mockCount++) {
            List<AppMain> appMainList = doAppMock();
            for (AppMain appMain : appMainList) {
                ctx.collect(appMain.toString());
                Thread.sleep(AppConfig.log_sleep);
            }
        }
    }

    @Override
    public void cancel() {
        mockCount = AppConfig.mock_count;
    }

    public List<AppMain> doAppMock() {
        List<AppMain> logList = new ArrayList<>();

        Date curDate = ParamUtil.checkDate(AppConfig.mock_date);
        ts = curDate.getTime();

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
        List<Map> pathList = JSON.parseArray(jsonFile, Map.class);
        RandomOptionGroup.Builder<List> builder = RandomOptionGroup.builder();

        // 抽取一个访问路径
        for (Map map : pathList) {
            List path = (List) map.get("path");
            Integer rate = (Integer) map.get("rate");
            builder.add(path, rate);
        }
        List chosenPath = builder.build().getRandomOpt().getValue();
        // ts+=appStart.getLoading_time() ;

        // 逐个输入日志
        // 每条日志  1 主行为  2 曝光  3 错误
        PageId lastPageId = null;
        for (Object o : chosenPath) {
            // common字段
            AppMain.AppMainBuilder pageBuilder = AppMain.builder().common(appCommon);

            String path = (String) o;

            int pageDuringTime = RandomNum.getRandInt(1000, AppConfig.page_during_max_ms);
            // 添加页面
            PageId pageId = EnumUtils.getEnum(PageId.class, path);
            AppPage page = AppPage.build(pageId, lastPageId, pageDuringTime);
            if (pageId == null) {
                System.out.println();
            }
            pageBuilder.page(page);
            // 置入上一个页面
            lastPageId = page.getPage_id();

            // 页面中的动作
            List<AppAction> appActionList = AppAction.buildList(page, ts, pageDuringTime);
            if (appActionList.size() > 0) {
                pageBuilder.actions(appActionList);
            }
            // 曝光
            List<AppDisplay> displayList = AppDisplay.buildList(page);
            if (displayList.size() > 0) {
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
