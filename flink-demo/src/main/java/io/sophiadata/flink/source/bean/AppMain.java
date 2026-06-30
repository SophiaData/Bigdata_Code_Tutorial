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

import com.alibaba.fastjson.JSON;
import io.sophiadata.flink.source.config.AppConfig;
import io.sophiadata.flink.source.utils.RandomOptionGroup;

import java.util.List;
import java.util.Objects;

/** (@sophiadata) (@date 2023/8/2 11:09). */
public class AppMain {

    private Long ts;
    private AppCommon common;
    private AppPage page;
    private AppError err;
    private AppNotice notice;
    private AppStart start;
    private List<AppDisplay> displays;
    private List<AppAction> actions;

    public AppMain(
            final Long ts,
            final AppCommon common,
            final AppPage page,
            final AppError err,
            final AppNotice notice,
            final AppStart start,
            final List<AppDisplay> displays,
            final List<AppAction> actions) {
        this.ts = ts;
        this.common = common;
        this.page = page;
        this.err = err;
        this.notice = notice;
        this.start = start;
        this.displays = displays;
        this.actions = actions;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(final Long ts) {
        this.ts = ts;
    }

    public AppCommon getCommon() {
        return common;
    }

    public void setCommon(final AppCommon common) {
        this.common = common;
    }

    public AppPage getPage() {
        return page;
    }

    public void setPage(final AppPage page) {
        this.page = page;
    }

    public AppError getErr() {
        return err;
    }

    public void setErr(final AppError err) {
        this.err = err;
    }

    public AppNotice getNotice() {
        return notice;
    }

    public void setNotice(final AppNotice notice) {
        this.notice = notice;
    }

    public AppStart getStart() {
        return start;
    }

    public void setStart(final AppStart start) {
        this.start = start;
    }

    public List<AppDisplay> getDisplays() {
        return displays;
    }

    public void setDisplays(final List<AppDisplay> displays) {
        this.displays = displays;
    }

    public List<AppAction> getActions() {
        return actions;
    }

    public void setActions(final List<AppAction> actions) {
        this.actions = actions;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AppMain that = (AppMain) o;
        return Objects.equals(ts, that.ts)
                && Objects.equals(common, that.common)
                && Objects.equals(page, that.page)
                && Objects.equals(err, that.err)
                && Objects.equals(notice, that.notice)
                && Objects.equals(start, that.start)
                && Objects.equals(displays, that.displays)
                && Objects.equals(actions, that.actions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ts, common, page, err, notice, start, displays, actions);
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    public static AppMainBuilder builder() {
        return new AppMainBuilder();
    }

    public static class AppMainBuilder {
        private Long ts;
        private AppCommon common;
        private AppPage page;
        private AppError err;
        private AppNotice notice;
        private AppStart start;
        private List<AppDisplay> displays;
        private List<AppAction> actions;

        public AppMainBuilder ts(final Long ts) {
            this.ts = ts;
            return this;
        }

        public AppMainBuilder common(final AppCommon common) {
            this.common = common;
            return this;
        }

        public AppMainBuilder page(final AppPage page) {
            this.page = page;
            return this;
        }

        public AppMainBuilder err(final AppError err) {
            this.err = err;
            return this;
        }

        public AppMainBuilder notice(final AppNotice notice) {
            this.notice = notice;
            return this;
        }

        public AppMainBuilder start(final AppStart start) {
            this.start = start;
            return this;
        }

        public AppMainBuilder displays(final List<AppDisplay> displays) {
            this.displays = displays;
            return this;
        }

        public AppMainBuilder actions(final List<AppAction> actions) {
            this.actions = actions;
            return this;
        }

        public void checkError() {
            final Integer errorRate = AppConfig.ERROR_RATE;
            final Boolean ifError =
                    RandomOptionGroup.builder()
                            .add(true, errorRate)
                            .add(false, 100 - errorRate)
                            .build()
                            .getRandBoolValue();
            if (ifError) {
                final AppError appError = AppError.build();
                this.err = appError;
            }
        }

        public AppMain build() {
            return new AppMain(ts, common, page, err, notice, start, displays, actions);
        }
    }
}
