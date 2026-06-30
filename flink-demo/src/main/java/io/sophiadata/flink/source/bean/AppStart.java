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

import io.sophiadata.flink.source.utils.RanOpt;
import io.sophiadata.flink.source.utils.RandomNum;
import io.sophiadata.flink.source.utils.RandomOptionGroup;

import java.util.Objects;

/** (@sophiadata) (@date 2023/8/2 11:12). */
public class AppStart {

    private final String entry;
    private final Long openAdId;
    private final Integer openAdMs;
    private final Integer openAdSkipMs;
    private final Integer loadingTime;

    public AppStart(
            String entry,
            Long openAdId,
            Integer openAdMs,
            Integer openAdSkipMs,
            Integer loadingTime) {
        this.entry = entry;
        this.openAdId = openAdId;
        this.openAdMs = openAdMs;
        this.openAdSkipMs = openAdSkipMs;
        this.loadingTime = loadingTime;
    }

    public String getEntry() {
        return entry;
    }

    public Long getOpenAdId() {
        return openAdId;
    }

    public Integer getOpenAdMs() {
        return openAdMs;
    }

    public Integer getOpenAdSkipMs() {
        return openAdSkipMs;
    }

    public Integer getLoadingTime() {
        return loadingTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AppStart that = (AppStart) o;
        return Objects.equals(entry, that.entry)
                && Objects.equals(openAdId, that.openAdId)
                && Objects.equals(openAdMs, that.openAdMs)
                && Objects.equals(openAdSkipMs, that.openAdSkipMs)
                && Objects.equals(loadingTime, that.loadingTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entry, openAdId, openAdMs, openAdSkipMs, loadingTime);
    }

    @Override
    public String toString() {
        return "AppStart{entry='"
                + entry
                + "', openAdId="
                + openAdId
                + ", openAdMs="
                + openAdMs
                + ", openAdSkipMs="
                + openAdSkipMs
                + ", loadingTime="
                + loadingTime
                + '}';
    }

    public static class Builder {
        private String entry;
        private Long openAdId;
        private Integer openAdMs;
        private Integer openAdSkipMs;
        private Integer loadingTime;
        private Integer firstOpen;

        public Builder() {
            entry =
                    new RandomOptionGroup<>(
                                    new RanOpt<>("install", 5),
                                    new RanOpt<>("icon", 75),
                                    new RanOpt<>("notice", 20))
                            .getRandStringValue();
            openAdId = RandomNum.getRandInt(1, 20) + 0L;
            openAdMs = RandomNum.getRandInt(1000, 10000);
            openAdSkipMs =
                    RandomOptionGroup.builder()
                            .add(0, 50)
                            .add(RandomNum.getRandInt(1000, openAdMs), 50)
                            .build()
                            .getRandIntValue();
            loadingTime = RandomNum.getRandInt(1000, 20000);
            firstOpen = RandomNum.getRandInt(0, 1);
        }

        public AppStart build() {
            return new AppStart(entry, openAdId, openAdMs, openAdSkipMs, loadingTime);
        }
    }
}
