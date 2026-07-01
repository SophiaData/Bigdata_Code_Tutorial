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

package io.sophiadata.flink.source.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** (@sophiadata) (@date 2023/8/2 11:19). */
public class RandomOptionGroup<T> {

    int totalWeight = 0;
    List<RanOpt<T>> optList = new ArrayList<>();

    public static <T> Builder<T> builder() {
        return new Builder<T>();
    }

    public static class Builder<T> {

        List<RanOpt<T>> optList = new ArrayList<>();

        int totalWeight = 0;

        public Builder<T> add(final T value, final int weight) {
            final RanOpt<T> ranOpt = new RanOpt<>(value, weight);
            totalWeight += weight;
            for (int i = 0; i < weight; i++) {
                optList.add(ranOpt);
            }
            return this;
        }

        public RandomOptionGroup<T> build() {
            return new RandomOptionGroup<T>(totalWeight, optList);
        }
    }

    public RandomOptionGroup(final int totalWeight, final List<RanOpt<T>> optList) {
        this.totalWeight = totalWeight;
        this.optList = optList;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public RandomOptionGroup(final String... values) {
        for (final String value : values) {
            totalWeight += 1;
            optList.add((RanOpt<T>) (RanOpt) new RanOpt<String>(value, 1));
        }
    }

    @SafeVarargs
    public RandomOptionGroup(final RanOpt<T>... opts) {
        for (final RanOpt<T> opt : opts) {
            totalWeight += opt.getWeight();
            for (int i = 0; i < opt.getWeight(); i++) {
                optList.add(opt);
            }
        }
    }

    public RandomOptionGroup(final int trueWeight, final int falseWeight) {
        this(
                new RanOpt<>((T) Boolean.TRUE, trueWeight),
                new RanOpt<>((T) Boolean.FALSE, falseWeight));
    }

    public RandomOptionGroup(final String trueRate) {
        this(ParamUtil.checkRatioNum(trueRate), 100 - ParamUtil.checkRatioNum(trueRate));
    }

    public T getValue() {
        final int i = new Random().nextInt(totalWeight);
        return optList.get(i).getValue();
    }

    public RanOpt<T> getRandomOpt() {
        final int i = new Random().nextInt(totalWeight);
        return optList.get(i);
    }

    public String getRandStringValue() {
        final int i = new Random().nextInt(totalWeight);
        return (String) optList.get(i).getValue();
    }

    public Integer getRandIntValue() {
        final int i = new Random().nextInt(totalWeight);
        return (Integer) optList.get(i).getValue();
    }

    public Boolean getRandBoolValue() {

        final int i = new Random().nextInt(totalWeight);
        return (Boolean) optList.get(i).getValue();
    }

    public static void main(final String[] args) {
        final RanOpt<String>[] opts =
                new RanOpt[] {
                    new RanOpt<>("zhang3", 20), new RanOpt<>("li4", 30), new RanOpt<>("wang5", 50)
                };
        final RandomOptionGroup<String> randomOptionGroup = new RandomOptionGroup<>(opts);
        for (int i = 0; i < 10; i++) {
            System.out.println(randomOptionGroup.getRandomOpt().getValue());
        }
    }
}
