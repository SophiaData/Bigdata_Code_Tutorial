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

import lombok.AllArgsConstructor;
import lombok.Builder;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** (@sophiadata) (@date 2023/8/2 11:19). */
@AllArgsConstructor
@Builder(builderClassName = "Builder")
public class RandomOptionGroup<T> {

    int totalWeight = 0;

    List<RanOpt> optList = new ArrayList();

    public static <T> Builder<T> builder() {
        return new Builder<T>();
    }

    public static class Builder<T> {

        List<RanOpt> optList = new ArrayList();

        int totalWeight = 0;

        public Builder add(T value, int weight) {
            RanOpt ranOpt = new RanOpt(value, weight);
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

    public RandomOptionGroup(String... values) {
        for (String value : values) {
            totalWeight += 1;
            optList.add(new RanOpt(value, 1));
        }
    }

    public RandomOptionGroup(RanOpt<T>... opts) {
        for (RanOpt opt : opts) {
            totalWeight += opt.getWeight();
            for (int i = 0; i < opt.getWeight(); i++) {
                optList.add(opt);
            }
        }
    }

    /* public   RandomOptionGroup(RanOpt<Boolean>... opts) {
        for (RanOpt opt : opts) {
            totalWeight+=opt.getWeight();
            for (int i = 0; i <opt.getWeight() ; i++) {
                optList.add(opt);
            }

        }
    }*/

    public RandomOptionGroup(int trueWeight, int falseWeight) {
        this(new RanOpt(true, trueWeight), new RanOpt(false, falseWeight));
    }

    public RandomOptionGroup(String trueRate) {
        this(ParamUtil.checkRatioNum(trueRate), 100 - ParamUtil.checkRatioNum(trueRate));
    }

    public T getValue() {
        int i = new Random().nextInt(totalWeight);
        return (T) optList.get(i).getValue();
    }

    public RanOpt<T> getRandomOpt() {
        int i = new Random().nextInt(totalWeight);
        return optList.get(i);
    }

    public String getRandStringValue() {
        int i = new Random().nextInt(totalWeight);
        return (String) optList.get(i).getValue();
    }

    public Integer getRandIntValue() {
        int i = new Random().nextInt(totalWeight);
        return (Integer) optList.get(i).getValue();
    }

    public Boolean getRandBoolValue() {

        int i = new Random().nextInt(totalWeight);
        return (Boolean) optList.get(i).getValue();
    }

    public static void main(String[] args) {
        RanOpt[] opts = {new RanOpt("zhang3", 20), new RanOpt("li4", 30), new RanOpt("wang5", 50)};
        RandomOptionGroup randomOptionGroup = new RandomOptionGroup(opts);
        for (int i = 0; i < 10; i++) {
            System.out.println(randomOptionGroup.getRandomOpt().getValue());
        }
    }
}
