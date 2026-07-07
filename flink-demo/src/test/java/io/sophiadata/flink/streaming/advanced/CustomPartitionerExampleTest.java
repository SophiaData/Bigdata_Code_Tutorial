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

package io.sophiadata.flink.streaming.advanced;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CustomPartitionerExampleTest {

    @Test
    void shouldCreateTuple3WithRegion() {
        final CustomPartitionerExample.Tuple3WithRegion tuple =
                new CustomPartitionerExample.Tuple3WithRegion("order_1", "user_A", "north");
        assertThat(tuple.orderId).isEqualTo("order_1");
        assertThat(tuple.userId).isEqualTo("user_A");
        assertThat(tuple.region).isEqualTo("north");
    }

    @Test
    void shouldPartitionByRegion() {
        final CustomPartitionerExample.RegionPartitioner partitioner =
                new CustomPartitionerExample.RegionPartitioner();

        assertThat(partitioner.partition("north", 4)).isEqualTo(0);
        assertThat(partitioner.partition("south", 4)).isEqualTo(1);
        assertThat(partitioner.partition("east", 4)).isEqualTo(2);
        assertThat(partitioner.partition("west", 4)).isEqualTo(3);
    }

    @Test
    void shouldHandleUnknownRegion() {
        final CustomPartitionerExample.RegionPartitioner partitioner =
                new CustomPartitionerExample.RegionPartitioner();

        final int partition = partitioner.partition("unknown", 4);
        assertThat(partition).isGreaterThanOrEqualTo(0).isLessThan(4);
    }
}
