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

package io.sophiadata.flink.sync.utils;

/** (@SophiaData) (@date 2023/5/22 10:47). */
public class SnowflakeIdGeneratorUtil2 {
    // 机器id所占的位置
    private final long workerIdBits = 5L;
    // 数据标识id所占的位数
    private final long datacenterIdBits = 5L;
    // 数据中心ID(0~31)
    private final long datacenterId;
    // 机器ID(0~31)
    private final long workerId;
    // 毫秒内序列号(0~4095)
    private long sequence = 0L;
    // 上次生成ID的时间截
    private long lastTimestamp = -1L;

    /**
     * 构造函数
     *
     * @param datacenterId 数据中心ID (0~31)
     * @param workerId 机器ID (0~31)
     */
    public SnowflakeIdGeneratorUtil2(long datacenterId, long workerId) {
        // 支持的最大数据标识id，结果是31(二进制表示为11111)
        long maxDatacenterId = ~(-1L << datacenterIdBits);
        if (datacenterId > maxDatacenterId || datacenterId < 0) {
            throw new IllegalArgumentException("Invalid datacenterId");
        }
        // 支持的最大机器id，结果是31(二进制表示为11111)
        long maxWorkerId = ~(-1L << workerIdBits);
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException("Invalid workerId");
        }
        this.datacenterId = datacenterId;
        this.workerId = workerId;
    }

    /**
     * 生成唯一ID
     *
     * @return 唯一ID
     */
    public synchronized long nextId() {
        long timestamp = System.currentTimeMillis();
        if (timestamp < lastTimestamp) {
            throw new RuntimeException("Clock moved backwards. Refusing to generate id");
        }
        // 序列在id中占的位数
        long sequenceBits = 12L;
        if (lastTimestamp == timestamp) {
            // 生成序列的掩码，这里为4095(二进制表示为111111111111=0xfff)，支持的最大序列是4095
            long sequenceMask = ~(-1L << sequenceBits);
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }
        lastTimestamp = timestamp;
        // 开始时间戳
        long twepoch = 1288834974657L;
        // 机器ID向左移12位
        // 数据标识id向左移17位(12+5)
        long datacenterIdShift = sequenceBits + workerIdBits;
        // 时间截向左移22位(5+5+12)
        long timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;
        return ((timestamp - twepoch) << timestampLeftShift)
                | (datacenterId << datacenterIdShift)
                | (workerId << sequenceBits)
                | sequence;
    }

    /**
     * 阻塞到下一个毫秒，直到获得新的时间戳
     *
     * @param lastTimestamp 上次生成ID的时间截
     * @return 当前时间戳
     */
    private long tilNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }

    public static void main(String[] args) {
        SnowflakeIdGeneratorUtil2 snowflakeIdGeneratorUtil2 = new SnowflakeIdGeneratorUtil2(1, 1);
        long nextId = snowflakeIdGeneratorUtil2.nextId();
        System.out.println(nextId);
    }
}
