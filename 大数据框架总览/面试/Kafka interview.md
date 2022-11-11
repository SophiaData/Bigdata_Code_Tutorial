## Kafka 如何进行压力测试？

Kafka官方自带压力测试脚本（kafka-consumer-perf-test.sh、kafka-producer-perf-test.sh）。Kafka压测时，可以查看到哪个地方出现了瓶颈（CPU，内存，网络IO）。一般都是网络IO达到瓶颈。

## Kafka 日志保存时间多久合适？

默认是7天，生产环境一般调整为3天。

## Kafka 的数据量应该如何计算呢？

每天总数据量100g，每天产生1亿条日志， 10000万/24/60/60=1150条/每秒钟
平均每秒钟：1150条
低谷每秒钟：50条
高峰每秒钟：1150条*（2-20倍）=2300条-23000条
每条日志大小：0.5k-2k（取1k）
每秒多少数据量：2.0M-20MB

## Kafka 的硬盘大小应该为多少？

每天的数据量100g*2个副本*3天/70%

## Kafka 有多少个 topic？

通常情况：多少个日志类型就多少个Topic。也有对日志类型进行合并的。
一般还有各种测试和废弃的topic，几十个到上百个都很正常，常用的会少一些。

## Kafka 的分区分配策略是什么？

> Select between the "range" or "roundrobin" strategy for assigning partitions to consumer streams.
The round-robin partition assignor lays out all the available partitions and all the available consumer threads. It then proceeds to do a round-robin assignment from partition to consumer thread. If the subscriptions of all consumer instances are identical, then the partitions will be uniformly distributed. (i.e., the partition ownership counts will be within a delta of exactly one across all consumer threads.) Round-robin assignment is permitted only if: (a) Every topic has the same number of streams within a consumer instance (b) The set of subscribed topics is identical for every consumer instance within the group.
Range partitioning works on a per-topic basis. For each topic, we lay out the available partitions in numeric order and the consumer threads in lexicographic order. We then divide the number of partitions by the total number of consumer streams (threads) to determine the number of partitions to assign to each consumer. If it does not evenly divide, then the first few consumers will have one extra partition.

默认参数为range

![image](https://user-images.githubusercontent.com/34996528/148711590-0e498bf9-86d4-47db-8555-9fec538940e4.png)


roundrobin 使用前提：
（a）每个主题在使用者实例中具有相同数量的流（b）集合 对于该组中的每个消费者实例，已订阅主题的数量都是相同的。
