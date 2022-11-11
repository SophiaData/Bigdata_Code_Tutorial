## MapJoin

当处理的数据量比较小的时候，可以采取 map join ，此时 hive 会将小表全部加载至内存中在 map 端进行 join 从而避免在 reducer 处理数据，如不符合 map join 条件，hive 解析器会将 join 
操作转换为 common join 即在 reduce 端进行 join 这样会增加数据倾斜的概率。

## 行列过滤

由于生产中 hive 分区表的数据通常会有亿级的数据量，此时合适的行列过滤操作对于数据处理来讲是非常必要的
对于列来讲，只拿需要的列，使用分区过滤，避免使用 select *
对于行来讲，在分区裁剪中，在进行外关联时，如果将副表的过滤条件写在 where 后面，那么就会先全表关联，再进行过滤操作。

## 列式存储

对于大数据存储来将，合适的列存储格式是十分必要的

## 采用分区技术

## 合理设置Map数

mapred.min.split.size: 指的是数据的最小分割单元大小；min的默认值是1B

mapred.max.split.size: 指的是数据的最大分割单元大小；max的默认值是256MB

通过调整max可以起到调整map数的作用，减小max可以增加map数，增大max可以减少map数。

需要提醒的是，直接调整mapred.map.tasks这个参数是没有效果的。

https://www.cnblogs.com/swordfall/p/11037539.html

## 合理设置Reduce数

Reduce个数并不是越多越好

（1）过多的启动和初始化Reduce也会消耗时间和资源；

（2）另外，有多少个Reduce，就会有多少个输出文件，如果生成了很多个小文件，那么如果这些小文件作为下一个任务的输入，则也会出现小文件过多的问题；

在设置Reduce个数的时候也需要考虑这两个原则：处理大数据量利用合适的Reduce数；使单个Reduce任务处理数据量大小要合适；

## 小文件如何产生的？

（1）动态分区插入数据，产生大量的小文件，从而导致map数量剧增；

（2）reduce数量越多，小文件也越多（reduce的个数和输出文件是对应的）；

（3）数据源本身就包含大量的小文件。

## 小文件解决方案

（1）在Map执行前合并小文件，减少Map数：CombineHiveInputFormat具有对小文件进行合并的功能（系统默认的格式）。HiveInputFormat没有对小文件合并功能。

（2）merge
// 输出合并小文件
```
SET hive.merge.mapfiles = true; -- 默认true，在map-only任务结束时合并小文件
SET hive.merge.mapredfiles = true; -- 默认false，在map-reduce任务结束时合并小文件
SET hive.merge.size.per.task = 268435456; -- 默认256M
SET hive.merge.smallfiles.avgsize = 16777216; -- 当输出文件的平均大小小于16m该值时，启动一个独立的map-reduce任务进行文件merge
```
（3）开启JVM重用
```
set mapreduce.job.jvm.numtasks=10
```
## 开启map端combiner（不影响最终业务逻辑）
```
set hive.map.aggr=true；
```
## 压缩（选择快的）
设置map端输出、中间结果压缩。（不完全是解决数据倾斜的问题，但是减少了IO读写和网络传输，能提高很多效率）
```
set hive.exec.compress.intermediate=true --启用中间数据压缩
set mapreduce.map.output.compress=true --启用最终数据压缩
set mapreduce.map.outout.compress.codec=…; --设置压缩方式
```
## 采用tez引擎或者spark引擎

通过配置 hive on tez 或者 hive on spark 提升数据处理效率




