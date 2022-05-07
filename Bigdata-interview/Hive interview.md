## Hive的架构

Hive元数据默认存储在derby数据库，不支持多客户端访问，所以将元数据存储在MySQl，支持多客户端访问。

<img width="236" alt="image" src="https://user-images.githubusercontent.com/34996528/164068515-09016e87-362d-4b98-a16f-585bc83668ae.png">


## Hive和数据库比较

Hive 和数据库除了拥有类似的查询语言，再无类似之处。
1）数据存储位置
Hive 存储在 HDFS 。数据库将数据保存在块设备或者本地文件系统中。
2）数据更新
Hive中不建议对数据的改写。而数据库中的数据通常是需要经常进行修改的， 
3）执行延迟
Hive 执行延迟较高。数据库的执行延迟较低。当然，这个是有条件的，即数据规模较小，当数据规模大到超过数据库的处理能力的时候，Hive的并行计算显然能体现出优势。
4）数据规模
Hive支持很大规模的数据计算；数据库可以支持的数据规模较小。


## 内部表和外部表

元数据、原始数据
1）删除数据时：
内部表：元数据、原始数据，全删除
外部表：元数据 只删除
2）在公司生产环境下，什么时候创建内部表，什么时候创建外部表？
在公司中绝大多数场景都是外部表。
自己使用的临时表，才会创建内部表；

## 4个By区别

1）Order By：全局排序，只有一个Reducer；
2）Sort By：分区内有序；
3）Distrbute By：类似MR中Partition，进行分区，结合sort by使用。
4） Cluster By：当Distribute by和Sorts by字段相同时，可以使用Cluster by方式。Cluster by除了具有Distribute by的功能外还兼具Sort by的功能。但是排序只能是升序排序，不能指定排序规则为ASC或者DESC。
在生产环境中Order By用的比较少，容易导致OOM。
在生产环境中Sort By + Distrbute By用的多。

## 系统函数

1）date_add、date_sub函数（加减日期）
2）next_day函数（周指标相关）
3）date_format函数（根据格式整理日期）
4）last_day函数（求当月最后一天日期）
5）collect_set函数
6）get_json_object解析json函数
7）NVL（表达式1，表达式2）
如果表达式1为空值，NVL返回值为表达式2的值，否则返回表达式1的值。

## 自定义UDF、UDTF函数

1）在项目中自定义过UDF、UDTF函数，他们处理了什么问题，自定义步骤？
（1）用UDF函数解析公共字段；用UDTF函数解析事件字段。
（2）自定义UDF：继承UDF，重写evaluate方法
（3）自定义UDTF：继承自GenericUDTF，重写3个方法：initialize(自定义输出的列名和类型)，process（将结果返回forward(result)），close
2）为什么要自定义UDF/UDTF？
因为自定义函数，可以自己埋点Log打印日志，出错或者数据异常，方便调试。

## 窗口函数

1）Rank
（1）RANK() 排序相同时会重复，总数不会变
（2）DENSE_RANK() 排序相同时会重复，总数会减少
（3）ROW_NUMBER() 会根据顺序计算
2） OVER()：指定分析函数工作的数据窗口大小，这个数据窗口大小可能会随着行的变而变化
（1）CURRENT ROW：当前行
（2）n PRECEDING：往前n行数据
（3） n FOLLOWING：往后n行数据
（4）UNBOUNDED：起点，UNBOUNDED PRECEDING 表示从前面的起点， UNBOUNDED FOLLOWING表示到后面的终点
（5） LAG(col,n)：往前第n行数据
（6）LEAD(col,n)：往后第n行数据
（7） NTILE(n)：把有序分区中的行分发到指定数据的组中，各个组有编号，编号从1开始，对于每一行，NTILE返回此行所属的组的编号。注意：n必须为int类型。







