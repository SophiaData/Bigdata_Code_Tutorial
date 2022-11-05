## DML 操作

### 数据导入

#### 向表中装载数据

1）语法
```
hive (default)> drop table dept;
hive> load data [local] inpath '数据的  path' [overwrite] into table 
student [partition (partcol1=val1,…)];
```
（1）load data:表示加载数据

（2）local:表示从本地加载数据到 hive 表；否则从 HDFS 加载数据到 hive 表

（3）inpath:表示加载数据的路径

（4）overwrite:表示覆盖表中已有数据，否则表示追加

（5）into table:表示加载到哪张表

（6）student:表示具体的表

（7）partition:表示上传到指定分区

2）实操案例

（0）创建一张表
```
hive (default)> create table student(id string, name string) row format 
delimited fields terminated by '\t';
```
（1）加载本地文件到  hive

（2）加载  HDFS 文件到  hive 中 

上传文件到 HDFS
```
hive (default)> load data local inpath
'/opt/module/hive/datas/student.txt' into table default.student;
```

加载 HDFS 上数据
```
hive (default)> dfs -put /opt/module/hive/data/student.txt 
/user/atguigu/hive;
hive (default)> load data inpath '/user/atguigu/hive/student.txt' into 
table default.student;
```

（3）加载数据覆盖表中已有的数据 

上传文件到 HDFS
```
hive (default)> dfs -put /opt/module/data/student.txt /user/atguigu/hive;
```

加载数据覆盖表中已有的数据
```
hive (default)> load data inpath '/user/atguigu/hive/student.txt' 
overwrite into table default.student;
```
#### 通过查询语句向表中插入数据（Insert）

1）创建一张表

```
hive (default)> create table student_par(id int, name string) row format 
delimited fields terminated by '\t';
```

2）基本插入数据
```
hive (default)> insert into table  student_par 
values(1,'wangwu'),(2,'zhaoliu');
hive (default)> insert overwrite table student_par
select id, name from student where month='201709';
```

3）基本模式插入（根据单张表查询结果）

insert into：以追加数据的方式插入到表或分区，原有数据不会删除 

insert overwrite：会覆盖表中已存在的数据

注意：insert 不支持插入部分字段

4）多表（多分区）插入模式（根据多张表查询结果）

```
hive (default)> from student
insert overwrite table student partition(month='201707') 
select id, name where month='201709'
insert overwrite table student partition(month='201706') 
select id, name where month='201709';
```
####  查询语句中创建表并加载数据（As Select）

根据查询结果创建表（查询的结果会添加到新创建的表中）
```
create table if not exists student3 
as select id, name from student;
```

####  创建表时通过 Location 指定加载数据路径

1）上传数据到 hdfs 上

```
hive (default)> dfs -mkdir /student;
hive (default)> dfs -put /opt/module/datas/student.txt /student;
```

2）创建表，并指定在 hdfs 上的位置

```
hive (default)> create external table if not exists student5(
id int, name string 
)
row format delimited fields terminated by '\t' 
location '/student;
```

3）查询数据

```
hive (default)> select * from student5;
```

#### Import 数据到指定  Hive 表中

注意：先用  export 导出后，再将数据导入。

```
hive (default)> import table student2 
from '/user/hive/warehouse/export/student';
```

### 数据导出

#### Insert 导出

1）将查询的结果导出到本地

```
hive (default)> insert overwrite local directory 
'/opt/module/hive/data/export/student'
select * from student;
```

2）将查询的结果格式化导出到本地

```
hive(default)>insert overwrite local directory 
'/opt/module/hive/data/export/student1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
select * from student;
```

3）将查询的结果导出到  HDFS 上(没有  local)

```
hive (default)> insert overwrite directory '/user/atguigu/student2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
select * from student;
```

#### Hadoop 命令导出到本地
```
hive (default)> dfs -get /user/hive/warehouse/student/student.txt 
/opt/module/data/export/student3.txt;

```

#### Hive Shell  命令导出

基本语法：（hive -f/-e  执行语句或者脚本   > file）
```
[xxx@hadoop102 hive]$ bin/hive -e 'select * from default.student;' > 
/opt/module/hive/data/export/student4.txt;
```
#### Export 导出到  HDFS 上

```
(defahiveult)> export table default.student 
to '/user/hive/warehouse/export/student';
```
export 和 import 主要用于两个 Hadoop 平台集群之间  Hive 表迁移。

#### Sqoop 导出

参照 sqoop 文档

####  清除表中数据（Truncate）

注意：Truncate 只能删除管理表，不能删除外部表中数据

```
hive (default)> truncate table student;
```

## 查询

https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select

查询语句语法：
```
SELECT [ALL | DISTINCT] select_expr, select_expr, ... 
FROM table_reference
[WHERE where_condition] 
[GROUP BY col_list] 
[ORDER BY col_list] 
[CLUSTER BY col_list
| [DISTRIBUTE BY col_list] [SORT BY col_list] 
]
[LIMIT number]
```
### 基本查询（Select…From）

####  全表和特定列查询

0）数据准备

（0）原始数据 

dept:

```
10 ACCOUNTING 1700
20 RESEARCH 1800
30 SALES  1900
40 OPERATIONS 1700
```

emp：

```
7369  SMITH  CLERK  7902   1980-12-17 800.00     20
7499   ALLEN  SALESMAN  7698  1981-2-20  1600.00   300.00   30
7521   WARD   SALESMAN  7698  1981-2-22   1250.00   500.00   30
7566   JONES  MANAGER     7839   1981-4-2  2975.00    20
7521   MARTIN SALESMAN   7698   1981-9-28  1250.00 1400.00 30
7698 BLAKE MANAGER 7839 1981-5-1  2850.00  30
7782 CLARK MANAGER 7839 1981-6-9  2450.00 10 
7788   SCOTT  ANALYST 7566   1987-4-19  3000.00    20
7839 KING  PRESIDENT    1981-11-17 5000.00    10
7844 TURNER SALESMAN  7698   1981-9-8   1500.00 0.00   30
7876  ADAMS  CLERK  7788  1987-5-23 1100.00 20
7900  JAMES   CLERK  7698  1981-12-3 950.00 30
7902   FORD  ANALYST  7566  1981-12-3  3000.00    20
7934   MILLER  CLERK  7782  1982-1-23  1300.00    10

```

（1）创建部门表

```
create table if not exists dept( 
deptno int,
dname string, 
loc int
)
row format delimited fields terminated by '\t';
```

（2）创建员工表

```
create table if not exists emp( 
empno int,
ename string, 
job string,
mgr int, 
hiredate string, 
sal double, 
comm double, 
deptno int)
row format delimited fields terminated by '\t';
```

（3）导入数据

```
load data local inpath '/opt/module/datas/dept.txt' into table 
dept;
load data local inpath '/opt/module/datas/emp.txt' into table emp;
```

1）全表查询

```
hive (default)> select * from emp;
hive (default)> select empno,ename,job,mgr,hiredate,sal,comm,deptno from 
emp ;
```

2）选择特定列查询

```
hive (default)> select empno, ename from emp;
```
注意：
（1）SQL  语言大小写不敏感。

（2）SQL  可以写在一行或者多行。

（3）关键字不能被缩写也不能分行。

（4）各子句一般要分行写。

（5）使用缩进提高语句的可读性。

#### 列别名

1）重命名一个列

2）便于计算

3）紧跟列名，也可以在列名和别名之间加入关键字‘AS’

4）案例实操

查询名称和部门

```
hive (default)> select ename AS name, deptno dn from emp;
```

