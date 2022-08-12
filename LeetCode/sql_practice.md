### [LeetCode SQL 入门](https://leetcode.cn/study-plan/sql/?progress=kyspxfh)

#### 595 大的国家

World 表:

| Column Name   | Type        | 
|---------------|-------------|
| name          | varchar     |
| continent     | varchar     |
| area          | int         |
| population    | int         |
| gdp           | int         |

name 是这张表的主键。

这张表的每一行提供：国家名称、所属大陆、面积、人口和 GDP 值。

如果一个国家满足下述两个条件之一，则认为该国是 大国 ：

面积至少为 300 万平方公里（即，3000000 km2），或者
人口至少为 2500 万（即 25000000）
编写一个 SQL 查询以报告 大国 的国家名称、人口和面积。

按 任意顺序 返回结果表。

- Result SQL:

```
select
name,
population,
area
from world where area >= 3000000 or population >= 25000000;
```
----------------

#### 1757 可回收且低脂的产品

表：Products

| Column Name | Type    |
|-------------|---------|
| product_id  | int     |
| low_fats    | enum    |
| recyclable  | enum    |

product_id 是这个表的主键。
low_fats 是枚举类型，取值为以下两种 ('Y', 'N')，其中 'Y' 表示该产品是低脂产品，'N' 表示不是低脂产品。
recyclable 是枚举类型，取值为以下两种 ('Y', 'N')，其中 'Y' 表示该产品可回收，而 'N' 表示不可回收。

写出 SQL 语句，查找既是低脂又是可回收的产品编号。

返回结果 无顺序要求 。

- Result SQL:

```
select 
product_id
from Products where low_fats  = 'Y' and recyclable = 'Y';
```
----------------

