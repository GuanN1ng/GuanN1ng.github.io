---
layout: post
title:  "MySQL GROUP BY"
date:   2021-06-08 16:55:36
categories: 
- MySQL
---

环境准备：

```
CREATE TABLE `t2` (
  `id` int(11) NOT NULL,
  `a` int(11) DEFAULT NULL,
  `b` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


begin 
declare i int; set i=1; while(i<=1000)do insert into t2 values(i, i%10, i); set i=i+1; end while; 
end
```

#### 执行流程

```
mysql> explain select  a ,count(*) c from t2 group by a;
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+---------------------------------+
| id | select_type | table | partitions | type | possible_keys | key  | key_len | ref  | rows | filtered | Extra                           |
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+---------------------------------+
|  1 | SIMPLE      | t2    | NULL       | ALL  | NULL          | NULL | NULL    | NULL | 1000 |   100.00 | Using temporary; Using filesort |
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+---------------------------------+
```

如上SQL explain的结果，在Extra字段中，可以看到：
* Using temporary，表示使用了临时表；
* Using filesort，表示需要排序。

SQL的执行流程为：

* **创建内存临时表，表内有两个字段a,c，主键是a**；
* 扫描表t2，依次取出行记录中的**字段a的值x**；
    * 如果临时表内没有主键为x的行，则插入一行记录(x,1)；
    * 如果临时表内有主键为x的行，则更新c的值为c+1；
* 遍历完成后，对**字段a做排序**，得到结果期。

    
#### 优化方法

##### 排序

当对结果集不需要排序时，可使用**order by null**，去除排序的步骤，如下，已经没有Using filesort关键字了。

```
mysql> explain select  a ,count(*) c from t2 group by a order by null;
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+-----------------+
| id | select_type | table | partitions | type | possible_keys | key  | key_len | ref  | rows | filtered | Extra           |
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+-----------------+
|  1 | SIMPLE      | t2    | NULL       | ALL  | NULL          | NULL | NULL    | NULL | 1000 |   100.00 | Using temporary |
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+-----------------+
```



##### 索引

group by的语义是统计不同值出现的次数，**因为group by的字段是无序的**，在创建临时表时，都需要构造一个带**唯一索引的表**，如果可以保证扫描过程
中出现的数据是有序的，那么计算group by时，只需依次顺序扫描累加，遇到不同的值，则将其哪一个值及其累加值放入结果集中，直至结束，这样既不再需要临时表，
也不再需要排序。

```
mysql> ALTER TABLE `demo`.`t2` ADD INDEX `a`(`a`) USING BTREE;
Query OK, 0 rows affected (0.03 sec)
Records: 0  Duplicates: 0  Warnings: 0

mysql> explain select  a ,count(*) c from t2 group by a;
+----+-------------+-------+------------+-------+---------------+------+---------+------+------+----------+-------------+
| id | select_type | table | partitions | type  | possible_keys | key  | key_len | ref  | rows | filtered | Extra       |
+----+-------------+-------+------------+-------+---------------+------+---------+------+------+----------+-------------+
|  1 | SIMPLE      | t2    | NULL       | index | a             | a    | 5       | NULL | 1000 |   100.00 | Using index |
+----+-------------+-------+------------+-------+---------------+------+---------+------+------+----------+-------------+
1 row in set, 1 warning (0.00 sec)
```    


给a字段加上索引后，只需扫描a上的索引即可完成统计，Using index表示使用了覆盖索引，无需回表。

MySQL5.7后的版本支持了**generated column机制**，可以实现列数据的关联更新，如果需要对列的原始数据进行运算后再group by，可以使用该方法：
```
alter table t2 add column m int generated always as(b%10**原始字段及操作**),add index(m);
```

##### 直接排序

对于不适合创建索引的场景，如果需要统计的数据量不大，尽量只使用内存表，可通过适当调大**tmp_table_size**，避免使用到磁盘临时表；如果数据量确实很大，
可以使用**SQL_BIG_RESULT**这个hint，提示优化器直接使用排序算法，此时MySQL会**使用sort_buffer来完成排序**，当sort_buffer内存不够用时，
利用磁盘文件辅助排序。

```
mysql> mysql> explain select b,count(*) c from t2 group by b;
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+---------------------------------+
| id | select_type | table | partitions | type | possible_keys | key  | key_len | ref  | rows | filtered | Extra                           |
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+---------------------------------+
|  1 | SIMPLE      | t2    | NULL       | ALL  | NULL          | NULL | NULL    | NULL | 1000 |   100.00 | Using temporary; Using filesort |
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+---------------------------------+


mysql> explain select SQL_BIG_RESULT b,count(*) c from t2 group by b;
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+----------------+
| id | select_type | table | partitions | type | possible_keys | key  | key_len | ref  | rows | filtered | Extra          |
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+----------------+
|  1 | SIMPLE      | t2    | NULL       | ALL  | NULL          | NULL | NULL    | NULL | 1000 |   100.00 | Using filesort |
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+----------------+
1 row in set, 1 warning (0.00 sec)
```

SQL_BIG_RESULT提示的SQL执行流程为：
* 初始化sort_buffer，放入一个字段b；
* 扫描表t2，依次取出b的值，放入sort_buffer；
* 扫描完成后，对sort_buffer的字段b排序，得到有序数组；
* 次数统计同利用索引。