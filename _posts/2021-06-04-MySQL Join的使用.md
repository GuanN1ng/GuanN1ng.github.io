---
layout: post
title:  "Lists,Stacks,Queues"
date:   2021-06-02 20:52:40
categories: Data Structures
---

环境准备：有表t1、t2，建表语句如下，表t1、t2结构相同。
```
CREATE TABLE `t1` (
	`id` INT ( 11 ) NOT NULL,
	`a` INT ( 11 ) DEFAULT NULL,
	`b` INT ( 11 ) DEFAULT NULL,
	PRIMARY KEY ( `id` ),
KEY `a` ( `a` ) 
) ENGINE = INNODB;
```



#### Index Nested-Loop Join

Index Nested-Loop Join是指可以使用被驱动表的索引的查询。SQL语句`select * from t1 straight_join  t2 on  t1.a = t2.a`的执行流程为：

* 从驱动表t1中读入一行数据R；
* 从数据行R中取出a字段到t2表查找；
* 取出表t2中满足条件的行，和R组成一行，作为结果集的一部分；
* 重复步骤1~3，直到表t1的末尾结束。

```
mysql> explain select * from t1 straight_join  t2 on  t1.a = t2.a;
+----+-------------+-------+------------+------+---------------+------+---------+---------------+------+----------+-------------+
| id | select_type | table | partitions | type | possible_keys | key  | key_len | ref           | rows | filtered | Extra       |
+----+-------------+-------+------------+------+---------------+------+---------+---------------+------+----------+-------------+
|  1 | SIMPLE      | t1    | NULL       | ALL  | a             | NULL | NULL    | NULL          |  100 |   100.00 | Using where |
|  1 | SIMPLE      | t2    | NULL       | ref  | a             | a    | 5       | demo.t1.a     |    1 |   100.00 | NULL        |
+----+-------------+-------+------------+------+---------------+------+---------+---------------+------+----------+-------------+
```

当可以使用**被驱动表的索引**时，假设驱动表的行数为N，被驱动表的行数为M，因为被驱动表可以走索引树搜索，查找被驱动表的时间复杂度为logM，使用Index Nested-Loop Join
的join查询时间复杂度为N*logM。


#### Block Nested Loop


当SQL语句为`select * from t1 straight_join  t2 on  t1.a = t2.b`，此时被驱动表上没有可用的索引，执行流程为：

* 把表t1的全部数据(因为`select *`)读入线程内存**join_buffer**中；
* 扫描表t2，把表t2的每一行取出来，和join_buffer中的数据对比，满足条件的作为结果集的一部分返回。


这种算法下，无论使用表t1、t2中的哪一个作为驱动表，因为都需要对两个表做一次全表扫描，则扫描行数为M+N，内存中判断次数为M * N。但**当表t1过大时，
join_buffer不一定能够放入t1的所有数据**，`join_buffer`的大小由**join_buffer_size**设定，对于无法全部放入join_buffer的表，Innodb会采用
**分段放**的策略，此时，执行过程变为：

* 扫描表t1，按顺序读取t1数据行放入join_buffer中，直至join_buffer容量耗尽；
* 扫描表t2，把表t2的每一行取出来，和join_buffer中的数据对比，满足条件的作为结果集的一部分返回；
* 清空join_buffer；
* 继续读取表t1中的剩余行，循环1~3步，直至读取完t1中的所有行。

```
mysql> explain select * from t1 straight_join  t2 on  t1.a = t2.b;
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+----------------------------------------------------+
| id | select_type | table | partitions | type | possible_keys | key  | key_len | ref  | rows | filtered | Extra                                              |
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+----------------------------------------------------+
|  1 | SIMPLE      | t1    | NULL       | ALL  | a             | NULL | NULL    | NULL |  100 |   100.00 | NULL                                               |
|  1 | SIMPLE      | t2    | NULL       | ALL  | NULL          | NULL | NULL    | NULL | 1000 |    10.00 | Using where; Using join buffer (Block Nested Loop) |
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+----------------------------------------------------+
```

#### 小表做驱动表

* 采用Index Nested-Loop Join算法时，驱动表越小，效率越高；

* 采用Block Nested Loop算法时：

    * join_buffer不够大时，**每一次分段读取t1后，都需要对t2进行一次全表扫描**，应选取**小表做驱动表**。

    * **join_buffer的size足够大时**，表t1,t2均需要一次全表扫描，此时选取**任意一个表做被驱动表的消耗是相同的**。
    
一个表是不是**小表**和具体的语句有直接关系，表的行数并不是判断是否为小表的唯一条件。对于语句`select * from t2 straight_join t1 on t1.b=t2.b where t2.id<=50;`
即使t2的行数远大于t1,因为字段b上无索引，此时表t2只需读取前50行，即join_buffer中只需放入t2的前50行，只要t1的行数大于50行，就应该选取t2作为驱动表。   


#### join优化

* Index Nested-Loop Join

    对于**二级索引的回表**问题，MySQL提供**Multi-Range Read**的优化，目的是**减少随机IO，尽量使用顺序度盘**。在MRR优化下，当对二级索引进行**范围查询**(多值查询)时，流程为：
    * 对满足条件的记录，先将主键id放入read_rnd_buffer中；
    * 对read_rnd_buffer中的id进行递增排序；
    * 使用排序后的id数组一次到聚簇索引中查找记录并返回。
    read_rnd_buffer的大小由read_rnd_buffer_size参数控制，如需稳定使用该优化，需要修改参数"mrr_cost_based=off"。
    
    Index Nested-Loop Join算法中，每次从驱动表中**只取出一行数据**，去被驱动表中查找，**造成大量的IO**。MySQL引入了**Batched Key Access**
    算法。BKA算法下，不再一行行的从驱动表中读取，而是使用join_buffer将驱动表的数据取出一部分然后去被驱动表批量查询。



* Block Nested Loop
    
    * 自行实现hash-join
    * 如果被驱动表上有where条件，可使用临时表减少被驱动表的行数，再进行join，减少判断次数。       