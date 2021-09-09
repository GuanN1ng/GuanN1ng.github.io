---
layout: post
title:  "Lists,Stacks,Queues"
date:   2021-06-09 13:57:09
categories: Data Structures
---

```
explain select * from t where a > '1000000'  order by b limit 10;
+----+-------------+-------+------------+-------+---------------+------+---------+------+------+----------+---------------------------------------+
| id | select_type | table | partitions | type  | possible_keys | key  | key_len | ref  | rows | filtered | Extra                                 |
+----+-------------+-------+------------+-------+---------------+------+---------+------+------+----------+---------------------------------------+
|  1 | SIMPLE      | t     | NULL       | range | a             | a    | 5       | NULL |    1 |   100.00 | Using index condition; Using filesort |
+----+-------------+-------+------------+-------+---------------+------+---------+------+------+----------+---------------------------------------+
```

Using filesort表示没有使用索引的排序，即需要MySQL自行排序。MySQL会为每个线程分配一块内存用于排序，称为sort_buffer。filesort使用的排序方法有两种：

* 全字段排序，对需要排序的记录生成<sort_key,additional_fields>的元数据，该元数据包含排序字段和需要返回的所有列。排序完后不需要回表，但是元数据要比第一种方法长得多，需要更多的空间用于排序。
                                                                           
* rowid排序，对需要排序的记录生成<sort_key,rowid>的元数据进行排序，该元数据仅包含排序字段和rowid。排序完成后只有按字段排序的rowid，因此还需要通过rowid进行回表操作获取所需要的列的值，可能会导致大量的随机IO读消耗；

采用何种排序算法由参数**max_length_for_sort_data**确定，当需要排序记录的字段长度超过max_length_for_sort_data时，采用rowid排序，反之采用全字段排序。
```
mysql> show variables like 'max_length_for_sort_data';
+--------------------------+-------+
| Variable_name            | Value |
+--------------------------+-------+
| max_length_for_sort_data | 1024  |
+--------------------------+-------+
```


全字段排序

全字段排序步骤：

* 过滤条件有索引，从二级索引依次获取所有符合条件的行记录，若无，则全表扫描获取，将需返回的字段及排序字段并依次放入sort_buffer中。

* MySQL对sort_buffer内的数据进行排序。

* 按照排序结果返回相应的结果集，例前10条。

```
mysql> show variables like 'sort_buffer_size';
+------------------+--------+
| Variable_name    | Value  |
+------------------+--------+
| sort_buffer_size | 262144 |
+------------------+--------+
```



rowid排序

rowid排序算法下，需要放入sort_buffer中的字段只有要排序的列(order by的条件)和主键id。

rowid排序步骤：

* 过滤条件有索引，从二级索引依次获取所有符合条件的行记录，若无，则全表扫描获取，将要排序的列和主键id放入sort_buffer中。

* 对sort_buffer中的数据进行排序。

* 遍历排序结果，按照id回原表取出需返回的字。


两种算法的sort_buffer排序，根据序排序的数据集不同，采用的算法也不同。超过**sort_buffer_size**的大小，则排序无法在内存中完成，需要利用磁盘临时文件服务排序。外部排序使用
归并排序算法，但也不是绝对的，若排序的行数与需返回的行数差别过大，如符合的行数有10000但limit 3,MySQL会采用优先队列排序法。

的OPTIMIZER_TRACE filesort_priority_queue_optimization的 chosen=true


优化：

 上述两种方法表明，MySQL在内存足够的情况下，会多利用内存，减少磁盘访问，但并非所有的order by都需要排序，通过修改索引我们可以完成对order by语句的
 优化。如对排序字段与过滤字段建立**联合索引**，避免排序，更进一步，我们也可以使用**覆盖索引**。

```
mysql> explain select a,b from t where a = '1000'  order by b limit 10;
+----+-------------+-------+------------+------+---------------+------+---------+-------+------+----------+--------------------------+
| id | select_type | table | partitions | type | possible_keys | key  | key_len | ref   | rows | filtered | Extra                    |
+----+-------------+-------+------------+------+---------------+------+---------+-------+------+----------+--------------------------+
|  1 | SIMPLE      | t     | NULL       | ref  | a_b           | a_b  | 5       | const |    1 |   100.00 | Using where; Using index |
+----+-------------+-------+------------+------+---------------+------+---------+-------+------+----------+--------------------------+
```
*Using index表示使用了覆盖索引