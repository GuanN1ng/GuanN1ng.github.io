---
layout: post 
title:  Kafka Replica的生命周期
date:   2021-11-10 22:37:58
categories: Kafka
---

副本(Replica)是分布式系统对数据和服务冗余的一种方式，**数据副本是指在不同节点上持久化同一份数据**，当某一个节点上存储的数据丢失时，仍可从其他副本上读取该数据，这是**解决分布式系统数据丢失问题**
最有效的手段；而**服务副本是指多个节点提供同样的服务**，每个节点都有能力接收来自外部的请求并进行相应的处理。

**Kafka为分区引入多副本机制，提升数据容灾能力，同时通过多副本机制实现故障自动转移(分区副本Leader选举)，在Kafka集群中某个Broker节点失效的情况下，保证服务可用**。

# 创建副本

Kafka中涉及创建副本的情况共有两种：创建Topic和分区副本重分配(增加分区副本数量或副本迁移)。



```

```

## 副本重分配

kafka-reassign-partitions.sh





### AlterPartitionReassignmentsRequest



