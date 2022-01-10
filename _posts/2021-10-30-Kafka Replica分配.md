---
layout: post 
title:  Kafka Replica分配
date:   2021-10-30 11:47:55 
categories: Kafka
---

Kafka中每个有效的Partition都有1个或多个Replica构成该分区的副本集合，为保障高可用，每个分区的Replicas应均匀的分布在集群的各Broker节点中，防止单Broker故障宕机导致的整个Partition不可用。
Replica分配方案即可以通过脚本工具手动指定，也可以由Kafka自动计算得出。本篇内容将讲解如何完成Replica的分配。

需要进行Replica分配的共有3种情况：

* 1、新建Topic；
* 2、对Topic进行分区扩容(对一个正常运行的Topic,Kafka只支持增加分区数，不允许减少分区数)；
* 3、修改指定Partition的Replica数或进行副本迁移；


# CreateTopic

用户通过`kafka-topics.sh`脚本命令创建Topic，可通过不同的参数控制是通过手动指定分配方案或由Kafka自动计算。

## 用户指定replicaAssignment

创建Topic时，使用`-replica-assignment`参数指定所有分区的副本分配方案，如下：

```
bin/kafka-topics.sh --create --bootstrap-server localhost:9092  --topic my-topic-name  --replica-assignment 0:1:2,0:1:2,0:1:2
  --config max.message.bytes=64000
```

`--replica-assignment 0:1:2,0:1:2,0:1:2`参数表示Topic共有三个Partition(P0、P1、P2)且每个分区都有3个Replica，且均匀的分布在BrokerId为0，1，2的Broker节点上。分配情况如下：

| broker-0 | broker-1 | broker-2 |
|----------|----------|----------|
| p0       | p0       | p0       |
| p1       | p1       | p1       |
| p2       | p2       | p2       |


## Kafka计算replicaAssignment

创建Topic时，只指定topic分区数及副本因子，由Kafka进行自动计算分配：

```
bin/kafka-topics.sh --create --bootstrap-server localhost:9092  --topic my-topic-name  --replication-factor 3 --partitions 3 
  --config max.message.bytes=64000
```

[Kafka Topic生命周期](https://guann1ng.github.io/kafka/2021/10/21/Kafka-Topic%E7%94%9F%E5%91%BD%E5%91%A8%E6%9C%9F/) 中分析过主题创建的流程，若用户未指定主题的分区副本分配方案，则调用AdminUtils#assignReplicasToBrokers()方法完成分配方案的计算，分配算法的目标有三个：

* 主题所有的分区副本均匀的分布在各broker节点上；
* 每个分区的所有副本(leader和follower)应分配到不同的broker节点；
* 如果所有broker都有机架信息，应将每个分区的副本分配到不同的机架(consumer消费时可选择preferredReadReplica进行消费);

源码如下：

```
def assignReplicasToBrokers(...): Map[Int, Seq[Int]] = {
  ...//param check
  if (brokerMetadatas.forall(_.rack.isEmpty))
    //所有broker节点均无机架信息
    assignReplicasToBrokersRackUnaware(nPartitions, replicationFactor, brokerMetadatas.map(_.id), fixedStartIndex,startPartitionId)
  else {
    if (brokerMetadatas.exists(_.rack.isEmpty))
      throw new AdminOperationException("Not all brokers have rack information for replica rack aware assignment.")
    //所有broker节点均有机架信息  
    assignReplicasToBrokersRackAware(nPartitions, replicationFactor, brokerMetadatas, fixedStartIndex,startPartitionId)
  }
}
```

分区副本分配方案的计算分为两类：无机架信息(rack.isEmpty)和有机架信息。

### 无机架信息

当所有broker节点均无机架信息时，分配方案由assignReplicasToBrokersRackUnaware()实现，源码如下：

```
private def assignReplicasToBrokersRackUnaware(...): Map[Int, Seq[Int]] = {
  //分配方案
  val ret = mutable.Map[Int, Seq[Int]]()
  //所有的broker节点
  val brokerArray = brokerList.toArray
  //fixedStartIndex = -1，startIndex为随机取一个broker
  val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
  //startPartitionId=-1 ,currentPartitionId=0
  var currentPartitionId = math.max(0, startPartitionId)
  //随机取一个分配步长
  var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
  //nPartitions分区数
  for (_ <- 0 until nPartitions) {
    if (currentPartitionId > 0 && (currentPartitionId % brokerArray.length == 0)) {
      //防止 nPartitions 过大时,其中某些partition的分配完全一样，currentPartitionId为brokerArray.length整数倍时+1
      nextReplicaShift += 1
    }
    //第一个分区副本的节点
    val firstReplicaIndex = (currentPartitionId + startIndex) % brokerArray.length
    val replicaBuffer = mutable.ArrayBuffer(brokerArray(firstReplicaIndex))
    //该分区剩余副本分配
    for (j <- 0 until replicationFactor - 1)
      replicaBuffer += brokerArray(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerArray.length))
    ret.put(currentPartitionId, replicaBuffer)
    currentPartitionId += 1
  }
  ret
}

//分区副本节点计算
private def replicaIndex(firstReplicaIndex: Int, secondReplicaShift: Int, replicaIndex: Int, nBrokers: Int): Int = {
  val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
  (firstReplicaIndex + shift) % nBrokers
}
```



分配算法主要分为两步：

* 从所有在线broker列表中随机选取一个起始位置，使用round-robin算法循环分配每个Partition的第一个Replica；
* 以该Partition的第一个Replica所在broker为起点，再通过nextReplicaShift步长确定下一个副本的节点，最后递增Broker列表下标(超出后从0再次开始)依次分配该Partition的剩余Replica。

例：假设一个Kafka集群有5个在线broker节点，其中一个Topic的Partition数为10，每个Partition有3个Replica，且最初随机选择的startIndex和nextReplicaShift节点均为0，计算过程如下：

* P0的第一个副本在`(0+0)%5=0`，第二个副本在`(0+(1+(0+0)%4)))%5=1`，第三副本在`(0+(1+(0+1)%4)))%5=2`，完成后currentPartitionId+1=1，nextReplicaShift=0；
* P1的第一个副本在`(1+0)%5=1`，第二个副本在`(1+(1+(0+0)%4)))%5=2`，第三副本在`(1+(1+(0+1)%4)))%5=3`，完成后currentPartitionId+1=2，nextReplicaShift=0；
* ...
* P4的第一个副本在`(4+0)%5=4`，第二个副本在`(4+(1+(0+0)%4)))%5=0`，第三副本在`(4+(1+(0+1)%4)))%5=1`，完成后currentPartitionId+1=5，nextReplicaShift=0；
* 此时currentPartitionId=5满足条件，nextReplicaShift+1=1，P5的第一个副本在`(5+0)%5=0`，第二个副本在`(0+(1+(1+0)%4)))%5=2`，第三副本在`(0+(1+(1+1)%4)))%5=3`，完成后currentPartitionId+1=6，nextReplicaShift=1；

最终分配结果如下：


| broker-0 | broker-1  | broker-2 | broker-3  | broker-4  |
|----------|-------|----------|----------|----------|
|p0      |p1      |p2      |p3      |p4     | 
|p5      |p6      |p7      |p8      |p9      |
|p4      |p0      |p1      |p2      |p3      |
|p8      |p9      |p5      |p6      |p7      |
|p3      |p4      |p0      |p1      |p2      |
|p7      |p8      |p9      |p5      |p6      |



### 有机架信息

当所有节点均有机架信息(rack)时，分配方案由assignReplicasToBrokersRackAware()方法实现，该方法**保证副本尽量在各机架及各节点间分配均匀**，如果分区副本数等于或大于机架数，则将确保每个机架至少获得一个副本，否则，每个机架最多只能获得一个副本。
实现如下：

```
  private def assignReplicasToBrokersRackAware(...): Map[Int, Seq[Int]] = {
    /**
     * 机架节点map，例
     * rack1: 0, 1, 2
     * rack2: 3, 4, 5
     * rack3: 6, 7, 8
     */
    val brokerRackMap = brokerMetadatas.collect { case BrokerMetadata(id, Some(rack)) =>
      id -> rack
    }.toMap
    val numRacks = brokerRackMap.values.toSet.size
    /**
     * 机架交替的broker列表,例
     * 0, 3, 6, 1, 4, 7, 2, 5, 8
     */
    val arrangedBrokerList = getRackAlternatedBrokerList(brokerRackMap)
    val numBrokers = arrangedBrokerList.size
    //结果集
    val ret = mutable.Map[Int, Seq[Int]]()
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(arrangedBrokerList.size)
    var currentPartitionId = math.max(0, startPartitionId)
    var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(arrangedBrokerList.size)
    for (_ <- 0 until nPartitions) {
      if (currentPartitionId > 0 && (currentPartitionId % arrangedBrokerList.size == 0))
        nextReplicaShift += 1
      //循环分配每个分区的第一个副本  
      val firstReplicaIndex = (currentPartitionId + startIndex) % arrangedBrokerList.size
      val leader = arrangedBrokerList(firstReplicaIndex)
      val replicaBuffer = mutable.ArrayBuffer(leader)
      val racksWithReplicas = mutable.Set(brokerRackMap(leader))
      val brokersWithReplicas = mutable.Set(leader)
      var k = 0
      for (_ <- 0 until replicationFactor - 1) {
        var done = false
        //while循环，直至找到合适的broker进行分配
        while (!done) {
          //获取下一个计算节点
          val broker = arrangedBrokerList(replicaIndex(firstReplicaIndex, nextReplicaShift * numRacks, k, arrangedBrokerList.size))
          //节点机架信息
          val rack = brokerRackMap(broker)
          // Skip this broker if
          // 1. there is already a broker in the same rack that has assigned a replica AND there is one or more racks
          //    that do not have any replica, or
          // 2. the broker has already assigned a replica AND there is one or more brokers that do not have replica assigned
          if ((!racksWithReplicas.contains(rack) || racksWithReplicas.size == numRacks)
              && (!brokersWithReplicas.contains(broker) || brokersWithReplicas.size == numBrokers)) {
            replicaBuffer += broker
            racksWithReplicas += rack
            brokersWithReplicas += broker
            done = true
          }
          k += 1
        }
      }
      ret.put(currentPartitionId, replicaBuffer)
      currentPartitionId += 1
    }
    ret
  }
```

算法实现与无机架信息的基本一致，只是需要将broker列表进一步处理，主要分为3步：

* 1、构架机架信息交替变化的broker节点列表，见源码注释；
* 2、从机架信息交替的broker列表中随机选取一个起始位置，循环分配每个Partition的第一个Replica；
* 3、计算分配每一个分区的其余副本，选取的Broker节点需要同时满足两个条件：
  * `!racksWithReplicas.contains(rack) || racksWithReplicas.size == numRacks`，Broker节点所在机架上不存在此分区的副本或所有机架上均已有此分区的副本；
  * `!brokersWithReplicas.contains(broker) || brokersWithReplicas.size == numBrokers`，Broker节点上不存在此分区的副本或所有Broker上均已有此分区的副本；

# Topic分区扩容

对Topic进行分区扩容同样有两种方式：指定扩容后分区副本分配方案和只指定扩容后分区数，副本分配由Kafka计算两种。

## 用户指定replicaAssignment

指定扩容后的分区数量和具体的副本分配方案，命令如下：

```
bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter  --topic my-topic-name  
   --replica-assignment 0:1:2,0:1:2,0:1:2,2:1:0  --partitions 4
```

`--replica-assignment 0:1:2,0:1:2,0:1:2,2:1:0`参数表示Topic共有4个Partition(P0、P1、P2、P3)且每个分区都有3个Replica，且均匀的分布在BrokerId为0，1，2的Broker节点上。分配情况如下：

| broker-0 | broker-1 | broker-2 |
|----------|----------|----------|
| p0       | p0       | p0       |
| p1       | p1       | p1       |
| p2       | p2       | p2       |
| p3       | p3       | p3       |


## Kafka计算replicaAssignment

仅指定扩容后的Partition数量，命令如下:

```
bin/kafka-topics.sh --bootstrap-server broker_host:port --alter --topic my_topic_name 
    --partitions 4
```

此时，Kafka会调用AdminZkClient#createNewPartitionsAssignment()完成**新增分区**(**不会改动之前的分区副本分配**)的副本分配方案计算，源码如下：

```
def createNewPartitionsAssignment(...): Map[Int, ReplicaAssignment] = {
  //获取P0分区的所有副本
  val existingAssignmentPartition0 = existingAssignment.getOrElse(0,throw new AdminOperationException(...)).replicas

  val partitionsToAdd = numPartitions - existingAssignment.size
  //只能增加分区数
  if (partitionsToAdd <= 0)
    throw new InvalidPartitionsException(...)
  
  //用户未指定时，不会执行
  replicaAssignment.foreach { proposedReplicaAssignment =>
    //验证用户指定的副本分配方案
    validateReplicaAssignment(proposedReplicaAssignment, existingAssignmentPartition0.size, allBrokers.map(_.id).toSet)
  }
  
  val proposedAssignmentForNewPartitions = replicaAssignment.getOrElse {
    //计算副本分配方案
    //startIndex为P0分区第一个副本所在Broker的id
    val startIndex = math.max(0, allBrokers.indexWhere(_.id >= existingAssignmentPartition0.head))
    AdminUtils.assignReplicasToBrokers(allBrokers, partitionsToAdd, existingAssignmentPartition0.size,
      startIndex, existingAssignment.size)
  }

  proposedAssignmentForNewPartitions.map { case (tp, replicas) =>
    tp -> ReplicaAssignment(replicas, List(), List())
  }
}
```

可以看到，和创建Topic时一样，副本分配方案的计算同样是调用AdminUtils.assignReplicasToBrokers()完成，但方法入参不一致：

* 新增Partition的**replicationFactor等于P0分区的副本数**；
* **fixedStartIndex**不再为Broker列表下标的随机值，而是**P0分区第一个副本所在节点**；
* startPartitionId为当前已有分区集合的大小；

后两个参数的变动主要是提升新增副本的分配随机性、均匀性。


# 修改副本数量或副本迁移

Topic创建完成后，若需要对已存在的Partition的Replicas进行数量修改(增加或减少)或重分配(迁移)，可通过`kafka-reassign-partitions.sh`脚本工具实现。命令如下：

```
bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file custom-reassignment.json --execute
```

命令中的custom-reassignment.json文件格式如下：

```
{
    "version":1,
    "partitions":[
        {
            "topic":"my_topic_name",
            "partition":0,
            "replicas":[0,1,2,3]
        },
        {
            "topic":"my_topic_name",
            "partition":1,
            "replicas":[1,2,3,0]
        },
        {
            "topic":"my_topic_name",
            "partition":2,
            "replicas":[2,3,0,1]
        }
    ]
}
```

replicas数组的长度表示修改后的副本数量，数组内的每个元素表示副本所在的Broker节点id。如上示例中，表示主题my_topic_name的P0分区共有4个副本，分别分配在
broker-0,broker-1,broker-2以及broker-3中。该命令的执行流程及相关源码分析放在后续的副本管理分析。
