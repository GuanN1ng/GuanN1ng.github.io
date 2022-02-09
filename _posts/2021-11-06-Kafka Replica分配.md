---
layout: post 
title:  Kafka Replica分配
date:   2021-11-06 11:47:55 
categories: Kafka
---

Kafka中每个有效的Partition都有1个或多个Replica构成该分区的副本集合，为保障高可用，每个分区的Replicas应均匀的分布在集群的各Broker节点中，防止单Broker故障宕机导致的整个Partition不可用。
Replica分配方案即可以通过脚本工具手动指定，也可以由Kafka自动计算得出。本篇内容将讲解如何完成Replica的分配。

需要进行Replica分配的共有3种情况：

* 1、新建Topic；
* 2、对Topic进行分区扩容(对一个正常运行的Topic,Kafka只支持增加分区数，不允许减少分区数)；
* 3、修改指定Partition的Replica数或进行副本重分配；


# CreateTopic

用户通过`kafka-topics.sh`脚本命令创建Topic，可通过不同的参数控制是通过手动指定分配方案或由Kafka自动计算。

## 用户指定replicaAssignment

创建Topic时，使用`-replica-assignment`参数指定所有分区的副本分配方案，如下：

```
bin/kafka-topics.sh --create --bootstrap-server localhost:9092  --topic my-topic-name  --replica-assignment 0:1:2,0:1:2,0:1:2
  --config max.message.bytes=64000
```

`--replica-assignment 0:1:2,1:2:0,2:0:1`参数表示Topic共有三个Partition(P0、P1、P2)且每个分区都有3个Replica，且均匀的分布在BrokerId为0，1，2的Broker节点上。分配情况如下：

| broker-0 | broker-1 | broker-2 |
|----------|----------|----------|
| p0-1     | p0-2     | p0-3     |
| p1-3     | p1-1     | p1-2     |
| p2-2     | p2-3     | p2-1     |


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


# 分区副本重分配

Topic创建完成后，若需要对已存在的Partition的Replicas进行重分配(增加或减少副本或迁移副本)，可通过`kafka-reassign-partitions.sh`脚本工具实现。命令如下：

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
broker-0,broker-1,broker-2以及broker-3中。`kafka-reassign-partitions.sh`脚本的内容如下：

```
exec $(dirname $0)/kafka-run-class.sh kafka.admin.ReassignPartitionsCommand "$@"
```

通过调用kafka.admin.ReassignPartitionsCommand#main()方法执行，主要内容是将json文件中的分配方案封装为**`AlterPartitionReassignmentsRequest`**请求并发送给controller
节点进行处理。controller节点收到AlterPartitionReassignmentsRequest请求后，会将新的分配方案写入**`/admin/reassign_partitions`ZK节点**中，触发controller为该节点注册的
PartitionReassignmentHandler，进行后续副本重分配工作。

```
class PartitionReassignmentHandler(eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  override val path: String = ReassignPartitionsZNode.path
  //创建ZkPartitionReassignment事件
  override def handleCreation(): Unit = eventManager.put(ZkPartitionReassignment)
}
```

## ZkPartitionReassignment

PartitionReassignmentHandler监听到**`/admin/reassign_partitions`ZK节点**的变化后，会创建**ZkPartitionReassignment**事件放入事件队列中，等待controller进行处理，
处理方法如下：

```
private def processZkPartitionReassignment(): Set[TopicPartition] = {
  // We need to register the watcher if the path doesn't exist in order to detect future
  // reassignments and we get the `path exists` check for free
  if (isActive && zkClient.registerZNodeChangeHandlerAndCheckExistence(partitionReassignmentHandler)) {
    val reassignmentResults = mutable.Map.empty[TopicPartition, ApiError]
    val partitionsToReassign = mutable.Map.empty[TopicPartition, ReplicaAssignment]
    //从/admin/reassign_partitions节点读取分区分配方案
    zkClient.getPartitionReassignment.forKeyValue { (tp, targetReplicas) =>
      //封装重分配方案
      maybeBuildReassignment(tp, Some(targetReplicas)) match {
        case Some(context) => partitionsToReassign.put(tp, context)
        case None => reassignmentResults.put(tp, new ApiError(Errors.NO_REASSIGNMENT_IN_PROGRESS))
      }
    }
    //执行重分配任务
    reassignmentResults ++= maybeTriggerPartitionReassignment(partitionsToReassign)
    val (partitionsReassigned, partitionsFailed) = reassignmentResults.partition(_._2.error == Errors.NONE)
    if (partitionsFailed.nonEmpty) {
      warn(s"Failed reassignment through zk with the following errors: $partitionsFailed")
      maybeRemoveFromZkReassignment((tp, _) => partitionsFailed.contains(tp))
    }
    partitionsReassigned.keySet
  } else {
    Set.empty
  }
}
```

进行分区副本重分配前需确保当前节点仍为controller节点，以及`/admin/reassign_partitions`节点仍存在，否则放弃执行重分配任务。maybeBuildReassignment()方法实现如下：

```
private def maybeBuildReassignment(topicPartition: TopicPartition,
                                   targetReplicasOpt: Option[Seq[Int]]): Option[ReplicaAssignment] = {
  //缓存中的分区分配方案
  val replicaAssignment = controllerContext.partitionFullReplicaAssignment(topicPartition)
  if (replicaAssignment.isBeingReassigned) {
    //分区有待进行的重分配，覆盖旧方案进行覆()
    val targetReplicas = targetReplicasOpt.getOrElse(replicaAssignment.originReplicas)
    Some(replicaAssignment.reassignTo(targetReplicas))
  } else {
    targetReplicasOpt.map { targetReplicas =>
      replicaAssignment.reassignTo(targetReplicas)
    }
  }
}
```

**当目标分区存在待执行的重分配方案时，直接丢弃旧方案，使用新的分配方案进行覆盖**。

## TriggerPartitionReassignment

分区副本重分配任务由方法KafkaController#maybeTriggerPartitionReassignment()方法触发执行，源码如下：

```
private def maybeTriggerPartitionReassignment(reassignments: Map[TopicPartition, ReplicaAssignment]): Map[TopicPartition, ApiError] = {
  reassignments.map { case (tp, reassignment) =>
    val topic = tp.topic
    val apiError = if (topicDeletionManager.isTopicQueuedUpForDeletion(topic)) {
      //对应的主题将被删除，不执行重新分配
      info(s"Skipping reassignment of $tp since the topic is currently being deleted")
      new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, "The partition does not exist.")
    } else {
      val assignedReplicas = controllerContext.partitionReplicaAssignment(tp)
      if (assignedReplicas.nonEmpty) {
        try {
          //执行副本重分配
          onPartitionReassignment(tp, reassignment)
          ApiError.NONE
        } catch {
          ...// error
        }
      } else {
        //本地缓存中不存在对应的分区
        new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, "The partition does not exist.")
      }
    }
    tp -> apiError
  }
}
```

maybeTriggerPartitionReassignment()方法可分为两部分内容：

* 1、重分配副本对应的主题为待删除状态或ControllerContext缓存中不存在对应的分区，则不进行副本重分配；
* 2、调用onPartitionReassignment()方法执行副本重分配。

## 执行副本分配任务

副本重分配由方法onPartitionReassignment()实现，执行过程中涉及到以下几种副本集合：

* RS = current assigned replica set，方法执行过程中，分区当前的副本列表；
* ORS = Original replica set for partition，该分区的原始副本列表；
* TRS = Reassigned (target) replica set，新的(目标)副本列表；
* AR = The replicas we are adding as part of this reassignment，从ORS->TRS，需要添加的副本列表，即AR = TRS - ORS；
* RR = The replicas we are removing as part of this reassignment，从ORS->TRS，需要移除的副本列表，即RR = ORS - TRS。

onPartitionReassignment()源码如下：

```
/**
 * This callback is invoked:
 * 1. By the AlterPartitionReassignments API
 * 2. By the reassigned partitions listener which is triggered when the /admin/reassign/partitions znode is created
 * 3. When an ongoing reassignment finishes - this is detected by a change in the partition's ISR znode
 * 4. Whenever a new broker comes up which is part of an ongoing reassignment
 * 5. On controller startup/failover
  * @param topicPartition 目标主题分区
 * @param reassignment 副本分配方案
 */
private def onPartitionReassignment(topicPartition: TopicPartition, reassignment: ReplicaAssignment): Unit = {
  // While a reassignment is in progress, deletion is not allowed
  //进行分区副本分配，标记主题不可删除
  topicDeletionManager.markTopicIneligibleForDeletion(Set(topicPartition.topic), reason = "topic reassignment in progress")
  //更新ZK和controllerContext中的内容，并标记分区为正在进行副本分配
  updateCurrentReassignment(topicPartition, reassignment)
  //需新建的副本
  val addingReplicas = reassignment.addingReplicas
  //待移除的副本
  val removingReplicas = reassignment.removingReplicas

  if (!isReassignmentComplete(topicPartition, reassignment)) {
    //重分配未完成 ，新的副本方案不在ISR中 targetReplicas.subsetOf(isr)
    // A1. Send LeaderAndIsr request to every replica in ORS + TRS (with the new RS, AR and RR).
    //向Replica节点发送LeaderAndIsrRequest
    updateLeaderEpochAndSendRequest(topicPartition, reassignment)
    // A2. replicas in AR -> NewReplica
    //副本状态机，将新建副本状态更新为NewReplica
    startNewReplicasForReassignedPartition(topicPartition, addingReplicas)
  } else {
    //新的副本已在ISR集合中
    // B1. replicas in AR -> OnlineReplica
    replicaStateMachine.handleStateChanges(addingReplicas.map(PartitionAndReplica(topicPartition, _)), OnlineReplica)
    // B2. Set RS = TRS, AR = [], RR = [] in memory.
    val completedReassignment = ReplicaAssignment(reassignment.targetReplicas)
    controllerContext.updatePartitionFullReplicaAssignment(topicPartition, completedReassignment)
    // B3. Send LeaderAndIsr request with a potential new leader (if current leader not in TRS) and
    //   a new RS (using TRS) and same isr to every broker in ORS + TRS or TRS
    moveReassignedPartitionLeaderIfRequired(topicPartition, completedReassignment)
    // B4. replicas in RR -> Offline (force those replicas out of isr)
    // B5. replicas in RR -> NonExistentReplica (force those replicas to be deleted)
    stopRemovedReplicasOfReassignedPartition(topicPartition, removingReplicas)
    // B6. Update ZK with RS = TRS, AR = [], RR = [].
    updateReplicaAssignmentForPartition(topicPartition, completedReassignment)
    // B7. Remove the ISR reassign listener and maybe update the /admin/reassign_partitions path in ZK to remove this partition from it.
    removePartitionFromReassigningPartitions(topicPartition, completedReassignment)
    // B8. After electing a leader in B3, the replicas and isr information changes, so resend the update metadata request to every broker
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicPartition))
    // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed
    topicDeletionManager.resumeDeletionForTopics(Set(topicPartition.topic))
  }
}
```


### 阶段一：副本方案元数据更新

负责完成副本方案元数据更新的方法为updateCurrentReassignment()，若ORS={1,2,3} and TRS={3，4,5,6}，此阶段需将新的副本集合RS=ORS+TRS={1,2,3，4,5,6}更新到内存和ZK中，
源码如下：

```
 private def updateCurrentReassignment(topicPartition: TopicPartition, reassignment: ReplicaAssignment): Unit = {
    //获取内存中分区的当前副本方案
    val currentAssignment = controllerContext.partitionFullReplicaAssignment(topicPartition)
    //新的副本方案与当前方案不一致，即需进行重分配
    if (currentAssignment != reassignment) {
      // U1. Update assignment state in zookeeper
      updateReplicaAssignmentForPartition(topicPartition, reassignment)
      // U2. Update assignment state in memory
      controllerContext.updatePartitionFullReplicaAssignment(topicPartition, reassignment)

      // If there is a reassignment already in progress, then some of the currently adding replicas
      // may be eligible for immediate removal, in which case we need to stop the replicas.
      val unneededReplicas = currentAssignment.replicas.diff(reassignment.replicas)
      if (unneededReplicas.nonEmpty)
        stopRemovedReplicasOfReassignedPartition(topicPartition, unneededReplicas)
    }
    ...// 低版本(小于2.7)的代码
    controllerContext.partitionsBeingReassigned.add(topicPartition)
  }
```

内容可分为两部分：
* 1、若新的副本方案与当前主题分区的副本方案不一致，进行数据更新：
  * 1.1、更新ZK节点数据，将Topic新的分区副本方案写入**`/brokers/topics/${topic}`**节点中；
  * 1.2、更新内存(ControllerContext)中的分区副本信息；
  * 1.3、该分区有正在进行的副本重分配任务(副本重分配过程非同步)，当前方案中可能有需要立即删除的副本，停止对应的Replica，nowState->OfflineReplica->ReplicaDeletionStarted->ReplicaDeletionSuccessful->NonExistentReplica。
* 2、将正在进行重分配的TopicPartition添加到partitionsBeingReassigned中，partitionsBeingReassigned中记录当前正在进行重分配的所有TopicPartition。

### 阶段二：创建副本



### 









