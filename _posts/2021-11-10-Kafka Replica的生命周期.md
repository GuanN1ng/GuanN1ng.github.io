---
layout: post 
title:  Kafka Replica的生命周期
date:   2021-11-10 22:37:58
categories: Kafka
---

副本(Replica)是分布式系统对数据和服务冗余的一种方式，**数据副本是指在不同节点上持久化同一份数据**，当某一个节点上存储的数据丢失时，仍可从其他副本上读取该数据，这是**解决分布式系统数据丢失问题**
最有效的手段；而**服务副本是指多个节点提供同样的服务**，每个节点都有能力接收来自外部的请求并进行相应的处理。

**Kafka为分区引入多副本机制，提升数据容灾能力，同时通过多副本机制实现故障自动转移(分区副本Leader选举)，在Kafka集群中某个Broker节点失效的情况下，保证服务可用**。

# 副本的创建或角色转换-LeaderAndIsrRequest

Kafka中涉及创建副本的情况共有两种：创建Topic和分区副本重分配(增加分区副本数量或副本迁移)，两种情况下，Controller节点均是通过LeaderAndIsrRequest请求通知目标broker完成本地副本的创建及角色转换，下面通过broker
对LeaderAndIsrRequest的处理来分析如何完成副本的创建或角色转换。

## LeaderAndIsrRequest

当TopicPartition的元数据，如Leader、ISR、ReplicaAssignment等发生变化时，Controller节点会向目标broker发送LeaderAndIsrRequest请求，TopicPartition的元数据会以
LeaderAndIsrPartitionState对象封装在请求体中，该对象的数据结构如下：

```
public static class LeaderAndIsrPartitionState implements Message {
    String topicName;  
    int partitionIndex; //分区号
    int controllerEpoch; 
    int leader; //leader副本id
    int leaderEpoch;
    List<Integer> isr; 
    int zkVersion;
    List<Integer> replicas;
    List<Integer> addingReplicas; //新增Replica
    List<Integer> removingReplicas; //需移除Replica
    boolean isNew; //是否为新建Replica
    ...
}    
```

## handleLeaderAndIsrRequest

LeaderAndIsrRequest请求处理的入口方法为KafkaApis#handleLeaderAndIsrRequest()，对请求处理的部分由ReplicaManager#becomeLeaderOrFollower()方法完成。源码如下：

```
def becomeLeaderOrFollower(correlationId: Int,
                           leaderAndIsrRequest: LeaderAndIsrRequest,
                           onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): LeaderAndIsrResponse = {
  val startMs = time.milliseconds()
  //同步代码块
  replicaStateChangeLock synchronized {
    val controllerId = leaderAndIsrRequest.controllerId
    //分区副本信息
    val requestPartitionStates = leaderAndIsrRequest.partitionStates.asScala

    val topicIds = leaderAndIsrRequest.topicIds()
    def topicIdFromRequest(topicName: String): Option[Uuid] = {
      val topicId = topicIds.get(topicName)
      // if invalid topic ID return None
      if (topicId == null || topicId == Uuid.ZERO_UUID)
        None
      else
        Some(topicId)
    }
    val response = {
      if (leaderAndIsrRequest.controllerEpoch < controllerEpoch) {
        //请求中的controller epoch已过期 controller已重新选举
        leaderAndIsrRequest.getErrorResponse(0, Errors.STALE_CONTROLLER_EPOCH.exception)
      } else {
        val responseMap = new mutable.HashMap[TopicPartition, Errors]
        controllerEpoch = leaderAndIsrRequest.controllerEpoch
        //本实例需处理的分区
        val partitions = new mutable.HashSet[Partition]()
        //本地副本为 leader 的 Partition 列表
        val partitionsToBeLeader = new mutable.HashMap[Partition, LeaderAndIsrPartitionState]()
        //本地副本为 follower 的 Partition 列表
        val partitionsToBeFollower = new mutable.HashMap[Partition, LeaderAndIsrPartitionState]()
        //
        val topicIdUpdateFollowerPartitions = new mutable.HashSet[Partition]()

        //按照分区遍历
        requestPartitionStates.foreach { partitionState =>
          val topicPartition = new TopicPartition(partitionState.topicName, partitionState.partitionIndex)
          //若分区不存在，初始化分区
          val partitionOpt = getPartition(topicPartition) match {
            case HostedPartition.Offline =>
              //分区已离线
              responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)
              None
            case HostedPartition.Online(partition) =>
              Some(partition)
            case HostedPartition.None =>
              //本地不存在，创建分区
              val partition = Partition(topicPartition, time, this)
              allPartitions.putIfNotExists(topicPartition, HostedPartition.Online(partition))
              Some(partition)
          }

          // Next check the topic ID and the partition's leader epoch
          partitionOpt.foreach { partition =>
            val currentLeaderEpoch = partition.getLeaderEpoch
            val requestLeaderEpoch = partitionState.leaderEpoch
            val requestTopicId = topicIdFromRequest(topicPartition.topic)
            val logTopicId = partition.topicId

            if (!hasConsistentTopicId(requestTopicId, logTopicId)) {
              //TopicId异常
              responseMap.put(topicPartition, Errors.INCONSISTENT_TOPIC_ID)
            } else if (requestLeaderEpoch > currentLeaderEpoch) {
              //leader epoch有效
              if (partitionState.replicas.contains(localBrokerId)) {
                partitions += partition
                if (partitionState.leader == localBrokerId) {
                  //leader副本位于该broker节点
                  partitionsToBeLeader.put(partition, partitionState)
                } else {
                  partitionsToBeFollower.put(partition, partitionState)
                }
              } else {
                //分区副本列表中没有位于该broker上的副本，返回异常
                responseMap.put(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
              }
            } else if (requestLeaderEpoch < currentLeaderEpoch) {
              //leader epoc异常
              responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH)
            } else {
             ...// TopicId处理
            }
          }
        }

        val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
        val partitionsBecomeLeader = if (partitionsToBeLeader.nonEmpty) {
          //设置leader副本
          makeLeaders(controllerId, controllerEpoch, partitionsToBeLeader, correlationId, responseMap,
            highWatermarkCheckpoints, topicIdFromRequest)
        } else
          Set.empty[Partition]
        val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty) {
          //设置follower副本
          makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, correlationId, responseMap,
            highWatermarkCheckpoints, topicIdFromRequest)
        } else
          Set.empty[Partition]
        ...

        // We initialize highwatermark thread after the first LeaderAndIsr request. This ensures that all the partitions
        // have been completely populated before starting the checkpointing there by avoiding weird race conditions
        startHighWatermarkCheckPointThread()

        maybeAddLogDirFetchers(partitions, highWatermarkCheckpoints, topicIdFromRequest)
        //清理无效线程
        replicaFetcherManager.shutdownIdleFetcherThreads()
        replicaAlterLogDirsManager.shutdownIdleFetcherThreads()

        //处理__consumer_offset和__transaction_state 的 leaderAndIsr 信息
        onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)

        val data = new LeaderAndIsrResponseData().setErrorCode(Errors.NONE.code)
          ...// 响应数据处理
        new LeaderAndIsrResponse(data, leaderAndIsrRequest.version)
      }
    }
    response
  }
}
```

除去相关参数如controllerEpoch、leaderEpoch、topicId及TopicPartition等参数的验证及处理，becomeLeaderOrFollower()方法总共包含三部分内容：

* 查找本地副本为leader副本的Partition列表(partitionsToBeLeader)，调用makeLeaders()方法完成Leader副本设置；
* 查找本地副本为follower副本的Partition列表(partitionsToBeFollower)，调用makeFollowers()方法完成Leader副本设置；
* 调用RequestHandlerHelper#onLeadershipChange()方法处理Kafka内部主题(__consumer_offset和__transaction_state)的leaderAndIsr信息变化；

### makeLeaders

makeLeaders()方法源码如下：

```
private def makeLeaders(controllerId: Int,
                        controllerEpoch: Int,
                        partitionStates: Map[Partition, LeaderAndIsrPartitionState],
                        correlationId: Int,
                        responseMap: mutable.Map[TopicPartition, Errors],
                        highWatermarkCheckpoints: OffsetCheckpoints,
                        topicIds: String => Option[Uuid]): Set[Partition] = {
  partitionStates.keys.foreach { partition =>
    responseMap.put(partition.topicPartition, Errors.NONE)
  }
  val partitionsToMakeLeaders = mutable.Set[Partition]()

  try {
    // First stop fetchers for all the partitions
    //停止副本同步
    replicaFetcherManager.removeFetcherForPartitions(partitionStates.keySet.map(_.topicPartition))

    // Update the partition information to be the leader
    partitionStates.forKeyValue { (partition, partitionState) =>
      try {
        //将本地副本设置为leader
        if (partition.makeLeader(partitionState, highWatermarkCheckpoints, topicIds(partitionState.topicName)))
          partitionsToMakeLeaders += partition
      } catch {
        case e: KafkaStorageException =>
          // If there is an offline log directory, a Partition object may have been created and have been added
          // to `ReplicaManager.allPartitions` before `createLogIfNotExists()` failed to create local replica due
          // to KafkaStorageException. In this case `ReplicaManager.allPartitions` will map this topic-partition
          // to an empty Partition object. We need to map this topic-partition to OfflinePartition instead.
          markPartitionOffline(partition.topicPartition)
          responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
      }
    }
  } catch {
    case e: Throwable =>
      // Re-throw the exception for it to be caught in KafkaApis
      throw e
  }
  partitionsToMakeLeaders
}
```

方法内容分两点：

* 调用ReplicaFetcherManager#removeFetcherForPartitions()方法，移除partitionsToBeLeader集合中TopicPartition的副本同步线程；
* 遍历TopicPartition集合，调用Partition#makeLeader()方法将该TopicPartition的本地副本设置为leader。

#### makeLeader

Partition#makeLeader()方法实现如下：

```
def makeLeader(partitionState: LeaderAndIsrPartitionState,
               highWatermarkCheckpoints: OffsetCheckpoints,
               topicId: Option[Uuid]): Boolean = {
  //
  val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
    // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
    // to maintain the decision maker controller's epoch in the zookeeper path
    controllerEpoch = partitionState.controllerEpoch

    val isr = partitionState.isr.asScala.map(_.toInt).toSet
    val addingReplicas = partitionState.addingReplicas.asScala.map(_.toInt)
    val removingReplicas = partitionState.removingReplicas.asScala.map(_.toInt)
    //更新内存中的分区副本数据 若需要 移除待删除副本removingReplicas、添加新的远程副本(不在本节点上的Replica)对象
    updateAssignmentAndIsr( assignment = partitionState.replicas.asScala.map(_.toInt), isr = isr, addingReplicas = addingReplicas, removingReplicas = removingReplicas )
    try {
      //若需要，创建本地Replica日志
      createLogIfNotExists(partitionState.isNew, isFutureReplica = false, highWatermarkCheckpoints, topicId)
    } catch {
      case e: ZooKeeperClientException =>
        return false
    }
    //获取地Replica日志，及leader副本日志
    val leaderLog = localLogOrException
    val leaderEpochStartOffset = leaderLog.logEndOffset

    //We cache the leader epoch here, persisting it only if it's local (hence having a log dir)
    leaderEpoch = partitionState.leaderEpoch
    leaderEpochStartOffsetOpt = Some(leaderEpochStartOffset)
    zkVersion = partitionState.zkVersion

    // In the case of successive leader elections in a short time period, a follower may have
    // entries in its log from a later epoch than any entry in the new leader's log. In order
    // to ensure that these followers can truncate to the right offset, we must cache the new
    // leader epoch and the start offset since it should be larger than any epoch that a follower
    // would try to query.
    leaderLog.maybeAssignEpochStartOffset(leaderEpoch, leaderEpochStartOffset)
    //之前是否是分区leader
    val isNewLeader = !isLeader
    val curTimeMs = time.milliseconds
    // initialize lastCaughtUpTime of replicas as well as their lastFetchTimeMs and lastFetchLeaderLogEndOffset.
    remoteReplicas.foreach { replica =>
      val lastCaughtUpTimeMs = if (isrState.isr.contains(replica.brokerId)) curTimeMs else 0L
      //更新内存中follower副本的lastFetchTimeMs and lastFetchLeaderLogEndOffset，以便下次follower副本进行同步
      replica.resetLastCaughtUpTime(leaderEpochStartOffset, curTimeMs, lastCaughtUpTimeMs)
    }

    if (isNewLeader) {
      //本地副本之前不是leader,本次新当选
      // mark local replica as the leader after converting hw
      //内存标记
      leaderReplicaIdOpt = Some(localBrokerId)
      // reset log end offset for remote replicas
      //更新follower副本的同步信息
      remoteReplicas.foreach { replica =>
        replica.updateFetchState(
          followerFetchOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata,
          followerStartOffset = UnifiedLog.UnknownOffset,
          followerFetchTimeMs = 0L,
          leaderEndOffset = UnifiedLog.UnknownOffset)
      }
    }
    // we may need to increment high watermark since ISR could be down to 1
    (maybeIncrementLeaderHW(leaderLog), isNewLeader)
  }
  // some delayed operations may be unblocked after HW changed
  if (leaderHWIncremented)
    tryCompleteDelayedRequests()
  isNewLeader
}
```

本地Replica设置为leader可分以下几步：

* 1、调用updateAssignmentAndIsr()方法更新内存中的分区数据，包括新的副本方案、ISR集合、远程副本集合；
* 2、若本地之前没有分区的副本，调用createLogIfNotExists()创建本地副本日志；
* 3、缓存Leader副本信息(只有leader副本所在broker需要)，包括leaderEpoch、zkVersion、leaderEpochStartOffsetOpt；
* 4、更新follower副本(本地为leader,即所有远程副本)的lastFetchTimeMs and lastFetchLeaderLogEndOffset；
* 5、若本地Replica为新的Leader(上一次选举时不是)，更新follower副本的同步信息；
* 6、检查一下是否需要更新本地副本日志的HW值。


### makeFollowers

makeFollowers()方法会将给定的分区集合的本地副本设置为follower副本，源码如下：

```
private def makeFollowers(controllerId: Int,
                          controllerEpoch: Int,
                          partitionStates: Map[Partition, LeaderAndIsrPartitionState],
                          correlationId: Int,
                          responseMap: mutable.Map[TopicPartition, Errors],
                          highWatermarkCheckpoints: OffsetCheckpoints,
                          topicIds: String => Option[Uuid]) : Set[Partition] = {

  partitionStates.forKeyValue { (partition, partitionState) =>
    responseMap.put(partition.topicPartition, Errors.NONE)
  }

  val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()
  try {
    partitionStates.forKeyValue { (partition, partitionState) =>
      val newLeaderBrokerId = partitionState.leader
      try {
        if (metadataCache.hasAliveBroker(newLeaderBrokerId)) {
          //leader副本节点存活 Only change partition state when the leader is available
          //将 Partition 的本地副本设置为 follower
          if (partition.makeFollower(partitionState, highWatermarkCheckpoints, topicIds(partitionState.topicName)))
            partitionsToMakeFollower += partition
        } else {
          // Create the local replica even if the leader is unavailable. This is required to ensure that we include
          // the partition's high watermark in the checkpoint file (see KAFKA-1647)
          partition.createLogIfNotExists(isNew = partitionState.isNew, isFutureReplica = false,
            highWatermarkCheckpoints, topicIds(partitionState.topicName))
        }
      } catch {
        case e: KafkaStorageException =>
          //offline log directory
          markPartitionOffline(partition.topicPartition)
          responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
      }
    }

    // Stopping the fetchers must be done first in order to initialize the fetch
    // position correctly.
    replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(_.topicPartition))

    partitionsToMakeFollower.foreach { partition =>
      //完成期间的日志读写请求
      completeDelayedFetchOrProduceRequests(partition.topicPartition)
    }

    if (isShuttingDown.get()) {
    } else {
      //初始化副本同步信息
      val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map { partition =>
        val leaderNode = partition.leaderReplicaIdOpt.flatMap(leaderId => metadataCache.
          getAliveBrokerNode(leaderId, config.interBrokerListenerName)).getOrElse(Node.noNode())
        val leader = new BrokerEndPoint(leaderNode.id(), leaderNode.host(), leaderNode.port())
        val log = partition.localLogOrException
        val fetchOffset = initialFetchOffset(log)
        partition.topicPartition -> InitialFetchState(topicIds(partition.topic), leader, partition.getLeaderEpoch, fetchOffset)
      }.toMap
      //启动副本同步
      replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)
    }
  } catch {
    case e: Throwable =>
      // Re-throw the exception for it to be caught in KafkaApis
      throw e
  }
  partitionsToMakeFollower
}
```

处理流程概述如下：

* 1、确保leader副本broker节点在线，否则停止将本地副本设置为follower，但若有需要，仍需完成本地副本日志的创建；
* 2、调用Partition#makeFollower()方法，将本地副本设置为follower(若无则创建本地副本日志)，并更新内存中的分区副本方案；
* 3、停止分区副本的同步线程，转为follower期间不再进行同步操作，确保之后初始化同步信息时数据正确；
* 4、完成延迟的消息日志读写请求；
* 5、构建新的同步信息并开始副本同步。


# Replica下线及删除-StopReplicaRequest

controller节点对分区的某一个Replica执行删除操作时，会调用ReplicaStateMachine进行状态转换，状态流转过程为currentStatus->OfflineReplica->ReplicaDeletionStarted->ReplicaDeletionSuccessful->NonExistentReplica，
再此过程中，controller会向目标broker(待删除Replica所在broker)发送两次StopReplicaRequest请求：

* Replica状态由currentStatus->OfflineReplica，发送第一次StopReplicaRequest(deletePartition=false)；
* Replica状态由OfflineReplica->OfflineReplica，发送第一次StopReplicaRequest(deletePartition=true))；

ReplicaStateMachine源码分析见[Kafka 分区状态机与副本状态机](https://guann1ng.github.io/kafka/2021/10/30/Kafka-%E5%88%86%E5%8C%BA%E7%8A%B6%E6%80%81%E6%9C%BA%E4%B8%8E%E5%89%AF%E6%9C%AC%E7%8A%B6%E6%80%81%E6%9C%BA/) 。目标broker收到请求后，开始分步执行Replica的删除操作，
下面通过broker对StopReplicaRequest的处理来分析如何完成副本的删除。


## handleStopReplicaRequest

StopReplicaRequest请求处理的入口方法为KafkaApis#handleStopReplicaRequest()，源码如下：

```
  def handleStopReplicaRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldNeverReceive(request))
    val stopReplicaRequest = request.body[StopReplicaRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    if (isBrokerEpochStale(zkSupport, stopReplicaRequest.brokerEpoch)) {
      //请求中的controllerEpoch失效，忽略请求
      requestHelper.sendResponseExemptThrottle(request, new StopReplicaResponse(new StopReplicaResponseData().setErrorCode(Errors.STALE_BROKER_EPOCH.code)))
    } else {
      val partitionStates = stopReplicaRequest.partitionStates().asScala
      //处理用户定义主题副本停止请求
      val (result, error) = replicaManager.stopReplicas( request.context.correlationId, stopReplicaRequest.controllerId, stopReplicaRequest.controllerEpoch, partitionStates)
      // Clear the coordinator caches in case we were the leader. In the case of a reassignment, we
      // cannot rely on the LeaderAndIsr API for this since it is only sent to active replicas.
      //内部主题副本停止请求
      result.forKeyValue { (topicPartition, error) =>
        if (error == Errors.NONE) {
          val partitionState = partitionStates(topicPartition)
          if (topicPartition.topic == GROUP_METADATA_TOPIC_NAME && partitionState.deletePartition) {
            //__consumer_offset
            val leaderEpoch = if (partitionState.leaderEpoch >= 0)
              Some(partitionState.leaderEpoch)
            else
              None
            groupCoordinator.onResignation(topicPartition.partition, leaderEpoch)
          } else if (topicPartition.topic == TRANSACTION_STATE_TOPIC_NAME && partitionState.deletePartition) {
            //__transaction_state
            val leaderEpoch = if (partitionState.leaderEpoch >= 0)
              Some(partitionState.leaderEpoch)
            else
              None
            txnCoordinator.onResignation(topicPartition.partition, coordinatorEpoch = leaderEpoch)
          }
        }
      }
      ...
    }
    //停止无用线程
    CoreUtils.swallow(replicaManager.replicaFetcherManager.shutdownIdleFetcherThreads(), this)
  }
```

对StopReplicaRequest请求的处理可分为两部分：

* 通过调用ReplicaManager#stopReplicas()方法执行副本下线及删除操作；
* 需删除副本所属的Topic为Kafka内部主题，则需进行额外操作：
  * 组消费位移Topic**`__consumer_offset`**，调用GroupCoordinator#onResignation()方法执行；
  * 事务消息Topic**`__transaction_state`**，调用TransactionCoordinator#onResignation()方法执行；

### stopReplicas

ReplicaManager#stopReplicas()方法源码如下：

```
  def stopReplicas(correlationId: Int, controllerId: Int, controllerEpoch: Int,
                   partitionStates: Map[TopicPartition, StopReplicaPartitionState]
                  ): (mutable.Map[TopicPartition, Errors], Errors) = {
    //加锁              
    replicaStateChangeLock synchronized {
      val responseMap = new collection.mutable.HashMap[TopicPartition, Errors]
      if (controllerEpoch < this.controllerEpoch) {
        //controllerEpoch无效
        (responseMap, Errors.STALE_CONTROLLER_EPOCH)
      } else {
        this.controllerEpoch = controllerEpoch
        //需执行删除的分区列表
        val stoppedPartitions = mutable.Map.empty[TopicPartition, Boolean]
        partitionStates.forKeyValue { (topicPartition, partitionState) =>
          //是否执行删除
          val deletePartition = partitionState.deletePartition()

          getPartition(topicPartition) match {
            case HostedPartition.Offline =>
              //partition is in an offline log directory
              responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)

            case HostedPartition.Online(partition) =>
              val currentLeaderEpoch = partition.getLeaderEpoch
              val requestLeaderEpoch = partitionState.leaderEpoch
              // When a topic is deleted, the leader epoch is not incremented. To circumvent this,
              // a sentinel value (EpochDuringDelete) overwriting any previous epoch is used.
              // When an older version of the StopReplica request which does not contain the leader
              // epoch, a sentinel value (NoEpoch) is used and bypass the epoch validation.
              if (requestLeaderEpoch == LeaderAndIsr.EpochDuringDelete ||
                  requestLeaderEpoch == LeaderAndIsr.NoEpoch ||
                  requestLeaderEpoch > currentLeaderEpoch) {
                //
                stoppedPartitions += topicPartition -> deletePartition
                // Assume that everything will go right. It is overwritten in case of an error.
                responseMap.put(topicPartition, Errors.NONE)
              } else if (requestLeaderEpoch < currentLeaderEpoch) {
                //leaderEpoch无效
                responseMap.put(topicPartition, Errors.FENCED_LEADER_EPOCH)
              } else {
                responseMap.put(topicPartition, Errors.FENCED_LEADER_EPOCH)
              }

            case HostedPartition.None =>
              // Delete log and corresponding folders in case replica manager doesn't hold them anymore.
              // This could happen when topic is being deleted while broker is down and recovers.
              stoppedPartitions += topicPartition -> deletePartition
              responseMap.put(topicPartition, Errors.NONE)
          }
        }
        //进行删除
        stopPartitions(stoppedPartitions).foreach { case (topicPartition, e) =>
          ...// 异常记录
          responseMap.put(topicPartition, Errors.forException(e))
        }
        (responseMap, Errors.NONE)
      }
    }
  }
```

stopReplicas()方法的核心内容是遍历StopReplicaRequest请求体中的`partitionStates: Map[TopicPartition, StopReplicaPartitionState]`集合，确认请求有效(controllerEpoch及leaderEpoch有效)后，
将分区添加到stoppedPartitions(`mutable.Map.empty[TopicPartition, Boolean]`)中，调用stopPartitions()方法执行删除操作。

stoppedPartitions为Map结构，key为相应的TopicPartition,value为Boolean类型，表示是否删除副本日志，即构建StopReplicaRequest请求时的deletePartition参数。

### stopPartitions

stopPartitions()方法源码如下：

```
  protected def stopPartitions(
    partitionsToStop: Map[TopicPartition, Boolean]
  ): Map[TopicPartition, Throwable] = {
    //停止副本同步
    val partitions = partitionsToStop.keySet
    replicaFetcherManager.removeFetcherForPartitions(partitions)
    replicaAlterLogDirsManager.removeFetcherForPartitions(partitions)

    // Second remove deleted partitions from the partition map. Fetchers rely on the
    // ReplicaManager to get Partition's information so they must be stopped first.
    val partitionsToDelete = mutable.Set.empty[TopicPartition]
    partitionsToStop.forKeyValue { (topicPartition, shouldDelete) =>
      //判断是否需要删除
      if (shouldDelete) {
        getPartition(topicPartition) match {
          case hostedPartition: HostedPartition.Online =>
            //删除分区信息
            if (allPartitions.remove(topicPartition, hostedPartition)) {
              maybeRemoveTopicMetrics(topicPartition.topic)
              // Logs are not deleted here. They are deleted in a single batch later on.
              // This is done to avoid having to checkpoint for every deletions.
              hostedPartition.partition.delete()
            }

          case _ =>
        }
        //添加待删除集合
        partitionsToDelete += topicPartition
      }
      // If we were the leader, we may have some operations still waiting for completion.
      // We force completion to prevent them from timing out.
      completeDelayedFetchOrProduceRequests(topicPartition)
    }

    // Third delete the logs and checkpoint.
    val errorMap = new mutable.HashMap[TopicPartition, Throwable]()
    if (partitionsToDelete.nonEmpty) {
      // Delete the logs and checkpoint.
      //执行日志删除
      logManager.asyncDelete(partitionsToDelete, (tp, e) => errorMap.put(tp, e))
    }
    errorMap
  }
```

从源码可知，第一次发送StopReplicaRequest(deletePartition=false)请求时，即副本下线，只是将副本同步停止，并完成期间的读写请求。第二次发送StopReplicaRequest(deletePartition=true)请求时，则会
调用LogManager#asyncDelete()方法开始副本日志删除操作。


本篇内容没有分析Kafka内部主题__consumer_offset和__transaction_state相关的副本操作，感兴趣的可自行翻看，涉及方法为RequestHandlerHelper#onLeadershipChange()、GroupCoordinator#onResignation()以及
TransactionCoordinator#onResignation()方法。