---
layout: post 
title:  Kafka Replica同步机制
date:   2021-12-07 09:25:31 
categories: Kafka
---

Kafka为分区引入多副本机制，提升了数据容灾能力，但副本集合中只有leader副本负责处理ProduceRequest，完成客户端生产的消息写入，而其它follower副本节点则需向leader副本节点发送
FetchRequest获取最新的消息并写入本地副本日志中，保证数据一致性，此过程即副本同步过程。

# 概念

* AR：TopicPartition的所有Replica(All Replica);
* ISR：与leader副本**保持一定程度同步**的副本(包括leader副本在内)组成的副本集合即ISR(In-Sync Replicas);
* OSR：于leader副本同步滞后过多的副本组成的副本集合即OSR(Out-of-Sync Replicas）。

即 AR=ISR+OSR。

* LogStartOffset：当前日志文件中第一条消息的offset;
* LEO：LEO是Log End Offset缩写，它标识当前日志文件中下一条待写入消息offset；
* HW：HW是High Watermark缩写，消费者客户端只能拉取到这个offset前的消息；

![日志偏移量含义](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/hw%26leo.png)


# 副本同步

[Kafka Replica的生命周期](https://guann1ng.github.io/kafka/2021/11/10/Kafka-Replica%E7%9A%84%E7%94%9F%E5%91%BD%E5%91%A8%E6%9C%9F/) 中分析了Broker节点对LeaderAndIsrRequest请求的处理，
若本地Replica被选为follower角色，则会调用AbstractFetcherManager#addFetcherForPartitions()方法启动同步线程开启副本同步，源码如下：

```
def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit = {
lock synchronized {
  val partitionsPerFetcher = partitionAndOffsets.groupBy { case (topicPartition, brokerAndInitialFetchOffset) =>
    /**
     * 为每个待同步分区初始化 FetcherId标识
     * FetcherId = Utils.abs(31 * topicPartition.topic.hashCode() + topicPartition.partition) % numFetchersPerBroker
     */
    BrokerAndFetcherId(brokerAndInitialFetchOffset.leader, getFetcherId(topicPartition))
  }

  //同步线程创建及启动方法
  def addAndStartFetcherThread(brokerAndFetcherId: BrokerAndFetcherId,
                               brokerIdAndFetcherId: BrokerIdAndFetcherId): T = {
    val fetcherThread = createFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker)
    //记录broker中所有的同步线程
    fetcherThreadMap.put(brokerIdAndFetcherId, fetcherThread)
    fetcherThread.start()
    fetcherThread
  }

  //遍历待同步分区
  for ((brokerAndFetcherId, initialFetchOffsets) <- partitionsPerFetcher) {
    val brokerIdAndFetcherId = BrokerIdAndFetcherId(brokerAndFetcherId.broker.id, brokerAndFetcherId.fetcherId)
    //获取同步线程
    val fetcherThread = fetcherThreadMap.get(brokerIdAndFetcherId) match {
      case Some(currentFetcherThread) if currentFetcherThread.sourceBroker == brokerAndFetcherId.broker =>
        // 可重用
        currentFetcherThread
      case Some(f) =>
        //brokerId 和 FetcherId 一致，但broker节点的ip 端口已变化
        //关闭旧的  创建并启动新的同步线程
        f.shutdown()
        addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
      case None =>
        addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
    }
    //为线程添加同步信息
    addPartitionsToFetcherThread(fetcherThread, initialFetchOffsets)
  }
}
}
```

addFetcherForPartitions()的主要作用是**完成同步线程ReplicaFetcherThread的创建及启动**，可分为三步：

* 1、为每个待同步TopicPartition初始化FetcherId，计算公式为`Utils.abs(31 * topicPartition.topic.hashCode() + topicPartition.partition) % numFetchersPerBroker`，其中numFetchersPerBroker
为配置**`num.replica.fetchers`**的值，含义为：**从一个broker同步数据的fetcher线程数，即并不一定会为每一个TopicPartition都启动一个fetcher线程，对于一个目的broker，只会启动num.replica.fetchers个线程，增加这个值时即增加该broker的IO并行度**；

* 2、查询缓存fetcherThreadMap，若已有brokerId与fetcherId一致的同步线程，则复用，否则调用addAndStartFetcherThread()方法创建并启动线程；

* 3、调用addPartitionsToFetcherThread()方法为线程添加分区同步信息。

# 创建同步线程-ReplicaFetcherThread

创建ReplicaFetcherThread的源码如下：

```
  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaFetcherThread = {
    val prefix = threadNamePrefix.map(tp => s"$tp:").getOrElse("")
    val threadName = s"${prefix}ReplicaFetcherThread-$fetcherId-${sourceBroker.id}"
    new ReplicaFetcherThread(threadName, fetcherId, sourceBroker, brokerConfig, failedPartitions, replicaManager,
      metrics, time, quotaManager)
  }
```

副本同步线程类ReplicaFetcherThread创建时共有以下参数可供配置：

| 参数         | 说明                                                                                                             | 默认值  |
|------------|----------------------------------------------------------------------------------------------------------------|------|
| num.replica.fetchers | 从某个leader broker复制消息的fetcher线程数(IO并发度)                                                                         | 1    | 
|replica.fetch.backoff.ms| fetch发生错误后，重试前的等待时间                                                                                            | 1000 |
|replica.fetch.max.bytes| 为每个分区获取的消息最大字节数，不绝对，参考`max.message.bytes`                                                                      | 1024 * 1024                                                                                                          |
|replica.fetch.min.bytes| 每次fetch请求最少拉取的消息字节数，如果不满足这个条件，那么要等待replica.fetch.wait.max.ms                                                   | 1                                                                                                                    |
|replica.lag.time.max.ms| 如果一个follower在这个时间内没有发送任何fetch请求或者在这个时间内没有追上leader当前的 log end offset，那么将会从ISR中移除                                | 30000                                                                                                                |
|replica.fetch.wait.max.ms| follower副本节点发送Fetch请求的最大等待时间，replica.fetch.wait.max.ms应小于或等于replica.lag.time.max.ms，防止topic吞吐量过低导致的ISR集合频繁变动   | 500     |
|replica.fetch.response.max.bytes| fetch响应返回的最大字节数，**并不绝对**，若拉取的第一条消息大于该值但小于message.max.bytes (broker config) or max.message.bytes (topic config)，仍然会返回 |10 * 1024 * 1024|


# 线程任务-doWork

完成ReplicaFetcherThread创建并启动后，开始执行副本同步任务，ReplicaFetcherThread#doWork()方法源码如下：

```
  override def doWork(): Unit = {
    //尝试日志截断
    maybeTruncate()
    //尝试从Leader副本拉取消息
    maybeFetch()
  }
```


## 副本同步-FetchRequest

maybeFetch()会尝试触发一次副本同步请求，先调用buildFetch()构建FetchRequest，然后调用processFetchRequest()进行请求发送及响应处理，源码如下：

```
  private def maybeFetch(): Unit = {
    val fetchRequestOpt = inLock(partitionMapLock) {
      //为所有分区构造FetchRequest
      val ResultWithPartitions(fetchRequestOpt, partitionsWithError) = buildFetch(partitionStates.partitionStateMap.asScala)
      //FetchRequest创建失败，对分区同步信息添加延迟fetchBackOffMs
      handlePartitionsWithErrors(partitionsWithError, "maybeFetch")
      if (fetchRequestOpt.isEmpty) {
        //无可拉取的分区
        trace(s"There are no active partitions. Back off for $fetchBackOffMs ms before sending a fetch request")
        partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
      }
      fetchRequestOpt
    }
    //发送请求
    fetchRequestOpt.foreach { case ReplicaFetch(sessionPartitions, fetchRequest) =>
      processFetchRequest(sessionPartitions, fetchRequest)
    }
  }
```

### BuildFetchRequest

ReplicaFetcherThread#buildFetch()的主要作用是完成同步请求的构建，FetchRequest的主要属性如下：

```
private final int maxWait;
private final int minBytes;
private final int replicaId;
private final Map<TopicPartition, PartitionData> toFetch; //同步分区数据
```

不同分区的副本同步请求数据以Map的数据结构存放，value对象为PartitionData，其数据结构如下：

```
Uuid topicId,
long fetchOffset, //本地日志的LEO,即本次同步的消息日志起始位置
long logStartOffset, 
int maxBytes,
Optional<Integer> currentLeaderEpoch, //本地副本保存的当前leaderEpoch
Optional<Integer> lastFetchedEpoch //上次同步请求的leaderEpoch
```


### ProcessFetchRequest

processFetchRequest()方法的主要内容是向leader broker发送FetchRequest，并对响应进行处理，请求的发送通过调用ReplicaFetcherThread#fetchFromLeader()方法实现，
底层仍是通过Java NIO实现，但会通过while循环实现Fetch请求的阻塞，请求结束后会关闭与leader broker的channel。

下面主要分析下leader节点对fetch请求的处理与follower节点处理响应数据。


#### Leader节点请求处理

Broker节点对FetchRequest请求的处理在[Kafka 消息日志读取-HandleFetchRequest](https://guann1ng.github.io/kafka/2021/11/30/Kafka-%E6%B6%88%E6%81%AF%E6%97%A5%E5%BF%97%E8%AF%BB%E5%8F%96-HandleFetchRequest/) 中已经详细分析过，如果请求
是follower副本的同步请求，leader节点需要记录follower副本的同步状态，源码如下：

```
  def fetchMessages(...): Unit = {
    val isFromFollower = Request.isValidBrokerId(replicaId)
    val isFromConsumer = !(isFromFollower || replicaId == Request.FutureLocalReplicaId)
    val fetchIsolation = if (!isFromConsumer)
      FetchLogEnd  // 副本同步请求，可读取的范围为LEO
    else if (isolationLevel == IsolationLevel.READ_COMMITTED)
      FetchTxnCommitted
    else
      FetchHighWatermark

    // Restrict fetching to leader if request is from follower or from a client with older version (no ClientMetadata)
    val fetchOnlyFromLeader = isFromFollower || (isFromConsumer && clientMetadata.isEmpty)
    def readFromLog(): Seq[(TopicIdPartition, LogReadResult)] = {
      //读取日志
      val result = readFromLocalLog(...)
      //更新同步状态
      if (isFromFollower) updateFollowerFetchState(replicaId, result)
      else result
    }
    
    ...  //other code
```

updateFollowerFetchState()方法的作用是更新leader端记录follower副本的同步状态，**主要涉及两部分内容：follower副本同步状态更新以及判断是否需要添加follower副本至
ISR中，分别对应方法Replica#updateFetchState()和方法Partition#maybeExpandIsr()**。

##### updateFetchState

Replica#updateFetchState()方法源码如下：

```
  def updateFetchState(followerFetchOffsetMetadata: LogOffsetMetadata,
                       followerStartOffset: Long,
                       followerFetchTimeMs: Long,
                       leaderEndOffset: Long): Unit = {
    //更新follower达到同步状态的时间
    if (followerFetchOffsetMetadata.messageOffset >= leaderEndOffset)
      _lastCaughtUpTimeMs = math.max(_lastCaughtUpTimeMs, followerFetchTimeMs)
    else if (followerFetchOffsetMetadata.messageOffset >= lastFetchLeaderLogEndOffset)
      _lastCaughtUpTimeMs = math.max(_lastCaughtUpTimeMs, lastFetchTimeMs)

    //保存follower副本的LSO LEO 
    _logStartOffset = followerStartOffset
    _logEndOffsetMetadata = followerFetchOffsetMetadata
    // 记录fetch时， leader的LEO
    lastFetchLeaderLogEndOffset = leaderEndOffset
    // 记录fetch的时间
    lastFetchTimeMs = followerFetchTimeMs
  }
```

follower副本的同步状态信息包括上一次达到同步状态的时间点，上一次follower副本发送fetch请求的时间点及follower副本的LSO LEO信息等，依据这些信息，leader副本才能判断出follower副本能否在ISR列表中。


##### maybeExpandIsr

maybeExpandIsr()方法源码如下：

```
  private def maybeExpandIsr(followerReplica: Replica): Unit = {
    //是否需要更新ISR
    val needsIsrUpdate = !isrState.isInflight && canAddReplicaToIsr(followerReplica.brokerId) && inReadLock(leaderIsrUpdateLock) {
      needsExpandIsr(followerReplica)
    }
    if (needsIsrUpdate) {
      //数据准备
      val alterIsrUpdateOpt = inWriteLock(leaderIsrUpdateLock) {
        // check if this replica needs to be added to the ISR
        if (!isrState.isInflight && needsExpandIsr(followerReplica)) {
          Some(prepareIsrExpand(followerReplica.brokerId))
        } else {
          None
        }
      }
      // Send the AlterIsr request outside of the LeaderAndIsr lock since the completion logic
      // may increment the high watermark (and consequently complete delayed operations).
      alterIsrUpdateOpt.foreach(submitAlterIsr)
    }
  }

  private def needsExpandIsr(followerReplica: Replica): Boolean = {
    //followerEndOffset >= leaderLog.highWatermark && leaderEpochStartOffsetOpt.exists(followerEndOffset >= _)
    canAddReplicaToIsr(followerReplica.brokerId) && isFollowerAtHighwatermark(followerReplica)
  }

  private def canAddReplicaToIsr(followerReplicaId: Int): Boolean = {
    val current = isrState
    //没有待响应的AlterIsr request 且当前follower副本不再ISR中
    !current.isInflight && !current.isr.contains(followerReplicaId)
  }
```

当前副本可添加到ISR集合中的主要条件为：

* followerEndOffset >= leaderLog.highWatermark，即follower副本的LEO>=leader副本的HW，表示follower副本已达到同步；
* leaderEpochStartOffsetOpt.exists(followerEndOffset >= _)，follower的fetchOffset至少比一个leader副本的日志段的LogStartOffset大，保证fetchOffset有效，防止数据丢失。

最后一步`alterIsrUpdateOpt.foreach(submitAlterIsr)`会遍历待更新的分区ISR集合信息，将新的LeaderAndIsr写入ZK节点`/brokers/topics/${topic}/partitions/${partitionIndex}/state`中。


#### Fetch响应处理

AbstractFetcherThread#processFetchRequest()方法中，关于响应处理的核心代码如下：

```
//kafka.server.AbstractFetcherThread#processFetchRequest
//处理拉取的数据
val logAppendInfoOpt = processPartitionData(topicPartition, currentFetchState.fetchOffset, partitionData)

logAppendInfoOpt.foreach { logAppendInfo =>
  val validBytes = logAppendInfo.validBytes
  val nextOffset = if (validBytes > 0) logAppendInfo.lastOffset + 1 else currentFetchState.fetchOffset
  val lag = Math.max(0L, partitionData.highWatermark - nextOffset)
  fetcherLagStats.getAndMaybePut(topicPartition).lag = lag

  // ReplicaDirAlterThread may have removed topicPartition from the partitionStates after processing the partition data
  if (validBytes > 0 && partitionStates.contains(topicPartition)) {
    // 更新副本同步状态信息
    val newFetchState = PartitionFetchState(currentFetchState.topicId, nextOffset, Some(lag),
      currentFetchState.currentLeaderEpoch, state = Fetching,logAppendInfo.lastLeaderEpoch)
    partitionStates.updateAndMoveToEnd(topicPartition, newFetchState)
    fetcherStats.byteRate.mark(validBytes)
  }
}
```

主要分为两步：

* 调用processPartitionData()方法完成响应的数据处理；
* 更新副本同步状态；

#### processPartitionData

processPartitionData()源码如下：

```
  override def processPartitionData(topicPartition: TopicPartition,
                                    fetchOffset: Long,
                                    partitionData: FetchData): Option[LogAppendInfo] = {
    val logTrace = isTraceEnabled
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    //本地副本日志
    val log = partition.localLogOrException
    //拉取的消息记录
    val records = toMemoryRecords(FetchResponse.recordsOrFail(partitionData))
    //旧版本error信息
    maybeWarnIfOversizedRecords(records, topicPartition)

    //请求的fetchOffset与当前日志LEO不一致，异常
    if (fetchOffset != log.logEndOffset)
      throw new IllegalStateException("Offset mismatch for partition %s: fetched offset = %d, log end offset = %d.".format(
        topicPartition, fetchOffset, log.logEndOffset))

    //日志追加 更新自身的LEO
    val logAppendInfo = partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false)

    val leaderLogStartOffset = partitionData.logStartOffset

    // For the follower replica, we do not need to keep its segment base offset and physical position.
    // These values will be computed upon becoming leader or handling a preferred read replica fetch.
    //更新副本的HW 和 LSO
    val followerHighWatermark = log.updateHighWatermark(partitionData.highWatermark)
    log.maybeIncrementLogStartOffset(leaderLogStartOffset, LeaderOffsetIncremented)

    // 限流记录
    if (quota.isThrottled(topicPartition))
      quota.record(records.sizeInBytes)
    if (partition.isReassigning && partition.isAddingLocalReplica)
      brokerTopicStats.updateReassignmentBytesIn(records.sizeInBytes)
    brokerTopicStats.updateReplicationBytesIn(records.sizeInBytes)

    logAppendInfo
  }
```

该方法主要是将拉取的消息追加到本地日志中，并完成LEO、HW及LSO(follower副本无需保存实际的logSegment的base offset and physical position)的更新。







