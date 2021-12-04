---
layout: post
title:  Kafka Poll
date:   2021-10-12 17:18:31
categories: Kafka
---

前面几篇内容分析了`updateAssignmentMetadataIfNeeded()`的执行流程，包含两部分内容：

* ConsumerCoordinator#poll方法，获取GroupCoordinator，完成JoinGroup及主题分区方案获取，详情见[Kafka Consumer JoinGroup](https://guann1ng.github.io/kafka/2021/09/06/Kafka-Consumer-JoinGroup/)；
* KafkaConsumer#updateFetchPositions方法，更新consumer订阅的TopicPartition的有效offset，确认下次消息拉取的偏移量(offset)，详情见[Kafka Consumer UpdateFetchPosition](https://guann1ng.github.io/kafka/2021/09/17/Kafka-Consumer-UpdateFetchPosition/)。

下面继续分析KafkaConsumer#poll方法的后半部分内容，即消息拉取部分，核心方法为pollForFetches()。

```
private ConsumerRecords<K, V> poll(final Timer timer, final boolean includeMetadataInTimeout) {
     //consumer不是线程安全的， CAS设置当前threadId获取锁，并确认consumer未关闭
    acquireAndEnsureOpen();
    try {
        ...
        do {  
            //更新元数据请求 updateAssignmentMetadataIfNeeded
            ...

            //拉取消息
            Map<TopicPartition, List<ConsumerRecord<K, V>>> records = pollForFetches(timer);
            if (!records.isEmpty()) {
                //如果拉取到的消息集合不为空，再返回该批消息之前，如果还有挤压的拉取请求，继续发送拉取请求，
                if (fetcher.sendFetches() > 0 || client.hasPendingRequests()) {
                    client.transmitSends();
                }
                //消费者拦截器
                return this.interceptors.onConsume(new ConsumerRecords<>(records));
            }
        } while (timer.notExpired());
        //超时未拉取到消息，返回空集合
        return ConsumerRecords.empty();
    } finally {
        release(); //释放锁
        this.kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
    }
}
```

# pollForFetches

pollForFetches方法的源码如下，方法可分为三部分：

* 发送消息拉取请求的**Fetcher#sendFetches()**；
* 触发网络读写事件的**ConsumerNetworkClient#poll()**，底层为Java NIO;
* 获取本地已拉取完成的消息记录的**Fetcher#fetchedRecords()**。

```
private Map<TopicPartition, List<ConsumerRecord<K, V>>> pollForFetches(Timer timer) {
    //获取拉取超时时间
    long pollTimeout = coordinator == null ? timer.remainingMs() : Math.min(coordinator.timeToNextPoll(timer.currentTimeMs()), timer.remainingMs());
    //本地已有拉取的消息，返回
    Map<TopicPartition, List<ConsumerRecord<K, V>>> records = fetcher.fetchedRecords();
    if (!records.isEmpty()) {
        return records;
    }
    //发送消息拉取请求 send any new fetches (won't resend pending fetches)
    fetcher.sendFetches();
    if (!cachedSubscriptionHashAllFetchPositions && pollTimeout > retryBackoffMs) {
        pollTimeout = retryBackoffMs;
    }
    Timer pollTimer = time.timer(pollTimeout);
    //NetworkClient poll 触发网络读写
    client.poll(pollTimer, () -> {
        return !fetcher.hasAvailableFetches();
    });
    timer.update(pollTimer.currentTimeMs());
    //返回拉取数据
    return fetcher.fetchedRecords();
}
```

可以看到，消息拉取的核心方法为sendFetches()及fetchedRecords()：

# sendFetches

sendFetches()方法的主要作用是向consumer订阅的所有可发送的TopicPartition发送FetchRequest拉取消息，并将结果缓存到consumer本地。

## sendFetchRequest

Fetch请求发送流程如下：

```
public synchronized int sendFetches() {
    sensors.maybeUpdateAssignment(subscriptions);
    //准备请求数据，主要是获取可发送的分区节点及相应的请求数据
    Map<Node, FetchSessionHandler.FetchRequestData> fetchRequestMap = prepareFetchRequests();
    for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequestMap.entrySet()) {
        final Node fetchTarget = entry.getKey();
        final FetchSessionHandler.FetchRequestData data = entry.getValue();
        final short maxVersion;
        if (!data.canUseTopicIds()) {
            maxVersion = (short) 12;
        } else {
           //ApiKeys.FETCH 请求类型
            maxVersion = ApiKeys.FETCH.latestVersion();
        }
        //构建拉取请求
        final FetchRequest.Builder request = FetchRequest.Builder
                //fetch.max.wait.ms: 拉取的等待时间   
                //fetch.min.bytes: 至少拉取的字节数，没有达到则等待
                .forConsumer(maxVersion, this.maxWaitMs, this.minBytes, data.toSend(), data.topicIds())
                .isolationLevel(isolationLevel) //消费者隔离级别 事务消息
                //一次消息拉取的最大字节数 fetch.max.bytes
                .setMaxBytes(this.maxBytes) 
                .metadata(data.metadata())
                .toForget(data.toForget())
                .rackId(clientRackId);

        //通过ConsuemrNetworkClient发送请求
        RequestFuture<ClientResponse> future = client.send(fetchTarget, request);
        //标记已有拉取请求的Broker
        this.nodesWithPendingFetchRequests.add(entry.getKey().id());
        //消息拉取响应监听器
        future.addListener(new RequestFutureListener<ClientResponse>(){...});
    }
    return fetchRequestMap.size();
}
```
方法可分为3步：

* 1、prepareFetchRequests()方法中获取所有可发送FetchRequest的分区节点与对应的请求数据。**可进行消息拉取的分区有以下三点要求**：
  * TopicPartition之前的拉取响应数据已全部处理(详见fetchablePartitions()方法)。
  * TopicPartition对应的分区副本节点有效(连接正常)；
  * 待读取副本所在节点没有待发送或挂起的请求，避免请求积压；

* 2、遍历第一步返回的<Broker,FetchRequestData>集合，构建FetchRequest，并调用NetworkClient发送；
* 3、为请求Future对象设置响应处理的Listener。

### selectReadReplica

Kafka2.4后支持consumer从follower副本中读取消息，以减少集群环境下跨数据中心的流量，prepareFetchRequests()方法获取目标副本节点时优先使用Broker返回的preferredReadReplica节点。

```
  Node selectReadReplica(TopicPartition partition, Node leaderReplica, long currentTimeMs) {
      //获取Broker返回的preferredReadReplica节点
      Optional<Integer> nodeId = subscriptions.preferredReadReplica(partition, currentTimeMs);
      if (nodeId.isPresent()) {
          Optional<Node> node = nodeId.flatMap(id -> metadata.fetch().nodeIfOnline(partition, id));
            //节点可用
          if (node.isPresent()) {
              return node.get();
          } else {
              //preferredReadReplica节点不可用，清理标记
              log.trace("Not fetching from {} for partition {} since it is marked offline or is missing from our metadata, using the leader instead.", nodeId, partition);
              subscriptions.clearPreferredReadReplica(partition);
              return leaderReplica;
          }
      } else {
          //返回leader副本节点
          return leaderReplica;
      }
  }
```

KafkaConsumer读取follower副本的特性可见：[KIP-392: Allow consumers to fetch from closest replica](https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica) 。

### send

FetchRequest的发送很简单，将请求放入待发送队列`unsent`中，调用NetworkClient的wakeup()方法，唤醒可能阻塞在poll中的NetworkClient，尽快的发送队列中的请求。

```
public RequestFuture<ClientResponse> send(Node node,AbstractRequest.Builder<?> requestBuilder,int requestTimeoutMs) {
    long now = time.milliseconds();
    RequestFutureCompletionHandler completionHandler = new RequestFutureCompletionHandler();
    ClientRequest clientRequest = client.newClientRequest(node.idString(), requestBuilder, now, true,requestTimeoutMs, completionHandler);
    unsent.put(node, clientRequest);
    client.wakeup();
    return completionHandler.future;
}
```

## handleFetchRequest

Broker端处理FetchRequest的入口为KafkaApis#handleFetchRequest方法，相关方法调用链为：

* KafkaApis#handleFetchRequest
* ReplicaManager#fetchMessages
* Partition#readRecords
* LocalLog#read
* LogSegment#read

KafkaApis#handleFetchRequest()中更多的逻辑是参数验证及响应定义，下面从ReplicaManager#fetchMessages()方法开始分析。

### ReplicaManager#fetchMessages

fetchMessages()方法实现如下：

```
  //从主题分区leader副本中拉取数据，等待拉取足够的数据或超时返回
  //KIP-392: Allow consumers to fetch from closest replica
  def fetchMessages(...): Unit = {
    //follower副本同步请求                
    val isFromFollower = Request.isValidBrokerId(replicaId)
    //消费者拉取消息请求 
    val isFromConsumer = !(isFromFollower || replicaId == Request.FutureLocalReplicaId)
    
    //
    val fetchIsolation = if (!isFromConsumer)
      FetchLogEnd //同步请求，上限为log结尾
    else if (isolationLevel == IsolationLevel.READ_COMMITTED)
      FetchTxnCommitted  //  consumer隔离级别READ_COMMITTED 消息读取位置上限为LOS
    else
      FetchHighWatermark   

    //判断是否必须从leader副本拉取数据  follower副本的同步请求必须从leader副本读取，consumer2.4后支持从follower副本拉取
    val fetchOnlyFromLeader = isFromFollower || (isFromConsumer && clientMetadata.isEmpty)
    
    //日志读取方法定义
    def readFromLog(): Seq[(TopicIdPartition, LogReadResult)] = {
      //日志读取
      val result = readFromLocalLog( replicaId = replicaId, fetchOnlyFromLeader = fetchOnlyFromLeader, fetchIsolation = fetchIsolation, fetchMaxBytes = fetchMaxBytes, hardMaxBytesLimit = hardMaxBytesLimit, readPartitionInfo = fetchInfos, quota = quota, clientMetadata = clientMetadata)
      //更新同步请求进度  
      if (isFromFollower) updateFollowerFetchState(replicaId, result)
      else result
    }
    //执行
    val logReadResults = readFromLog()

    // check if this fetch request can be satisfied right away
    ...

    if (timeout <= 0 || fetchInfos.isEmpty || bytesReadable >= fetchMinBytes || errorReadingData || hasDivergingEpoch) {
      // 立即响应的情况            1) fetch request does not want to wait
      //                        2) fetch request does not require any data
      //                        3) has enough data to respond
      //                        4) some error happens while reading data
      //                        5) we found a diverging epoch  
      val fetchPartitionData = logReadResults.map { case (tp, result) =>
        val isReassignmentFetch = isFromFollower && isAddingReplica(tp.topicPartition, replicaId)
        tp -> result.toFetchPartitionData(isReassignmentFetch)
      }
      responseCallback(fetchPartitionData)
    } else {
      //延时响应任务
      // construct the fetch results from the read results
      val fetchPartitionStatus = new mutable.ArrayBuffer[(TopicIdPartition, FetchPartitionStatus)]
      fetchInfos.foreach { case (topicIdPartition, partitionData) =>
        logReadResultMap.get(topicIdPartition).foreach(logReadResult => {
          val logOffsetMetadata = logReadResult.info.fetchOffsetMetadata
          fetchPartitionStatus += (topicIdPartition -> FetchPartitionStatus(logOffsetMetadata, partitionData))
        })
      }
      val fetchMetadata: SFetchMetadata = SFetchMetadata(fetchMinBytes, fetchMaxBytes, hardMaxBytesLimit,
        fetchOnlyFromLeader, fetchIsolation, isFromFollower, replicaId, fetchPartitionStatus)
      //延时任务  
      val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, quota, clientMetadata,responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
      val delayedFetchKeys = fetchPartitionStatus.map { case (tp, _) => TopicPartitionOperationKey(tp) }
      delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
    }
  }
```

fetchMessages()方法执行流程如下：

* 确定可读取日志的范围及是否必须从leader副本读取数据：
  * fetch请求为follower副本的同步请求，则必须从leader副本读取，上限位置为FetchLogEnd，即日志末尾；
  * fetch请求为consumer拉取消息的请求，不要求必须从leader副本读取消息：
    * 若隔离级别为READ_COMMITTED，上限位置为LastStableOffset
    * 若隔离级别为READ_UNCOMMITTED，上限位置为HighWaterMark
* 调用readFromLocalLog()方法读取消息；
* 判断是否立即返回，否则通过延时操作延时返回。

#### readFromLocalLog

readFromLocalLog()方法可分为两部分内容：
* read()方法定义；
* 遍历所有分区并调用read方法读取数据。

```
  /**
   * Read from multiple topic partitions at the given offset up to maxSize bytes
   */
  def readFromLocalLog(...)] = {
    val traceEnabled = isTraceEnabled
    //read方法定义
    def read(tp: TopicIdPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {...}
    //剩余可读取字节数
    var limitBytes = fetchMaxBytes
    val result = new mutable.ArrayBuffer[(TopicIdPartition, LogReadResult)]
    var minOneMessage = !hardMaxBytesLimit
    //遍历分区调用read方法读取数据
    readPartitionInfo.foreach { case (tp, fetchInfo) =>
      val readResult = read(tp, fetchInfo, limitBytes, minOneMessage)
      val recordBatchSize = readResult.info.records.sizeInBytes
      //只要从分区读到一次消息，就把至少从一个分区读一条的配置项去掉 
      if (recordBatchSize > 0)
        minOneMessage = false
      //读完一个分区后，更新剩余可读取字节数
      limitBytes = math.max(0, limitBytes - recordBatchSize)
      result += (tp -> readResult)
    }
    result
  }
```

##### read

read()方法源码如下：

```
  def read(tp: TopicIdPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {
    val offset = fetchInfo.fetchOffset
    val partitionFetchSize = fetchInfo.maxBytes
    val followerLogStartOffset = fetchInfo.logStartOffset

    val adjustedMaxBytes = math.min(fetchInfo.maxBytes, limitBytes)
    try {
      
      val partition = getPartitionOrException(tp.topicPartition)
      val fetchTimeMs = time.milliseconds

      // Check if topic ID from the fetch request/session matches the ID in the log
      val topicId = if (tp.topicId == Uuid.ZERO_UUID) None else Some(tp.topicId)
      if (!hasConsistentTopicId(topicId, partition.topicId))
        throw new InconsistentTopicIdException("Topic ID in the fetch session did not match the topic ID in the log.")

      // 当前节点为leader节点，查找更适合consumer读取的preferredReadReplica
      val preferredReadReplica = clientMetadata.flatMap(
        metadata => findPreferredReadReplica(partition, metadata, replicaId, fetchInfo.fetchOffset, fetchTimeMs))

      if (preferredReadReplica.isDefined) {
        // 有相应的preferredReadReplica ，跳过消息读取，直接返回 preferredReadReplica响应，下次consumer poll再进行消息拉取
        val offsetSnapshot = partition.fetchOffsetSnapshot(fetchInfo.currentLeaderEpoch, fetchOnlyFromLeader = false)
        LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
          divergingEpoch = None, 
          ... // other param
          //返回preferredReadReplica
          preferredReadReplica = preferredReadReplica,
          exception = None)
      } else {
        // 开始进行分区副本消息读取
        val readInfo: LogReadInfo = partition.readRecords(
          lastFetchedEpoch = fetchInfo.lastFetchedEpoch,
          fetchOffset = fetchInfo.fetchOffset,
          currentLeaderEpoch = fetchInfo.currentLeaderEpoch,
          maxBytes = adjustedMaxBytes,
          fetchIsolation = fetchIsolation,
          fetchOnlyFromLeader = fetchOnlyFromLeader,
          minOneMessage = minOneMessage)

        val fetchDataInfo = if (shouldLeaderThrottle(quota, partition, replicaId)) {
          FetchDataInfo(readInfo.fetchedData.fetchOffsetMetadata, MemoryRecords.EMPTY)
        } else if (!hardMaxBytesLimit && readInfo.fetchedData.firstEntryIncomplete) {
          FetchDataInfo(readInfo.fetchedData.fetchOffsetMetadata, MemoryRecords.EMPTY)
        } else {
          readInfo.fetchedData
        }
        //返回消息结果
        LogReadResult(info = fetchDataInfo,
          divergingEpoch = readInfo.divergingEpoch,
          highWatermark = readInfo.highWatermark,
          leaderLogStartOffset = readInfo.logStartOffset,
          leaderLogEndOffset = readInfo.logEndOffset,
          followerLogStartOffset = followerLogStartOffset,
          fetchTimeMs = fetchTimeMs,
          lastStableOffset = Some(readInfo.lastStableOffset),
          preferredReadReplica = preferredReadReplica,
          exception = None)
      }
    } catch {
      ...//异常返回
  }

```

read()方法主要是调用Partition#readRecords()方法读取数据，并将读取结果封装为LogReadResult返回。进行分区消息读取前，还会**调用findPreferredReadReplica()方法判断是否有更适合当前consumer读取消息的分区，即PreferredReadReplica**，
若获取到PreferredReadReplica，则直接返回，下一次KafkaConsumer调用poll()方法拉取消息，则会发送Fetch请求至PreferredReadReplica。

##### findPreferredReadReplica

findPreferredReadReplica()实现如下：

```
  /**
    * Using the configured [[ReplicaSelector]], determine the preferred read replica for a partition given the
    * client metadata, the requested offset, and the current set of replicas. If the preferred read replica is the
    * leader, return None
    */
  def findPreferredReadReplica(partition: Partition,
                               clientMetadata: ClientMetadata,
                               replicaId: Int,
                               fetchOffset: Long,
                               currentTimeMs: Long): Option[Int] = {
    partition.leaderReplicaIdOpt.flatMap { leaderReplicaId =>
      // Don't look up preferred for follower fetches via normal replication
      if (Request.isValidBrokerId(replicaId))
        None
      else {
        replicaSelectorOpt.flatMap { replicaSelector =>
          //可用的ISR副本节点
          val replicaEndpoints = metadataCache.getPartitionReplicaEndpoints(partition.topicPartition, new ListenerName(clientMetadata.listenerName))
          //获取可供读取的副本集合
          val replicaInfos = partition.remoteReplicas
            // Exclude replicas that don't have the requested offset (whether or not if they're in the ISR)
            //follower副本日志位移小于consumer消费进度
            .filter(replica => replica.logEndOffset >= fetchOffset && replica.logStartOffset <= fetchOffset)
            .map(replica => new DefaultReplicaView(
              replicaEndpoints.getOrElse(replica.brokerId, Node.noNode()),
              replica.logEndOffset,
              currentTimeMs - replica.lastCaughtUpTimeMs))
          //leader副本
          val leaderReplica = new DefaultReplicaView(
            replicaEndpoints.getOrElse(leaderReplicaId, Node.noNode()),
            partition.localLogOrException.logEndOffset, 0L)
          
          //选择范围   
          val replicaInfoSet = mutable.Set[ReplicaView]() ++= replicaInfos += leaderReplica

          val partitionInfo = new DefaultPartitionView(replicaInfoSet.asJava, leaderReplica)
          //选择合适副本
          replicaSelector.select(partition.topicPartition, clientMetadata, partitionInfo).asScala.collect {
            // 可能返回leader副本，若为leader副本，返回None,consuemr直接从leader副本读取消息
            case selected if !selected.endpoint.isEmpty && selected != leaderReplica => selected.endpoint.id
          }
        }
      }
    }
  }
```

select()方法实现如下：

```
public Optional<ReplicaView> select(TopicPartition topicPartition,
                                    ClientMetadata clientMetadata,
                                    PartitionView partitionView) {
    //kafkaConsuemr发送的消息携带了机架信息                                
    if (clientMetadata.rackId() != null && !clientMetadata.rackId().isEmpty()) {
        Set<ReplicaView> sameRackReplicas = partitionView.replicas().stream()
                
                .filter(replicaInfo -> clientMetadata.rackId().equals(replicaInfo.endpoint().rack()))
                .collect(Collectors.toSet());
        if (sameRackReplicas.isEmpty()) {
            return Optional.of(partitionView.leader());
        } else {
            if (sameRackReplicas.contains(partitionView.leader())) {
                // leader副本也在同一机架，则直接返回leader副本
                return Optional.of(partitionView.leader());
            } else {
                // 多个则选择最优
                return sameRackReplicas.stream().max(ReplicaView.comparator());
            }
        }
    } else {
        //返回leader
        return Optional.of(partitionView.leader());
    }
}

static Comparator<ReplicaView> comparator() {
    return Comparator.comparingLong(ReplicaView::logEndOffset) //比较logEndOffset
        .thenComparing(Comparator.comparingLong(ReplicaView::timeSinceLastCaughtUpMs).reversed()) // 距high watermark时间
        .thenComparing(replicaInfo -> replicaInfo.endpoint().id()); //副本所在brokerId
}
```

可以看到PreferredReadReplica的获取**依赖于机架信息的配置**，KafkaConsumer端的配置为`client.rack`，Broker机架配置项为`broker.rack`。

### Partition#readRecords

完成待读取的TopicPartition的副本选择后，即可执行消息读取的下一阶段：调用Partition#readRecords()方法，源码如下：

```
  def readRecords(lastFetchedEpoch: Optional[Integer],
                  fetchOffset: Long,
                  currentLeaderEpoch: Optional[Integer],
                  maxBytes: Int,
                  fetchIsolation: FetchIsolation,
                  fetchOnlyFromLeader: Boolean,
                  minOneMessage: Boolean): LogReadInfo = inReadLock(leaderIsrUpdateLock) {
                  
    // 获取日志对象
    val localLog = localLogWithEpochOrException(currentLeaderEpoch, fetchOnlyFromLeader)

    // 记录当前日志偏移量，防止读取过程中发生append导致变化
    val initialHighWatermark = localLog.highWatermark
    val initialLogStartOffset = localLog.logStartOffset
    val initialLogEndOffset = localLog.logEndOffset
    val initialLastStableOffset = localLog.lastStableOffset
    
    
    lastFetchedEpoch.ifPresent { fetchEpoch =>
      ...// 副本同步 参数校验
    }
    //日志读取
    val fetchedData = localLog.read(fetchOffset, maxBytes, fetchIsolation, minOneMessage)
    //读取结果
    LogReadInfo( fetchedData = fetchedData, divergingEpoch = None, highWatermark = initialHighWatermark, logStartOffset = initialLogStartOffset, logEndOffset = initialLogEndOffset, lastStableOffset = initialLastStableOffset)
  }
```

### LocalLog#read

LocalLog是消息日志的抽象，每个LocalLog对象相应的也管理者一个或多个LogSegment，read()方法源码如下：

```
  def read(startOffset: Long,
           maxLength: Int,
           minOneMessage: Boolean,
           maxOffsetMetadata: LogOffsetMetadata,
           includeAbortedTxns: Boolean): FetchDataInfo = {
    maybeHandleIOException(s"Exception while reading from $topicPartition in dir ${dir.getParent}") {
      
      val endOffsetMetadata = nextOffsetMetadata
      val endOffset = endOffsetMetadata.messageOffset
      
      //获取比Fetch请求的起始offset小但偏差最近的LogSegment 
      var segmentOpt = segments.floorSegment(startOffset)

      // return error on attempt to read beyond the log end offset
      if (startOffset > endOffset || segmentOpt.isEmpty)
        throw new OffsetOutOfRangeException(...)
      //消息读取的起始offset验证
      if (startOffset == maxOffsetMetadata.messageOffset)
        emptyFetchDataInfo(maxOffsetMetadata, includeAbortedTxns)
      else if (startOffset > maxOffsetMetadata.messageOffset)
        emptyFetchDataInfo(convertToOffsetMetadataOrThrow(startOffset), includeAbortedTxns)
      else {
        // Do the read on the segment with a base offset less than the target offset
        // but if that segment doesn't contain any messages with an offset greater than that
        // continue to read from successive segments until we get some messages or we reach the end of the log
        var fetchDataInfo: FetchDataInfo = null
        while (fetchDataInfo == null && segmentOpt.isDefined) {
          val segment = segmentOpt.get
          val baseOffset = segment.baseOffset

          val maxPosition =
          // Use the max offset position if it is on this segment; otherwise, the segment size is the limit.
            if (maxOffsetMetadata.segmentBaseOffset == segment.baseOffset) maxOffsetMetadata.relativePositionInSegment
            else segment.size
          //数据读取
          fetchDataInfo = segment.read(startOffset, maxLength, maxPosition, minOneMessage)
          if (fetchDataInfo != null) {
            //成功读取到数据
            if (includeAbortedTxns)
              // READ_COMMMIT  读已提交，将退出的事务返回，由consumer自行过滤
              fetchDataInfo = addAbortedTransactions(startOffset, segment, fetchDataInfo)
          } else
              //未读取到数据，换下一段LogSegment 
              segmentOpt = segments.higherSegment(baseOffset)
        }

        if (fetchDataInfo != null) fetchDataInfo
        else {
          // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
          // this can happen when all messages with offset larger than start offsets have been deleted.
          // In this case, we will return the empty set with log end offset metadata
          FetchDataInfo(nextOffsetMetadata, MemoryRecords.EMPTY)
        }
      }
    }
  }

```
read()方法核心功能有两点：

* 根据Fetch请求的startOffset获取相应的LogSegment对象，确认offset有效后，调用LogSegment#read()方法读取消息；
* 若KafkaConsumer的事务隔离级别为READ_COMMIT，调用addAbortedTransactions()方法将读取消息范围内的中止的事务信息添加到读取结果中一起返回给consumer。

#### addAbortedTransactions

添加中止事务信息的源码实现如下：

```
  private def addAbortedTransactions(startOffset: Long, segment: LogSegment,
                                     fetchInfo: FetchDataInfo): FetchDataInfo = {
    //读取的字节数                                 
    val fetchSize = fetchInfo.records.sizeInBytes
    //读取的第一条消息的物理偏移量与相对偏移量
    val startOffsetPosition = OffsetPosition(fetchInfo.fetchOffsetMetadata.messageOffset, fetchInfo.fetchOffsetMetadata.relativePositionInSegment)
    //消息位移上限
    val upperBoundOffset =
      //比最后一条消息偏移量大的索引 
      segment.fetchUpperBoundOffset(startOffsetPosition, fetchSize).getOrElse {
      //下一段LogSegment的起始offset
      segments.higherSegment(segment.baseOffset).map(_.baseOffset).getOrElse(logEndOffset)
    }

    val abortedTransactions = ListBuffer.empty[FetchResponseData.AbortedTransaction]
    def accumulator(abortedTxns: List[AbortedTxn]): Unit = abortedTransactions ++= abortedTxns.map(_.asAbortedTransaction)
    //查找偏移量范围内的中止事务信息
    collectAbortedTransactions(startOffset, upperBoundOffset, segment, accumulator)

    FetchDataInfo(fetchOffsetMetadata = fetchInfo.fetchOffsetMetadata,
      records = fetchInfo.records,
      firstEntryIncomplete = fetchInfo.firstEntryIncomplete,
      abortedTransactions = Some(abortedTransactions.toList))
  }
```


#### TransactionIndex#collectAbortedTxns

中止事务信息的查找依赖于事务索引，索引存储在`offset.txnindex`形式的文件中。事务索引中维护关于对应LogSegment的中止事务的元数据，包括**中止事务的开始和结束偏移量以及中止时的最后一个LSO**。 
事务索引主要用于为在READ_COMMITTED隔离级别下的KafkaConsumer查找给定偏移量范围内的中止事务信息。

索引数据结构定义如下：

```
private[log] class AbortedTxn(val buffer: ByteBuffer) {
  import AbortedTxn._

  def this(producerId: Long,
           firstOffset: Long,
           lastOffset: Long,
           lastStableOffset: Long) = {
    this(ByteBuffer.allocate(AbortedTxn.TotalSize))
    buffer.putShort(CurrentVersion) //版本信息
    buffer.putLong(producerId) //生产者id
    buffer.putLong(firstOffset) //本次中止事务的第一条事务消息offset
    buffer.putLong(lastOffset)  // 本次中止事务的最后一条事务消息offset
    buffer.putLong(lastStableOffset)  //消息日志上一次的LSO
    buffer.flip()
  }

  def this(completedTxn: CompletedTxn, lastStableOffset: Long) =
    this(completedTxn.producerId, completedTxn.firstOffset, completedTxn.lastOffset, lastStableOffset)
```

查找方法如下：

```
  def collectAbortedTxns(fetchOffset: Long, upperBoundOffset: Long): TxnIndexSearchResult = {
    val abortedTransactions = ListBuffer.empty[AbortedTxn]
    //从头开始读取事务索引文件查找
    for ((abortedTxn, _) <- iterator()) {
      //偏移量比较
      if (abortedTxn.lastOffset >= fetchOffset && abortedTxn.firstOffset < upperBoundOffset)
        abortedTransactions += abortedTxn

      if (abortedTxn.lastStableOffset >= upperBoundOffset)
        return TxnIndexSearchResult(abortedTransactions.toList, isComplete = true)
    }
    TxnIndexSearchResult(abortedTransactions.toList, isComplete = false)
  }
```

### LogSegment#read

LogSegment#read()方法的主要功能是进行文件读取，主要分为两步：

* 完成消息偏移量到文件物理位置的转换查找，即`translateOffset()`方法；
* 日志文件的IO读取，即FileRecords#slice()方法。

```
  def read(startOffset: Long,
           maxSize: Int,
           maxPosition: Long = size,
           minOneMessage: Boolean = false): FetchDataInfo = {
    if (maxSize < 0)
      throw new IllegalArgumentException(s"Invalid max size $maxSize for log read from segment $log")
    //偏移量转为文件的物理位置  
    val startOffsetAndSize = translateOffset(startOffset)

    //未找到对应的信息
    if (startOffsetAndSize == null)
      return null
    
    val startPosition = startOffsetAndSize.position
    val offsetMetadata = LogOffsetMetadata(startOffset, this.baseOffset, startPosition)

    val adjustedMaxSize =
      if (minOneMessage) math.max(maxSize, startOffsetAndSize.size)
      else maxSize

    if (adjustedMaxSize == 0)
      return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY)

    // 计算要读取的物理长度
    val fetchSize: Int = min((maxPosition - startPosition).toInt, adjustedMaxSize)

    FetchDataInfo(offsetMetadata, 
                  log.slice(startPosition, fetchSize), //读取文件
                  firstEntryIncomplete = adjustedMaxSize < startOffsetAndSize.size)
  }
```

#### LogSegment#translateOffset

因为消息日志的偏移量索引是**稀疏索引**，所以根据offset获取相应的物理文件地址分为两步：

* 查找偏移量索引文件，获取小于等于指定offset的最大索引；
* 根据索引的指示的物理位置依次读取日志文件中的消息，直至找到指定offset的物理位置。

```
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): LogOffsetPosition = {
    //查找偏移量索引文件
    val mapping = offsetIndex.lookup(offset)
    //从索引位置
    log.searchForOffsetWithSize(offset, max(mapping.position, startingFilePosition))
  }
```

##### OffsetIndex#lookup

Broker将日志偏移量索引文件映射到内存中进行二分查找，并读取出该位置索引的内容。

```
  def lookup(targetOffset: Long): OffsetPosition = {
    maybeLock(lock) {
      内存映射 复制 防止变化
      val idx = mmap.duplicate
      //二分法查找
      val slot = largestLowerBoundSlotFor(idx, targetOffset, IndexSearchType.KEY)
      if(slot == -1)
        OffsetPosition(baseOffset, 0)
      else
        //从内存映射中读取结果 相对偏移量 及 物理位置
        parseEntry(idx, slot)
    }
  }
  //每一个索引项的大小
  override def entrySize = 8
 
  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize)
  
  private def physical(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize + 4)

  override protected def parseEntry(buffer: ByteBuffer, n: Int): OffsetPosition = {
    OffsetPosition(baseOffset + relativeOffset(buffer, n), physical(buffer, n))
  }

```

偏移量索引的基本格式<relativeOffset(Int 4B) ，position(Int 4B)>，所以此处的计算公式为：

* relativeOffset = buffer.getInt(n * 8)
* position = buffer.getInt(n * 8 + 4) 



##### FileRecords#searchForOffsetWithSize

searchForOffsetWithSize()方法将**通过偏移量索引指向的物理位置向后遍历查找**，直至找到targetOffset的准确物理位置信息。

```
public LogOffsetPosition searchForOffsetWithSize(long targetOffset, int startingPosition) {
    for (FileChannelRecordBatch batch : batchesFrom(startingPosition)) {
        //从索引指示的物理位置开始读
        long offset = batch.lastOffset();
        if (offset >= targetOffset)
            //获取到符合的位置返回
            return new LogOffsetPosition(offset, batch.position(), batch.sizeInBytes());
    }
    return null;
}
```



#### FileRecords#slice

文件IO实现如下：

```
public FileRecords slice(int position, int size) throws IOException {
    //可读字节数
    int availableBytes = availableBytes(position, size);
    //读取起始位置
    int startPosition = this.start + position;
    //IO读取
    return new FileRecords(file, channel, startPosition, startPosition + availableBytes, true);
}

```


## RequestFutureListener

FetchRequest请求发送时注册的Listener，会在获取到响应时触发回调，主要将响应数据放入`completedFetches`中以及从`nodesWithPendingFetchRequests`将Broker节点移除，以便可以进行
下次请求的发送。

```
future.addListener(new RequestFutureListener<ClientResponse>() {
    @Override
   public void onSuccess(ClientResponse resp) {
       synchronized (Fetcher.this) {
           try {
               FetchResponse response = (FetchResponse) resp.responseBody();
               FetchSessionHandler handler = sessionHandler(fetchTarget.id());
               if (handler == null) {
                   log.error("Unable to find FetchSessionHandler for node {}. Ignoring fetch response.",fetchTarget.id());
                   return;
               }
               if (!handler.handleResponse(response, resp.requestHeader().apiVersion())) {
                   if (response.error() == Errors.FETCH_SESSION_TOPIC_ID_ERROR || response.error() == Errors.UNKNOWN_TOPIC_ID || response.error() == Errors.INCONSISTENT_TOPIC_ID) {
                       //元数据更新
                       metadata.requestUpdate();
                   }
                   return;
               }
               Map<TopicPartition, FetchResponseData.PartitionData> responseData = response.responseData(data.topicNames(), resp.requestHeader().apiVersion());
               Set<TopicPartition> partitions = new HashSet<>(responseData.keySet());
               FetchResponseMetricAggregator metricAggregator = new FetchResponseMetricAggregator(sensors, partitions);

               for (Map.Entry<TopicPartition, FetchResponseData.PartitionData> entry : responseData.entrySet()) {
                   TopicPartition partition = entry.getKey();
                   FetchRequest.PartitionData requestData = data.sessionPartitions().get(partition);
                   if (requestData == null) {
                       ... 
                       throw new IllegalStateException(message);
                   } else {
                       long fetchOffset = requestData.fetchOffset;
                       FetchResponseData.PartitionData partitionData = entry.getValue();
                       Iterator<? extends RecordBatch> batches = FetchResponse.recordsOrFail(partitionData).batches().iterator();
                       short responseVersion = resp.requestHeader().apiVersion();
                       //响应数据放入completedFetches
                       completedFetches.add(new CompletedFetch(partition, partitionData, metricAggregator, batches, fetchOffset, responseVersion));
                   }
               }
               sensors.fetchLatency.record(resp.requestLatencyMs());
           } finally {
                //移除Node请求发送表示，可以进行下一次请求
               nodesWithPendingFetchRequests.remove(fetchTarget.id());
           }
       }
   }

   @Override
   public void onFailure(RuntimeException e) {
       synchronized (Fetcher.this) {
           try {
               //异常处理
               FetchSessionHandler handler = sessionHandler(fetchTarget.id());
               if (handler != null) {
                   handler.handleError(e);
               }
           } finally {
               //移除Node请求发送表示，可以进行下一次请求
               nodesWithPendingFetchRequests.remove(fetchTarget.id());
           }
       }
   }
});

```

# fetchedRecords

上一步的sendFetches方法中会把成功的结果放在sendFetches这个completedFetches集合中，fetchedRecords方法主要有两部分作用：

* 将缓存在completedFetches中的数据进一步验证处理返回给consumer进行消费，已暂停消费的分区(如使用pause(Collection<TopicPartition> partitions)的分区)不会返回；
* 更新TopicPartitionState中的offset信息，准备下一次拉取。

```
public Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords() {
    //结果集
    Map<TopicPartition, List<ConsumerRecord<K, V>>> fetched = new HashMap<>();
    Queue<CompletedFetch> pausedCompletedFetches = new ArrayDeque<>();
    int recordsRemaining = maxPollRecords;
    try {
        //可拉取数为0 退出
        while (recordsRemaining > 0) {
            if (nextInLineFetch == null || nextInLineFetch.isConsumed) {
                //从completedFetches获取一个响应数据
                CompletedFetch records = completedFetches.peek();
                //缓存中所有的拉取响应已处理完
                if (records == null) break;
                if (records.notInitialized()) {
                    try {
                        nextInLineFetch = initializeCompletedFetch(records);  //①
                    } catch (Exception e) {
                        FetchResponseData.PartitionData partition = records.partitionData;
                        //异常 移除该CompletedFetch
                        if (fetched.isEmpty() && FetchResponse.recordsOrFail(partition).sizeInBytes() == 0) { completedFetches.poll(); }
                        throw e;
                    }
                } else {
                    nextInLineFetch = records;
                }
                completedFetches.poll();
            } else if (subscriptions.isPaused(nextInLineFetch.partition)) {
                //已暂停分区消费
                pausedCompletedFetches.add(nextInLineFetch);
                nextInLineFetch = null;
            } else {
                //完成数据
                List<ConsumerRecord<K, V>> records = fetchRecords(nextInLineFetch, recordsRemaining); //②
                if (!records.isEmpty()) {
                    //数据填充
                    TopicPartition partition = nextInLineFetch.partition;
                    List<ConsumerRecord<K, V>> currentRecords = fetched.get(partition);
                    if (currentRecords == null) {
                        fetched.put(partition, records);
                    } else {
                        List<ConsumerRecord<K, V>> newRecords = new ArrayList<>(records.size() + currentRecords.size());
                        newRecords.addAll(currentRecords);
                        newRecords.addAll(records);
                        fetched.put(partition, newRecords);
                    }
                    //更新剩余可拉取消息数
                    recordsRemaining -= records.size();
                }
            }
        }
    } catch (KafkaException e) {
        if (fetched.isEmpty()) throw e;
    } finally {
        completedFetches.addAll(pausedCompletedFetches);
    }
    return fetched;
}
```

可以看到整个方法分为两步：initializeCompletedFetch()及fetchRecords()。


## CompletedFetch

通过FetchRequest请求获取的数据封装为CompletedFetch存储在KafkaConsumer端，其结构如下：

```
private class CompletedFetch {
        private final TopicPartition partition; //主题分区
        private final Iterator<? extends RecordBatch> batches;  //消息
        private final Set<Long> abortedProducerIds; //中止事务的producerId
        private final PriorityQueue<FetchResponse.AbortedTransaction> abortedTransactions; //中止的事务id
        private final FetchResponse.PartitionData<Records> partitionData;  //响应数据
        private final FetchResponseMetricAggregator metricAggregator;
        private final short responseVersion;

        private int recordsRead;  //已读取的行数,
        private int bytesRead;  //已读取的字节数
        private RecordBatch currentBatch; //正在读取的RecordBatch
        private Record lastRecord; 
        private CloseableIterator<Record> records;
        private long nextFetchOffset;
        private Optional<Integer> lastEpoch;
        private boolean isConsumed = false;
        private Exception cachedRecordException = null;
        private boolean corruptLastRecord = false;
        private boolean initialized = false; 
        
        ...// 成员方法
}

```

## initializeCompletedFetch

initializeCompletedFetch方法主要是确保响应数据有效，并更新本地消息SubscriptionState.TopicPartitionState数据。

```
private CompletedFetch initializeCompletedFetch(CompletedFetch nextCompletedFetch) {
    TopicPartition tp = nextCompletedFetch.partition;
    FetchResponseData.PartitionData partition = nextCompletedFetch.partitionData;
    long fetchOffset = nextCompletedFetch.nextFetchOffset;
    CompletedFetch completedFetch = null;
    Errors error = Errors.forCode(partition.errorCode());

    try {
        //分区验证
        if (!subscriptions.hasValidPosition(tp)) {
        } else if (error == Errors.NONE) {
            //位移验证
            FetchPosition position = subscriptions.position(tp);
            if (position == null || position.offset != fetchOffset) { return null;}

            Iterator<? extends RecordBatch> batches = FetchResponse.recordsOrFail(partition).batches().iterator();
            completedFetch = nextCompletedFetch;
            if (!batches.hasNext() && FetchResponse.recordsSize(partition) > 0) {
                ... //拉取失败 抛出异常
            }
            //更新SubscriptionState.TopicPartitionState数据
            if (partition.highWatermark() >= 0) {
                subscriptions.updateHighWatermark(tp, partition.highWatermark());
            }
            if (partition.logStartOffset() >= 0) {
                subscriptions.updateLogStartOffset(tp, partition.logStartOffset());
            }
            if (partition.lastStableOffset() >= 0) {
                subscriptions.updateLastStableOffset(tp, partition.lastStableOffset());
            }
            if (FetchResponse.isPreferredReplica(partition)) {
                subscriptions.updatePreferredReadReplica(completedFetch.partition, partition.preferredReadReplica(), () -> {
                    long expireTimeMs = time.milliseconds() + metadata.metadataExpireMs();
                    return expireTimeMs;
                });
            }
            nextCompletedFetch.initialized = true;
        } else if (...) {
            // 异常处理
        } 
    } finally {
        if (completedFetch == null)
            nextCompletedFetch.metricAggregator.record(tp, 0, 0);
        if (error != Errors.NONE)
            subscriptions.movePartitionToEnd(tp);
    }
    return completedFetch;
}

```

## fetchRecords

fetchRecords中完成消息的反序列化及本地消费offset(TopicPartitionState#position)的更新，并将消息返回。

```
private List<ConsumerRecord<K, V>> fetchRecords(CompletedFetch completedFetch, int maxRecords) {
    if (!subscriptions.isAssigned(completedFetch.partition)) {
        //再次验证分区，防止rebalance发生
    } else if (!subscriptions.isFetchable(completedFetch.partition)) {
        //再次判断分区是否被暂停消费
    } else {
        FetchPosition position = subscriptions.position(completedFetch.partition);
        if (position == null) {
            throw new IllegalStateException("Missing position for fetchable partition " + completedFetch.partition);
        }
        if (completedFetch.nextFetchOffset == position.offset) {
            //位移验证正确，拉取消息 此处完成消息序列化
            List<ConsumerRecord<K, V>> partRecords = completedFetch.fetchRecords(maxRecords);
            if (completedFetch.nextFetchOffset > position.offset) {
                FetchPosition nextPosition = new FetchPosition(completedFetch.nextFetchOffset,completedFetch.lastEpoch,position.currentLeader);
                //更新本场消费位移
                subscriptions.position(completedFetch.partition, nextPosition);
            }
            Long partitionLag = subscriptions.partitionLag(completedFetch.partition, isolationLevel);
            if (partitionLag != null)
                this.sensors.recordPartitionLag(completedFetch.partition, partitionLag);
            Long lead = subscriptions.partitionLead(completedFetch.partition);
            if (lead != null) {
                this.sensors.recordPartitionLead(completedFetch.partition, lead);
            }
            //返回消息
            return partRecords;
        } else { //消费位移不正确 }
    }
    completedFetch.drain();
    return emptyList();
}
```
