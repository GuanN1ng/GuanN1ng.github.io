---
layout: post
title:  Kafka 消息日志读取-HandleFetchRequest
date:   2021-11-30 23:25:10
categories: Kafka
---
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

因为消息日志的偏移量索引是**稀疏索引**，所以根据offset获取相应的物理文件位置分为两步：

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

偏移量索引存储的格式<relativeOffset(Int 4B) ，position(Int 4B)>，所以此处的计算公式为：

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