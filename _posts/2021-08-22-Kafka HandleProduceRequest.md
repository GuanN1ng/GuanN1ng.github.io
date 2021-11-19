---
layout: post
title:  "Kafka HandleProduceRequest"
date:   2021-08-22 14:25:10
categories: Kafka
---

[Kafka Producer 消息发送](https://guann1ng.github.io/kafka/2021/08/16/Kafka-Producer-%E6%B6%88%E6%81%AF%E5%8F%91%E9%80%81/)分析了KafkaProducer如何将消息发送至Broker，本篇内容将继续分析Kafka Broker端是如何处理ProducerRequest。

## KafkaApis

Broker端由KafkaApis对所有请求进行分发处理的，入口方法为`kafka.server.KafkaApis#handle`：

```
override def handle(request: RequestChannel.Request): Unit = {
    try {
      if (!apiVersionManager.isApiEnabled(request.header.apiKey)) {
        throw new IllegalStateException(s"API ${request.header.apiKey} is not enabled")
      }
      request.header.apiKey match {
        //生产消息请求
        case ApiKeys.PRODUCE => handleProduceRequest(request)
        //拉取消息请求
        case ApiKeys.FETCH => handleFetchRequest(request)
        case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
        ...
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable => requestHelper.handleError(request, e)
    } finally {
      replicaManager.tryCompleteActions()
      if (request.apiLocalCompleteTimeNanos < 0)
        request.apiLocalCompleteTimeNanos = time.nanoseconds
    }
  }
```

KafkaApis#hanlde方法会验证请求的ApiKey是否有效，然后根据ApiKey指定具体的Handler进行消息处理，负责处理ProducerRequest是`handleProduceRequest()`方法。

### handleProduceRequest

handleProduceRequest()方法源码如下：

```
  def handleProduceRequest(request: RequestChannel.Request): Unit = {
    val produceRequest = request.body[ProduceRequest]
    val requestSize = request.sizeInBytes

    if (RequestUtils.hasTransactionalRecords(produceRequest)) {
      ... //事务检验
    }
    val unauthorizedTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val nonExistingTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val invalidRequestResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val authorizedRequestInfo = mutable.Map[TopicPartition, MemoryRecords]()

    val authorizedTopics = authHelper.filterByAuthorized(request.context, WRITE, TOPIC, produceRequest.data().topicData().asScala)(_.name())
    //消息校验
    produceRequest.data.topicData.forEach(topic => topic.partitionData.forEach { partition =>
      val topicPartition = new TopicPartition(topic.name, partition.index)
      val memoryRecords = partition.records.asInstanceOf[MemoryRecords]
      if (!authorizedTopics.contains(topicPartition.topic))
        //权限认证校验 未通过返回Errors.TOPIC_AUTHORIZATION_FAILED
        unauthorizedTopicResponses += topicPartition -> new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED)
      else if (!metadataCache.contains(topicPartition))
        //主题分区元数据校验，未通过返回 Errors.UNKNOWN_TOPIC_OR_PARTITION
        nonExistingTopicResponses += topicPartition -> new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION)
      else
        try {
          //消息校验
          ProduceRequest.validateRecords(request.header.apiVersion, memoryRecords)
          //有效消息集合
          authorizedRequestInfo += (topicPartition -> memoryRecords)
        } catch {
          case e: ApiException =>
            invalidRequestResponses += topicPartition -> new PartitionResponse(Errors.forException(e))
        }
    })

    //回调处理，消息完成写入后，通过sendResponseCallback方法发送produceResponse
    @nowarn("cat=deprecation")
    def sendResponseCallback(responseStatus: Map[TopicPartition, PartitionResponse]): Unit = { ... }


    if (authorizedRequestInfo.isEmpty)
      //有效消息为空
      sendResponseCallback(Map.empty)
    else {
      val internalTopicsAllowed = request.header.clientId == AdminUtils.AdminClientId
      //调用副本管理器追加消息  
      replicaManager.appendRecords(
        timeout = produceRequest.timeout.toLong,
        requiredAcks = produceRequest.acks,
        internalTopicsAllowed = internalTopicsAllowed,
        origin = AppendOrigin.Client,
        entriesPerPartition = authorizedRequestInfo,
        responseCallback = sendResponseCallback,
        recordConversionStatsCallback = processingStatsCallback)
       
      produceRequest.clearPartitionRecords()
    }
  }
```

handleProduceRequest方法中主要做了三件工作：

* 1、完成消息请求校验，包括权限、TopicPartition以及Records内容格式校验。
* 2、定义回调函数sendResponseCallback，用于返回响应。
* 3、调用`ReplicaManager#appendRecords()`，完成消息写入。


## ReplicaManager

ReplicaManager的主要功能是对分区副本进行管理，包含消息日志的读取和写入、副本同步及ISR维护等。

### appendRecords

KafkaApis#handleProduceRequest()中调用ReplicaManager#appendRecords()处理消息写入，源码实现如下：

```
  def appendRecords(timeout: Long,requiredAcks: Short,internalTopicsAllowed: Boolean,origin: AppendOrigin,entriesPerPartition: Map[TopicPartition, MemoryRecords],responseCallback: Map[TopicPartition, PartitionResponse] => Unit,delayedProduceLock: Option[Lock] = None,recordConversionStatsCallback: Map[TopicPartition, RecordConversionStats] => Unit = _ => ()): Unit = {
    //校验 ack设置是否有效 0,1,-1(Producer端all会被转为-1)
    if (isValidRequiredAcks(requiredAcks)) {
      val sTime = time.milliseconds
      //将消息追加入日志
      val localProduceResults = appendToLocalLog(internalTopicsAllowed = internalTopicsAllowed, origin, entriesPerPartition, requiredAcks)
      
      //TopicPartition的消息追加结果
      val produceStatus = localProduceResults.map { case (topicPartition, result) =>
        topicPartition -> ProducePartitionStatus(
          result.info.lastOffset + 1, // required offset
          new PartitionResponse(result.error,result.info.firstOffset.map(_.messageOffset).getOrElse(-1),result.info.logAppendTime,result.info.logStartOffset,result.info.recordErrors.asJava,result.info.errorMessage)
        ) // response status
      }
      ...
      recordConversionStatsCallback(localProduceResults.map { case (k, v) => k -> v.info.recordConversionStats })

      if (delayedProduceRequestRequired(requiredAcks, entriesPerPartition, localProduceResults)) {
        // acks = -1  需要所有ISR副本完成同步才能返回
        val produceMetadata = ProduceMetadata(requiredAcks, produceStatus)
        // 构建producer处理及响应的延时任务
        val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback, delayedProduceLock)
        //所涉及的所有TopicPartition集合
        val producerRequestKeys = entriesPerPartition.keys.map(TopicPartitionOperationKey(_)).toSeq
        //保存未完成ISR同步的Produce请求，后续由单独线程异步处理，尽快处理下一批请求
        delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)
      } else {
        //acks = 1 leader副本完成写入即返回
        val produceResponseStatus = produceStatus.map { case (k, status) => k -> status.responseStatus }
        // handleProduceRequest中定义的回调函数
        responseCallback(produceResponseStatus)
      }
    } else {
      //acks参数设置无效 异常返回 Errors.INVALID_REQUIRED_ACKS
      val responseStatus = entriesPerPartition.map { case (topicPartition, _) =>
        topicPartition -> new PartitionResponse(Errors.INVALID_REQUIRED_ACKS,LogAppendInfo.UnknownLogAppendInfo.firstOffset.map(_.messageOffset).getOrElse(-1), RecordBatch.NO_TIMESTAMP, LogAppendInfo.UnknownLogAppendInfo.logStartOffset)
      }
      // handleProduceRequest中定义的回调函数
      responseCallback(responseStatus)
    }
  }
```

appendRecords()方法中更多的是对消息日志追加的结果处理，如`acks=1`，leader副本写入后直接调用handleProduceRequest中定义的回调函数返回响应，`acks=-1`，需要当前TopicPartition的ISR副本集合均完成
写入，才可返回，为了防止线程阻塞在ISR同步过程判断中，这里通过构建`DelayedProduce`由单独线程统一异步处理(后续介绍Broker端定时任务设计时再详细介绍)。日志的追加则是在`appendToLocalLog`中完成。


### appendToLocalLog

appendToLocalLog()对主题及分区作了进一步验证。如禁止KafkaProducer向内部主题(_consumer_offset和 _transaction_state)写消息，以及Partition对象的获取。消息的写入仍然通过继续向下调用
`Partition.appendRecordsToLeader()`完成。

```
  private def appendToLocalLog(internalTopicsAllowed: Boolean,origin: AppendOrigin,entriesPerPartition: Map[TopicPartition, MemoryRecords],requiredAcks: Short): Map[TopicPartition, LogAppendResult] = {
    ...
    //遍历集合 追加消息
    entriesPerPartition.map { case (topicPartition, records) =>
      if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
        //内部topic,且没有权限，返回失败 内部topic: _consuemr_offset  _transaction_state
        (topicPartition, LogAppendResult(
          LogAppendInfo.UnknownLogAppendInfo,
          Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}"))))
      } else {
        try {
          //获取分区
          val partition = getPartitionOrException(topicPartition)
          //写入消息
          val info = partition.appendRecordsToLeader(records, origin, requiredAcks)
          val numAppendedMessages = info.numMessages
          ...//metrics更新
          //返回结果
          (topicPartition, LogAppendResult(info))
        } catch {
          ...//异常分类处理
        }
      }
    }
  }
```

## Partition

Partition是主题分区副本的抽象数据结构，如果为leader副本，那么该对象将负责AR, ISR等数据的维护。

### appendRecordsToLeader

appendRecordsToLeader()方法的内容可分为以下几部分：

* 1、判断当前Partition对象是否是leader副本，不是则抛出NotLeaderOrFollowerException异常；
* 2、`acks=-1`时，判断ISR集合中的副本数是否满足`min.insync.replicas`配置的要求，不满足则抛出NotEnoughReplicasException异常；
* 3、调用Log#appendAsLeader()方法写入消息，并判读是否更新Log的可读位置(HighWaterMark)

```
  def appendRecordsToLeader(records: MemoryRecords, origin: AppendOrigin, requiredAcks: Int): LogAppendInfo = {
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      leaderLogIfLocal match { //获取对应的leader log
        case Some(leaderLog) =>
          //  min.insync.replicas配置 最小同步副本数量
          val minIsr = leaderLog.config.minInSyncReplicas 
          val inSyncSize = isrState.isr.size //isr数量
          //acks=-1时，若此时ISR副本数低于配置值，抛出异常
          if (inSyncSize < minIsr && requiredAcks == -1) {
            throw new NotEnoughReplicasException(s"The size of the current ISR ${isrState.isr} " +s"is insufficient to satisfy the min.isr requirement of $minIsr for partition $topicPartition")
          }
          //向副本对应的Log写入消息
          val info = leaderLog.appendAsLeader(records, leaderEpoch = this.leaderEpoch, origin,interBrokerProtocolVersion)
          //判断是否增加Leader的可读部分(HighWater)
          (info, maybeIncrementLeaderHW(leaderLog))

        case None =>
          //leader副本不在本Broker实例
          throw new NotLeaderOrFollowerException("Leader not local for partition %s on broker %d".format(topicPartition, localBrokerId))
      }
    }
    info.copy(leaderHwChange = if (leaderHWIncremented) LeaderHwChange.Increased else LeaderHwChange.Same)
  }
```

## Log

Log是消息日志的抽象，[Kafka 简介](https://guann1ng.github.io/kafka/2021/07/29/Kafka-%E7%AE%80%E4%BB%8B/)中介绍了实际上的日志是以一段一段存在的，即LogSegment，Log对象中负责维护当前分区副本的所有LogSegment集合，属性如下：

```
//key 为每段LogSegment起始偏移量  value为对应的LogSegment
//SkipListMap  可根据offset快速定位对应的LogSegment
private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]
//活跃的日志分段，即当前负责写的segments，为最后一个entry的value
def activeSegment = segments.lastEntry.getValue
```

LogSegment是根据可配置的策略创建的，配置如下：

* log.segment.bytes，设置LogSegment文件的大小，超出则创建新的；
* log.segment.ms，每隔`log.segment.ms`创建一个新的LogSegment，即使未达到log.segment.bytes依然创建。


### append

日志的追加写入是通过append()方法实现，相关方法源码如下(代码有省略)：

```
  def appendAsLeader(records: MemoryRecords,leaderEpoch: Int,origin: AppendOrigin = AppendOrigin.Client,interBrokerProtocolVersion: ApiVersion = ApiVersion.latestVersion): LogAppendInfo = {
    val validateAndAssignOffsets = origin != AppendOrigin.RaftLeader
    //日志追加
    append(records, origin, interBrokerProtocolVersion, validateAndAssignOffsets, leaderEpoch, ignoreRecordSize = false)
  }

  private def append(records: MemoryRecords,origin: AppendOrigin,interBrokerProtocolVersion: ApiVersion,validateAndAssignOffsets: Boolean,leaderEpoch: Int,ignoreRecordSize: Boolean): LogAppendInfo = {
    //确保分区元数据文件写入完成
    maybeFlushMetadataFile()
    //对消息进行校验，如CRC，消息大小等。并返回消息概要 Number of messages Number of valid bytes等信息
    val appendInfo = analyzeAndValidateRecords(records, origin, ignoreRecordSize, leaderEpoch)

    // return if we have no valid messages or if this is a duplicate of the last appended entry
    if (appendInfo.shallowCount == 0) appendInfo
    else {
      //去除无效消息
      var validRecords = trimInvalidBytes(records, appendInfo)

      // they are valid, insert them in the log
      lock synchronized {
        maybeHandleIOException(s"Error while appending records to $topicPartition in dir ${dir.getParent}") {
          checkIfMemoryMappedBufferClosed()

          if (validateAndAssignOffsets) {
              //获取消息集的起始offset
              val offset = new LongRef(nextOffsetMetadata.messageOffset)
              appendInfo.firstOffset = Some(LogOffsetMetadata(offset.value))
              val now = time.milliseconds
              val validateAndOffsetAssignResult = try {
                //消息校验及为消息分配偏移量
                LogValidator.validateMessagesAndAssignOffsets(...)
              } catch {
                case e: IOException =>
                  throw new KafkaException(s"Error validating messages while appending to log $name", e)
              }
              //更新appendInfo
              validRecords = validateAndOffsetAssignResult.validatedRecords
              appendInfo.maxTimestamp = validateAndOffsetAssignResult.maxTimestamp
              appendInfo.offsetOfMaxTimestamp = validateAndOffsetAssignResult.shallowOffsetOfMaxTimestamp
              appendInfo.lastOffset = offset.value - 1 //最后一条消息的 offset
              appendInfo.recordConversionStats = validateAndOffsetAssignResult.recordConversionStats
              if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME)
                appendInfo.logAppendTime = now
  
              if (!ignoreRecordSize && validateAndOffsetAssignResult.messageSizeMaybeChanged) {
                validRecords.batches.forEach { batch =>
                  //maxMessageSize配置判断
                  if (batch.sizeInBytes > config.maxMessageSize) {
                    throw new RecordTooLargeException(s"Message batch size is ${batch.sizeInBytes} bytes in append to" +s"partition $topicPartition which exceeds the maximum configured size of ${config.maxMessageSize}.")
                  }
                }
              }
            } else {...}

          //消息大小大于LogSegment的最大长度，抛出异常
          if (validRecords.sizeInBytes > config.segmentSize) {
            throw new RecordBatchTooLargeException(s"Message batch size is ${validRecords.sizeInBytes} bytes in append " +s"to partition $topicPartition, which exceeds the maximum configured segment size of ${config.segmentSize}.")
          }

          //判断是否新建LogSegment
          val segment = maybeRoll(validRecords.sizeInBytes, appendInfo)
          //日志偏移量元数据
          val logOffsetMetadata = LogOffsetMetadata(
            messageOffset = appendInfo.firstOrLastOffsetOfFirstBatch,
            segmentBaseOffset = segment.baseOffset,
            relativePositionInSegment = segment.size)
          
          //验证幂等及事务  
          val (updatedProducers, completedTxns, maybeDuplicate) = analyzeAndValidateProducerState(
            logOffsetMetadata, validRecords, origin)

          maybeDuplicate match {
            case Some(duplicate) =>
               //开启幂等或事务时，重复消息不处理
              appendInfo.firstOffset = Some(LogOffsetMetadata(duplicate.firstOffset))
              appendInfo.lastOffset = duplicate.lastOffset
              appendInfo.logAppendTime = duplicate.timestamp
              appendInfo.logStartOffset = logStartOffset
            case None =>
              appendInfo.firstOffset = appendInfo.firstOffset.map { offsetMetadata =>
                offsetMetadata.copy(segmentBaseOffset = segment.baseOffset, relativePositionInSegment = segment.size)
              }
              //调用segment.append写入
              segment.append(largestOffset = appendInfo.lastOffset,largestTimestamp = appendInfo.maxTimestamp,shallowOffsetOfMaxTimestamp = appendInfo.offsetOfMaxTimestamp,records = validRecords)
              //事务消息处理
              completedTxns.foreach { completedTxn =>
                val lastStableOffset = producerStateManager.lastStableOffset(completedTxn)
                segment.updateTxnIndex(completedTxn, lastStableOffset)
                producerStateManager.completeTxn(completedTxn)
              }
              producerStateManager.updateMapEndOffset(appendInfo.lastOffset + 1)
               / update the first unstable offset (which is used to compute LSO)
              maybeIncrementFirstUnstableOffset()
              //是否到达刷盘配置log.flush.interval.messages消息数
              if (unflushedMessages >= config.flushInterval) flush()
          }
          appendInfo
        }
      }
    }
  }
```

`append()`方法的主要内容可分为以下几点：

* `analyzeAndValidateRecords()`，主要是检查消息大小及CRC校验，返回appendInfo；
* `LogValidator.validateMessagesAndAssignOffsets()`，为每条消息设置偏移量；
* `maybeRoll()`，获取activeSegment，若需要新建segment，则新建并返回;
* `analyzeAndValidateProducerState()`，幂等及事务判断，如重复消息；
* `LogSegment#append()`,向segment中追加消息；
* `flush()`，若距离上一次文件刷盘后的写入消息数已到达配置`log.flush.interval.messages`，调用flush()完成刷盘。

这里主要关注日志写入相关的实现：`maybeRoll()`和`LogSegment#append()`方法。

### maybeRoll

LogSegment的获取是通过`maybeRoll()`方法，这里涉及到segment的新建，源码如下：

```
  private def maybeRoll(messagesSize: Int, appendInfo: LogAppendInfo): LogSegment = {
    //当前活跃日志分段
    val segment = activeSegment
    val now = time.milliseconds

    val maxTimestampInMessages = appendInfo.maxTimestamp
    val maxOffsetInMessages = appendInfo.lastOffset
    //是否需要新建日志分段
    if (segment.shouldRoll(RollParams(config, appendInfo, messagesSize, now))) {
      val rollOffset = appendInfo
        .firstOffset
        .map(_.messageOffset)
        .getOrElse(maxOffsetInMessages - Integer.MAX_VALUE)
      //新建日志分段
      roll(Some(rollOffset))
    } else {
      //不需新建，直接返回
      segment
    }
  }

```

#### LogSegment#shouldRoll

是否需要创建新的LogSegment的条件判断在方法`shouldRoll()`中，源码如下：

```
  def shouldRoll(rollParams: RollParams): Boolean = {
    //距离上次创建日志分段的时间是否达到了设置的阈值（log.roll.hours or log.roll.ms）
    val reachedRollMs = timeWaitedForRoll(rollParams.now, rollParams.maxTimestampInMessages) > rollParams.maxSegmentMs - rollJitterMs
    size > rollParams.maxSegmentBytes - rollParams.messagesSize ||
      (size > 0 && reachedRollMs) ||
      offsetIndex.isFull || timeIndex.isFull || !canConvertToRelativeOffset(rollParams.maxOffsetInMessages)
  }
``` 

具体条件为(以下给出的配置均为Broker端的默认配置项，另有Topic粒度的配置项，可[自行查找](https://kafka.apache.org/081/documentation.html#topic-config))：

* 1、`size > rollParams.maxSegmentBytes - rollParams.messagesSize`，待写入的消息大小加上segment的当前大小超过了log.segment.bytes配置值；
* 2、`size > 0 && reachedRollMs`，当前segment不为空且已到达segment定时切换的时间配置log.roll.hours或log.roll.ms(优先级高)；
* 3、`offsetIndex.isFull`，偏移量索引文件已写满(默认log.index.size.max.bytes)；
* 4、`timeIndex.isFull`，时间戳索引文件已写满(默认log.index.size.max.bytes)；
* 5、`!canConvertToRelativeOffset(rollParams.maxOffsetInMessages)`，当前segment的相对偏移量超过了Int的阈值

以上5个条件满足任意一个，则会调用`roll()`方法创建新的LogSegment。

#### roll

roll()源码如下：

```
  def roll(expectedNextOffset: Option[Long] = None): LogSegment = {
    maybeHandleIOException(s"Error while rolling log segment for $topicPartition in dir ${dir.getParent}") {
      val start = time.hiResClockMs()
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        //新LogSegment的基准偏移量  
        val newOffset = math.max(expectedNextOffset.getOrElse(0L), logEndOffset)
        //新建磁盘文件 baseOffset.log
        val logFile = Log.logFile(dir, newOffset)
        
        //是否已有此偏移量的日志文件
        if (segments.containsKey(newOffset)) {
          //segment with the same base offset already exists and loaded
          if (activeSegment.baseOffset == newOffset && activeSegment.size == 0) {
            // We have seen this happen (see KAFKA-6388) after shouldRoll() returns true for an active segment of size zero because of one of the indexes is "full" (due to _maxEntries == 0).
            removeAndDeleteSegments(Seq(activeSegment), asyncDelete = true, LogRoll)
          } else {
            //抛出已存在异常
            throw new KafkaException(s"Trying to roll a new log segment for topic partition $topicPartition with start offset $newOffset" +s" =max(provided offset = $expectedNextOffset, LEO = $logEndOffset) while it already exists. Existing " +s"segment is ${segments.get(newOffset)}.")
          }
        } else if (!segments.isEmpty && newOffset < activeSegment.baseOffset) {
          //存在大于该偏移量的文件
          throw new KafkaException(s"Trying to roll a new log segment for topic partition $topicPartition with " +s"start offset $newOffset =max(provided offset = $expectedNextOffset, LEO = $logEndOffset) lower than start offset of the active segment $activeSegment")
        } else {
          
          //创建对应的偏移量索引文件
          val offsetIdxFile = offsetIndexFile(dir, newOffset)
          //时间戳索引
          val timeIdxFile = timeIndexFile(dir, newOffset)
          //事务索引 如果Producer有事务
          val txnIdxFile = transactionIndexFile(dir, newOffset)

          for (file <- List(logFile, offsetIdxFile, timeIdxFile, txnIdxFile) if file.exists) {
            //删除同名文件
            Files.delete(file.toPath)
          }
          //将上一个LogSegment结束操作，如添加最后的时间索引，并trim 日志文件和偏移量索引
          Option(segments.lastEntry).foreach(_.getValue.onBecomeInactiveSegment())
        }

        producerStateManager.updateMapEndOffset(newOffset)
        producerStateManager.takeSnapshot()
        //创建LogSegment对象
        val segment = LogSegment.open(dir,
          baseOffset = newOffset,
          config,
          time = time,
          initFileSize = initFileSize,
          preallocate = config.preallocate)

        //添加到集合中
        addSegment(segment)

        updateLogEndOffset(nextOffsetMetadata.messageOffset)
        //定时刷新任务
        scheduler.schedule("flush-log", () => flush(newOffset), delay = 0L)

        segment
      }
    }
  }
```

roll()方法完成了LogSegment对象的创建，主要包含三部分文件：Log数据文件、offsetIdxFile以及timeIdxFile。


### LogSegment#append

获取到LogSegment后，即可调用`append()`完成消息的写入。

```
  @nonthreadsafe
  def append(largestOffset: Long,largestTimestamp: Long,shallowOffsetOfMaxTimestamp: Long,records: MemoryRecords): Unit = {
    if (records.sizeInBytes > 0) {
      val physicalPosition = log.sizeInBytes()
      if (physicalPosition == 0)
        rollingBasedTimestamp = Some(largestTimestamp)
      
      //最大物理偏移量 - 文件基准偏移量  要大于0且小于Int.MaxValue
      ensureOffsetInRange(largestOffset)
      //写入日志文件
      val appendedBytes = log.append(records)
      if (largestTimestamp > maxTimestampSoFar) {
        maxTimestampSoFar = largestTimestamp
        offsetOfMaxTimestampSoFar = shallowOffsetOfMaxTimestamp
      }
      //如果需要，添加索引  log.index.interval.bytes 每隔多少字节添加一个索引
      if (bytesSinceLastIndexEntry > indexIntervalBytes) {
        offsetIndex.append(largestOffset, physicalPosition)
        timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar)
        bytesSinceLastIndexEntry = 0
      }
      bytesSinceLastIndexEntry += records.sizeInBytes
    }
  }
```

将消息写入日志文件后，还需判断是否需要添加对应的索引，可通过配置`log.index.interval.bytes`参数控制每隔多少字节添加一个索引。

#### FileRecords#append

消息日志文件的写入通过Java NIO实现。

```
public int append(MemoryRecords records) throws IOException {
    if (records.sizeInBytes() > Integer.MAX_VALUE - size.get())
        throw new IllegalArgumentException("Append of size " + records.sizeInBytes() +
                " bytes is too large for segment with current file position at " + size.get());

    int written = records.writeFullyTo(channel);
    size.getAndAdd(written);
    return written;
}

public int writeFullyTo(GatheringByteChannel channel) throws IOException {
    buffer.mark();
    int written = 0;
    while (written < sizeInBytes())
        written += channel.write(buffer);
    buffer.reset();
    return written;
}
```

#### OffsetIndex#append

偏移量索引的添加实现如下：

```
  def append(offset: Long, position: Int): Unit = {
    inLock(lock) {
      require(!isFull, "Attempt to append to a full index (size = " + _entries + ").")
      if (_entries == 0 || offset > _lastOffset) {
        trace(s"Adding index entry $offset => $position to ${file.getAbsolutePath}")
        //根据绝对位移获取相对位移
        mmap.putInt(relativeOffset(offset))
        mmap.putInt(position)
        //索引计数
        _entries += 1
        _lastOffset = offset
        require(_entries * entrySize == mmap.position(), s"$entries entries but file position in index is ${mmap.position()}.")
      } else {
        throw new InvalidOffsetException(s"Attempt to append an offset ($offset) to position $entries no larger than the last offset appended (${_lastOffset}) to ${file.getAbsolutePath}.")
      }
    }
  }
```

可以看出偏移量索引包含两部分内容：

* relativeOffset(相对位移)
    
    该参数表示消息的offset相对于该LogSegment的baseOffset的偏移量，这里将消息Long类型的offset转化为Int类型，这样可以**节省4个字节，减小索引文件占用的空间**。如一个LogSegment的baseOffset为1024，对应的偏移量索引文件为
    `00000000000000000032.index`，offset为35的消息如果有索引的话，其relativeOffset = 35 - 32 = 3。
    
```
  private def toRelative(offset: Long): Option[Int] = {
    val relativeOffset = offset - baseOffset
    if (relativeOffset < 0 || relativeOffset > Int.MaxValue)
      None
    else
      Some(relativeOffset.toInt)
  }
  
```

* position：消息在**Log文件中对应的物理位置**，可以根据索引快速找到对应的消息

偏移量索引特点总结：

* 1、每隔`log.index.interval.bytes`字节创建一个索引，即偏移量索引是稀疏的；
* 2、偏移量索引中通过`relativeOffset`达到节省文件空间的目的，方便映射到内存读取；
* 3、偏移量索引有序，可通过二分查找定位指定位移的索引，然后通过position找到消息；

#### TimeIndex#maybeAppend

时间索引的添加实现如下：

```
  def maybeAppend(timestamp: Long, offset: Long, skipFullCheck: Boolean = false): Unit = {
    inLock(lock) {
      if (!skipFullCheck)
        require(!isFull, "Attempt to append to a full time index (size = " + _entries + ").")
      //有效性校验
      if (_entries != 0 && offset < lastEntry.offset)
        throw new InvalidOffsetException(s"Attempt to append an offset ($offset) to slot ${_entries} no larger than the last offset appended (${lastEntry.offset}) to ${file.getAbsolutePath}.")
      if (_entries != 0 && timestamp < lastEntry.timestamp)
        throw new IllegalStateException(s"Attempt to append a timestamp ($timestamp) to slot ${_entries} no larger" +
          s" than the last timestamp appended (${lastEntry.timestamp}) to ${file.getAbsolutePath}.")
      
      if (timestamp > lastEntry.timestamp) {
        trace(s"Adding index entry $timestamp => $offset to ${file.getAbsolutePath}.")
        mmap.putLong(timestamp)
        mmap.putInt(relativeOffset(offset))
        _entries += 1
        _lastEntry = TimestampOffset(timestamp, offset)
        require(_entries * entrySize == mmap.position(), s"${_entries} entries but file position in index is ${mmap.position()}.")
      }
    }
  }
```

时间索引的属性如下：

* timestamp(Long) 写入消息批次的最大时间戳
* relativeOffset(Int)，时间戳对应消息的相对偏移量的relativeOffset，计算方式同偏移量索引

每个追加的时间戳索引项中的timestamp必须大于之前追加的索引项的timestamp，否则不予追加，抛出异常，如果Broker端参数log.message.timestamp.type设置为LogAppendTime，那么消息的时间戳必定能够保持单调递增；
相反，如果是CreateTime类型则无法保证。


## sendResponseCallback

消息追加的结果会回调handleProduceRequest()方法中定义的sendResponseCallback()响应给Producer，源码如下：

```
def sendResponseCallback(responseStatus: Map[TopicPartition, PartitionResponse]): Unit = {
      val mergedResponseStatus = responseStatus ++ unauthorizedTopicResponses ++ nonExistingTopicResponses ++ invalidRequestResponses
      var errorInResponse = false
      //遍历所有分区追加结果，判断是否存在异常
      mergedResponseStatus.forKeyValue { (topicPartition, status) =>
        if (status.error != Errors.NONE) {
          //异常标志
          errorInResponse = true
      }
      //kafka限流实现
      val timeMs = time.milliseconds()
      val bandwidthThrottleTimeMs = quotas.produce.maybeRecordAndGetThrottleTimeMs(request, requestSize, timeMs)
      val requestThrottleTimeMs =
        if (produceRequest.acks == 0) 0
        else quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
      val maxThrottleTimeMs = Math.max(bandwidthThrottleTimeMs, requestThrottleTimeMs)
      if (maxThrottleTimeMs > 0) {
        request.apiThrottleTimeMs = maxThrottleTimeMs
        if (bandwidthThrottleTimeMs > requestThrottleTimeMs) {
          quotas.produce.throttle(request, bandwidthThrottleTimeMs, requestChannel.sendResponse)
        } else {
          quotas.request.throttle(request, requestThrottleTimeMs, requestChannel.sendResponse)
        }
      }

      if (produceRequest.acks == 0) {
        //acks = 0
        if (errorInResponse) {
          val exceptionsSummary = mergedResponseStatus.map { case (topicPartition, status) =>
            topicPartition -> status.error.exceptionName }.mkString(", ")
          //通过关闭Socket连接间接的通知Producer，Producer会更新元数据重新建立连接
          requestHelper.closeConnection(request, new ProduceResponse(mergedResponseStatus.asJava).errorCounts)
        } else {
          //不发送响应
          requestHelper.sendNoOpResponseExemptThrottle(request)
        }
      } else {
        //acks =1 or -1
        requestHelper.sendResponse(request, Some(new ProduceResponse(mergedResponseStatus.asJava, maxThrottleTimeMs)), None)
      }
    }
```

这里主要关注acks=0时Broker的处理，当消息追加出现异常时，Broker会主动关闭与Producer的Socket连接，间接地使Producer感知异常，使Producer重新建立连接并刷新元数据。


