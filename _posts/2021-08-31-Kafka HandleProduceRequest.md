---
layout: post
title:  "Kafka HandleProduceRequest"
date:   2021-11-17 14:25:10
categories: Kafka
---

[Kafka Producer 消息发送]()分析了KafkaProducer如何将消息发送至Broker，本篇内容将继续分析Kafka Broker端是如何处理ProducerRequest。

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

    //定义响应回调 这里先省略
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

Log是消息日志的抽象，[Kafka 简介]()中介绍了实际上的日志是以一段一段存在的，即LogSegment，Log对象中负责维护当前分区副本的所有LogSegment集合，属性如下：

```
//跳表结构
private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]
```

LogSegment是根据可配置的策略创建的，配置如下：

* log.segment.bytes，设置LogSegment文件的大小，超出则创建新的；
* log.segment.ms，每隔`log.segment.ms`创建一个新的LogSegment，即使未达到log.segment.bytes依然创建。


### append

追加日志的方法源码如下：

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

          ...//省略部分消息验证

          //消息大小大于LogSegment的最大长度，抛出异常
          if (validRecords.sizeInBytes > config.segmentSize) {
            throw new RecordBatchTooLargeException(s"Message batch size is ${validRecords.sizeInBytes} bytes in append " +s"to partition $topicPartition, which exceeds the maximum configured segment size of ${config.segmentSize}.")
          }

          //判断是否新建LogSegment
          val segment = maybeRoll(validRecords.sizeInBytes, appendInfo)
          
          val logOffsetMetadata = LogOffsetMetadata(
            messageOffset = appendInfo.firstOrLastOffsetOfFirstBatch,
            segmentBaseOffset = segment.baseOffset,
            relativePositionInSegment = segment.size)

          val (updatedProducers, completedTxns, maybeDuplicate) = analyzeAndValidateProducerState(
            logOffsetMetadata, validRecords, origin)

          maybeDuplicate match {
            case Some(duplicate) =>
              appendInfo.firstOffset = Some(LogOffsetMetadata(duplicate.firstOffset))
              appendInfo.lastOffset = duplicate.lastOffset
              appendInfo.logAppendTime = duplicate.timestamp
              appendInfo.logStartOffset = logStartOffset
            case None =>
              appendInfo.firstOffset = appendInfo.firstOffset.map { offsetMetadata =>
                offsetMetadata.copy(segmentBaseOffset = segment.baseOffset, relativePositionInSegment = segment.size)
              }

              segment.append(largestOffset = appendInfo.lastOffset,
                largestTimestamp = appendInfo.maxTimestamp,
                shallowOffsetOfMaxTimestamp = appendInfo.offsetOfMaxTimestamp,
                records = validRecords)

              updateLogEndOffset(appendInfo.lastOffset + 1)

              updatedProducers.values.foreach(producerAppendInfo => producerStateManager.update(producerAppendInfo))

              completedTxns.foreach { completedTxn =>
                val lastStableOffset = producerStateManager.lastStableOffset(completedTxn)
                segment.updateTxnIndex(completedTxn, lastStableOffset)
                producerStateManager.completeTxn(completedTxn)
              }

              producerStateManager.updateMapEndOffset(appendInfo.lastOffset + 1)

              maybeIncrementFirstUnstableOffset()

              if (unflushedMessages >= config.flushInterval) flush()
          }
          appendInfo
        }
      }
    }
  }

```