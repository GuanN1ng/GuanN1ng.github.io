---
layout: post
title:  Kafka Consumer UpdateFetchPosition
date:   2021-09-17 14:32:14
categories: Kafka
---

前面已经介绍了consumer的join group以及heartbeat内容，consumer已经获取到TopicPartition的分配方案，但还不能开始进行消息拉取，此时consumer并不知道该从TopicPartition的哪个位置(offset)开始消费，本文将继续分析consumer
如何更新订阅主题分区的消费偏移量(offset)。

[Kafka Consumer JoinGroup](https://guann1ng.github.io/kafka/2021/09/06/Kafka-Consumer-JoinGroup/)中分析KafkaConsumer#updateAssignmentMetadataIfNeeded()方法时，只分析了ConsumerCoordinator#poll的方法调用：

```
boolean updateAssignmentMetadataIfNeeded(final Timer timer, final boolean waitForJoinGroup) {
    if (coordinator != null && !coordinator.poll(timer, waitForJoinGroup)) {
        return false;
    }
    return updateFetchPositions(timer);
}
```

coordinator.poll()方法中consumer完成了JoinGroup的完整过程，包括：

* 1、FIND_COORDINATOR：获取GroupCoordinator的地址并建立TCP连接；
* 2、JOIN_GROUP：consumer leader选举及分区策略选举，触发rebalance；
* 3、SYNC_GROUP：获取消息主题分区的分配结果；
* 4、心跳及消费位移提交(auto commit)。

下面开始`updateFetchPositions`方法的分析。

### updateFetchPositions

updateFetchPositions的作用是更新分配到的TopicPartitions的消费进度，防止consumer重复消费，源码如下：

```
private boolean updateFetchPositions(final Timer timer) {

    //Validate offsets for all assigned partitions for which a leader change has been detected.
    fetcher.validateOffsetsIfNeeded();

    //判断所有分区 是否有有效的消费位移
    cachedSubscriptionHashAllFetchPositions = subscriptions.hasAllFetchPositions();
    if (cachedSubscriptionHashAllFetchPositions) return true;
    
    //consuemr向groupCoordinator发送OffsetFetchRequest请求，获取当前组消费者上次提交的offset
    if (coordinator != null && !coordinator.refreshCommittedOffsetsIfNeeded(timer)) return false;
    
   
    // 仍未获取到消费位移信息的分区是否全部配置了位移重置策勒（auto.offset.reset：earliest或者latest）
    // 否则抛出异常 NoOffsetForPartitionException
    subscriptions.resetInitializingPositions();
    
    //发送ListOffsetsRequest，根据auto.offset.reset策略重置TopicPartition的消费进度
    fetcher.resetOffsetsIfNeeded();
    return true;
}
```

主要关注两部分内容：

* refreshCommittedOffsetsIfNeeded()：对FetchState为INITIALIZING的分区，向groupCoordinator发送请求OffsetFetchRequest，获取并更新TopicPartition对应的committed offsets；

* resetOffsetsIfNeeded()：若仍存在没有位移信息的分区，向groupCoordinator发送请求ListOffsetsRequest，按照auto.offset.reset执行offset重置。


### TopicPartitionState

consumer端会保存订阅的每个TopicPartition的消费进度信息，并在每次拉取后更新，存储信息的对象为`TopicPartitionState`：

```
private static class TopicPartitionState {

    private FetchState fetchState; //消费进度状态
    private FetchPosition position; // last consumed position

    private Long highWatermark; // the high watermark from last fetch
    private Long logStartOffset; // the log start offset
    private Long lastStableOffset;
    private boolean paused;  // whether this partition has been paused by the user
    private OffsetResetStrategy resetStrategy;  // the strategy to use if the offset needs resetting
    private Long nextRetryTimeMs;
    private Integer preferredReadReplica;
    private Long preferredReadReplicaExpireTimeMs;
    private boolean endOffsetRequested;
    
    ...//成员方法
}

```

#### 初始化

TopicPartitionState的初始化在JoinGroup完成后的onJoinComplete方法中完成，方法调用路径为：`ConsumerCoordinator#onJoinComplete()->SubscriptionState#assignFromSubscribed()`。

```
public synchronized void assignFromSubscribed(Collection<TopicPartition> assignments) {
    if (!this.hasAutoAssignedPartitions()) throw new IllegalArgumentException("Attempt to dynamically assign partitions while manual assignment in use");
    Map<TopicPartition, TopicPartitionState> assignedPartitionStates = new HashMap<>(assignments.size());
    for (TopicPartition tp : assignments) {
        TopicPartitionState state = this.assignment.stateValue(tp);
        if (state == null)
            // 为 null 创建新的
            state = new TopicPartitionState();
        assignedPartitionStates.put(tp, state);
    }
    assignmentId++;
    this.assignment.set(assignedPartitionStates);
}

```

#### 状态机

FetchState：

* INITIALIZING：初始化状态；
* FETCHING：正常消费状态；
* AWAIT_RESET：没有消费进度或丢失，待重置，auto.offset.reset：earliest或者latest；
* AWAIT_VALIDATION：订阅元数据发生变化，通过发送OffsetsForLeaderEpochRequest重新获取。

### OffsetFetchRequest

refreshCommittedOffsetsIfNeeded()方法中会为状态是INITIALIZING的分区发起对GroupCoordinator的OffsetFetchRequest请求，获取last consumed offset，作为下次消息拉取的参数。

```
public boolean refreshCommittedOffsetsIfNeeded(Timer timer) {
    //获取state.fetchState.equals(FetchStates.INITIALIZING)的分区
    final Set<TopicPartition> initializingPartitions = subscriptions.initializingPartitions();
    //发送OffsetFetchRequest，获取位移
    final Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(initializingPartitions, timer);
    if (offsets == null) return false;
    //响应处理
    for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
        final TopicPartition tp = entry.getKey();
        final OffsetAndMetadata offsetAndMetadata = entry.getValue();
        if (offsetAndMetadata != null) {
            //更新epoch
            entry.getValue().leaderEpoch().ifPresent(epoch -> this.metadata.updateLastSeenEpochIfNewer(entry.getKey(), epoch));
            //数据更新
            if (this.subscriptions.isAssigned(tp)) {
                final ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(tp);
                final SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(offsetAndMetadata.offset(), offsetAndMetadata.leaderEpoch(),leaderAndEpoch);
                //TopicPartitionState更新
                this.subscriptions.seekUnvalidated(tp, position);
                log.info("Setting offset for partition {} to the committed offset {}", tp, position);
            } else {
                log.info("Ignoring the returned {} since its partition {} is no longer assigned",offsetAndMetadata, tp);
            }
        }
    }
    return true;
}
```


#### sendOffsetFetchRequest

请求发送的逻辑比较简单，当上次请求的结果仍未返回时，Kafka会等待上次的请求结果，不会发起新的请求，具体的方法调用为：ConsumerCoordinator#fetchCommittedOffsets()->ConsumerCoordinator#sendOffsetFetchRequest()。

```
private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
    Node coordinator = checkAndGetCoordinator();
    //
    if (coordinator == null) return RequestFuture.coordinatorNotAvailable();
    OffsetFetchRequest.Builder requestBuilder = new OffsetFetchRequest.Builder(this.rebalanceConfig.groupId, true, new ArrayList<>(partitions), throwOnFetchStableOffsetsUnsupported);

    return client.send(coordinator, requestBuilder).compose(new OffsetFetchResponseHandler());
}
```
GroupCoordinator的响应由offsetFetchResponseHandler进行数据预处理并返回。

#### handleOffsetFetchRequest

请求由KafkaApis#handleOffsetFetchRequest方法处理，这里只贴出核心方法`getOffsets`代码。

```
  def getOffsets(groupId: String, requireStable: Boolean, topicPartitionsOpt: Option[Seq[TopicPartition]]): Map[TopicPartition, PartitionData] = {
    val group = groupMetadataCache.get(groupId)
    if (group == null) {
      //消费者组元信息不存在 返回INVALID_OFFSET(-1)  Errors.NONE
      topicPartitionsOpt.getOrElse(Seq.empty[TopicPartition]).map { topicPartition =>
        val partitionData = new PartitionData(OffsetFetchResponse.INVALID_OFFSET, Optional.empty(), "", Errors.NONE) 
        topicPartition -> partitionData
      }.toMap
    } else {
      group.inLock {
        if (group.is(Dead)) {
          //消费者组消费offset日志已被删除 返回 INVALID_OFFSET Errors.NONE
          topicPartitionsOpt.getOrElse(Seq.empty[TopicPartition]).map { topicPartition =>
            val partitionData = new PartitionData(OffsetFetchResponse.INVALID_OFFSET, Optional.empty(), "", Errors.NONE)
            topicPartition -> partitionData
          }.toMap
        } else {
          //返回group消费的所有tp的offset信息
          val topicPartitions = topicPartitionsOpt.getOrElse(group.allOffsets.keySet)

          topicPartitions.map { topicPartition =>
            if (requireStable && group.hasPendingOffsetCommitsForTopicPartition(topicPartition)) {
              //该分区正在更新消费offset Errors.UNSTABLE_OFFSET_COMMIT
              topicPartition -> new PartitionData(OffsetFetchResponse.INVALID_OFFSET,Optional.empty(), "", Errors.UNSTABLE_OFFSET_COMMIT)
            } else {
              val partitionData = group.offset(topicPartition) match {
                case None =>
                  //新订阅的主题，不存在消费进度，返回INVALID_OFFSET Errors.NONE
                  new PartitionData(OffsetFetchResponse.INVALID_OFFSET,Optional.empty(), "", Errors.NONE)
                case Some(offsetAndMetadata) =>
                  //获取元数据中保存的消费进度， 返回last consumed offset
                  new PartitionData(offsetAndMetadata.offset,offsetAndMetadata.leaderEpoch, offsetAndMetadata.metadata, Errors.NONE)
              }
              topicPartition -> partitionData
            }
          }.toMap
        }
      }
    }
  }
```


GroupCoordinator获取不到该主题分区的offset，返回INVALID_OFFSET(`NVALID_OFFSET = -1L`)的情况有以下几种(无异常) ：

* consumer group为新建group；
* 当_consumer_offsets主题中关于这个group的消费位移消息被删除后(例超过offsets.retention.minutes配置时间，过期删除)，则消费位移丢失。
* 所查找的Topic为consumerGroup新订阅的主题；



#### OffsetFetchResponseHandler

OffsetFetchResponse会返回对应分区的消费进度，若offset有效(partitionData.offset >= 0)，则更新TopicPartitionState等元数据，若无效(INVALID_OFFSET)
则不做处理。

```
public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
    if (response.hasError()) {
        ...// 异常处理
    }

    Set<String> unauthorizedTopics = null;
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(response.responseData().size());
    Set<TopicPartition> unstableTxnOffsetTopicPartitions = new HashSet<>();
    for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData().entrySet()) {
        TopicPartition tp = entry.getKey();
        OffsetFetchResponse.PartitionData partitionData = entry.getValue();
        if (partitionData.hasError()) {
            Errors error = partitionData.error;
            log.debug("Failed to fetch offset for partition {}: {}", tp, error.message());
            if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                future.raise(new KafkaException("Topic or Partition " + tp + " does not exist"));
                return;
            } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                if (unauthorizedTopics == null) {
                    unauthorizedTopics = new HashSet<>();
                }
                unauthorizedTopics.add(tp.topic());
            } else if (error == Errors.UNSTABLE_OFFSET_COMMIT) {
                //重试
                unstableTxnOffsetTopicPartitions.add(tp);
            } else {
                future.raise(new KafkaException("Unexpected error in fetch offset response for partition " +tp + ": " + error.message()));
                return;
            }
        } else if (partitionData.offset >= 0) {
            // record the position with the offset 
            offsets.put(tp, new OffsetAndMetadata(partitionData.offset, partitionData.leaderEpoch, partitionData.metadata));
        } else {
            // INVALID_OFFSET(-1)   -1 indicates no committed offset to fetch
            offsets.put(tp, null);
        }
    }

    if (unauthorizedTopics != null) {
        future.raise(new TopicAuthorizationException(unauthorizedTopics));
    } else if (!unstableTxnOffsetTopicPartitions.isEmpty()) {
        // just retry
        future.raise(new UnstableOffsetCommitException("There are unstable offsets for the requested topic partitions"));
    } else {
        future.complete(offsets);
    }
}
```

### ListOffsetsRequest


经过OffsetFetchRequest请求后，仍未获取到相应的offset的分区，则会根据配置的`auto.offset.reset`的值来决定从何处(offset)进行消费，auto.offset.reset共有3个可选配置项：

* earliest：**默认值**，从分区消息日志的起始处开始消费
* latest：从分区日志的末尾开始消费
* none：抛出NoOffsetForPartitionException异常

```
public void resetOffsetsIfNeeded() {
    RuntimeException exception = cachedListOffsetsException.getAndSet(null);
    if (exception != null)
        throw exception;
    //状态为AWAIT_RESET的分区
    Set<TopicPartition> partitions = subscriptions.partitionsNeedingReset(time.milliseconds());
    if (partitions.isEmpty())
        return;

    final Map<TopicPartition, Long> offsetResetTimestamps = new HashMap<>();
    for (final TopicPartition partition : partitions) {
        //根据策略转换固定时间戳
        Long timestamp = offsetResetStrategyTimestamp(partition);
        if (timestamp != null)
            offsetResetTimestamps.put(partition, timestamp);
    }
    //发送请求
    resetOffsetsAsync(offsetResetTimestamps);
}
```

ListOffsetsRequest本质是根据请求参数中的timeStamp获取消费者能够fetch的位移，timestamp根据配置的策略决定，如下：

```
private Long offsetResetStrategyTimestamp(final TopicPartition partition) {
    OffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
    if (strategy == OffsetResetStrategy.EARLIEST) // -2L
        return ListOffsetsRequest.EARLIEST_TIMESTAMP;
    else if (strategy == OffsetResetStrategy.LATEST)
        return ListOffsetsRequest.LATEST_TIMESTAMP; // -1L
    else
        return null;
}

```

#### sendListOffsetRequest

resetOffsetsAsync()方法主要是对分区数据按照leader副本所在节点进行分组，然后遍历发送并注册处理响应的Listener。

```
private void resetOffsetsAsync(Map<TopicPartition, Long> partitionResetTimestamps) {
    //根据TopicPartition leader副本所在节点 进行分组
    Map<Node, Map<TopicPartition, ListOffsetsPartition>> timestampsToSearchByNode =
            groupListOffsetRequests(partitionResetTimestamps, new HashSet<>());
    for (Map.Entry<Node, Map<TopicPartition, ListOffsetsPartition>> entry : timestampsToSearchByNode.entrySet()) {
        //按照节点发送请求
        
        Node node = entry.getKey();
        final Map<TopicPartition, ListOffsetsPartition> resetTimestamps = entry.getValue();
        subscriptions.setNextAllowedRetry(resetTimestamps.keySet(), time.milliseconds() + requestTimeoutMs);

        RequestFuture<ListOffsetResult> future = sendListOffsetRequest(node, resetTimestamps, false);
        future.addListener(new RequestFutureListener<ListOffsetResult>() {
            @Override
            public void onSuccess(ListOffsetResult result) {
                if (!result.partitionsToRetry.isEmpty()) {
                    subscriptions.requestFailed(result.partitionsToRetry, time.milliseconds() + retryBackoffMs);
                    metadata.requestUpdate();
                }
                
                for (Map.Entry<TopicPartition, ListOffsetData> fetchedOffset : result.fetchedOffsets.entrySet()) {
                    //更新TopicPartitionState
                    TopicPartition partition = fetchedOffset.getKey();
                    ListOffsetData offsetData = fetchedOffset.getValue();
                    ListOffsetsPartition requestedReset = resetTimestamps.get(partition);
                    resetOffsetIfNeeded(partition, timestampToOffsetResetStrategy(requestedReset.timestamp()), offsetData);
                }
            }

            @Override
            public void onFailure(RuntimeException e) {
                subscriptions.requestFailed(resetTimestamps.keySet(), time.milliseconds() + retryBackoffMs);
                metadata.requestUpdate();
                if (!(e instanceof RetriableException) && !cachedListOffsetsException.compareAndSet(null, e))
                    log.error("Discarding error in ListOffsetResponse because another error is pending", e);
            }
        });
    }
}

```

请求发送：

```
private RequestFuture<ListOffsetResult> sendListOffsetRequest(final Node node,final Map<TopicPartition, ListOffsetsPartition> timestampsToSearch,boolean requireTimestamp) {
    ListOffsetsRequest.Builder builder = ListOffsetsRequest.Builder
            .forConsumer(requireTimestamp, isolationLevel, false)
            .setTargetTimes(ListOffsetsRequest.toListOffsetsTopics(timestampsToSearch));

    return client.send(node, builder).compose(new RequestFutureAdapter<ClientResponse, ListOffsetResult>() {
                @Override
                public void onSuccess(ClientResponse response, RequestFuture<ListOffsetResult> future) {
                    ListOffsetsResponse lor = (ListOffsetsResponse) response.responseBody();
                    handleListOffsetResponse(lor, future);
                }
            });
}

```

#### handleListOffsetRequest

Broker端请求处理的入口为KafkaApis#handleListOffsetRequest，核心方法Partition#fetchOffsetForTimestamp()的实现：


```
  def fetchOffsetForTimestamp(timestamp: Long,
                              isolationLevel: Option[IsolationLevel],
                              currentLeaderEpoch: Optional[Integer],
                              fetchOnlyFromLeader: Boolean): Option[TimestampAndOffset] = inReadLock(leaderIsrUpdateLock) {
    //获取日志对象
    val localLog = localLogWithEpochOrException(currentLeaderEpoch, fetchOnlyFromLeader)
    
    //consumer隔离级别，获取最新可拉取的位移
    val lastFetchableOffset = isolationLevel match {
      case Some(IsolationLevel.READ_COMMITTED) => localLog.lastStableOffset
      case Some(IsolationLevel.READ_UNCOMMITTED) => localLog.highWatermark
      case None => localLog.logEndOffset
    }
    ...

    val maybeOffsetsError: Option[ApiException] = leaderEpochStartOffsetOpt
      .filter(epochStart => isolationLevel.isDefined && epochStart > localLog.highWatermark)
      .map(epochStart => Errors.OFFSET_NOT_AVAILABLE.exception(...)
    
    //根据时间戳获取对应的log的位移
    def getOffsetByTimestamp: Option[TimestampAndOffset] = {
      logManager.getLog(topicPartition).flatMap(log => log.fetchOffsetByTimestamp(timestamp))
    }

    timestamp match {
      case ListOffsetsRequest.LATEST_TIMESTAMP =>
        //返回最新的可拉取位移
        maybeOffsetsError.map(e => throw e)
          .orElse(Some(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, lastFetchableOffset, Optional.of(leaderEpoch))))
      case ListOffsetsRequest.EARLIEST_TIMESTAMP =>
        //获取startOffset
        getOffsetByTimestamp
      case _ =>
        //KafkaConsuemr#offsetsForTimes()
        getOffsetByTimestamp.filter(timestampAndOffset => timestampAndOffset.offset < lastFetchableOffset)
          .orElse(maybeOffsetsError.map(e => throw e))
    }
  }
```

Log层的源码实现后续分析Server端日志存储时再进行分析。

### 总结

updateFetchPositions方法执行完毕后，无论是通过OffsetFetchRequest还是ListOffsetsRequest，consumer所有订阅的TopicPartition(除去查询不到offset且auto.offset.reset=none的分区)已获取到有效的分区offset，
拉取消息前的所有准备工作已经完成。