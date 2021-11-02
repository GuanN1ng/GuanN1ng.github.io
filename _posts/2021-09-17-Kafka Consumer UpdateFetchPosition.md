---
layout: post
title:  Kafka Consumer UpdateFetchPosition
date:   2021-09-17 14:32:14
categories: Kafka
---

前面两篇内容已经介绍了consumer的join group以及heartbeat内容，consumer已经获取到TopicPartition的分配方案，但还不能开始进行消息拉取，consumer不知道该从TopicPartition的哪个位置(offset)开始消费，本文继续KafkaConsumer#poll方法的源码解析，来了解consumer
如何获取订阅主题分区的拉取offset。

[Kafka Consumer JoinGroup](https://guann1ng.github.io/kafka/2021/09/06/Kafka-Consumer-JoinGroup/)中我们查看KafkaConsumer#updateAssignmentMetadataIfNeeded方法源码时，
只分析了ConsumerCoordinator#poll的方法调用：

```
boolean updateAssignmentMetadataIfNeeded(final Timer timer, final boolean waitForJoinGroup) {
    if (coordinator != null && !coordinator.poll(timer, waitForJoinGroup)) {
        return false;
    }
    return updateFetchPositions(timer);
}
```

coordinator.poll完成了consumer入组的完整过程，包括：

* 1、FIND_COORDINATOR：获取GroupCoordinator的地址并建立TCP连接；
* 2、JOIN_GROUP：consumer leader选举及分区策略选举，触发rebalance；
* 3、SYNC_GROUP：获取消息主题分区的分配结果；
* 4、心跳及消费位移提交(auto commit)。

下面开始`updateFetchPositions`方法的分析。

### updateFetchPositions

updateFetchPositions的作用是更新分配到的TopicPartitions的消费进度，防止consumer重复消费，源码如下：

```
private boolean updateFetchPositions(final Timer timer) {
    //是否有分区的leader切换，有抛出异常，或epoch变更
    fetcher.validateOffsetsIfNeeded();
    //判断所有分区 是否在「FETCHING」状态中
    cachedSubscriptionHashAllFetchPositions = subscriptions.hasAllFetchPositions();
    if (cachedSubscriptionHashAllFetchPositions) return true;
    
    //没有消费状态的分区，consuemr向groupCoordinator发送OffsetFetchRequest请求，获取当前组消费者上次提交的offset
    if (coordinator != null && !coordinator.refreshCommittedOffsetsIfNeeded(timer)) return false;
    
    // 有auto.offset.reset策略的分区，设置策略，比如earliest或者latest
    // 否则抛出异常 NoOffsetForPartitionException
    subscriptions.resetInitializingPositions();
    
    //发送ListOffsetsRequest，根据策略获取TopicPartition的offset
    fetcher.resetOffsetsIfNeeded();
    return true;
}
```

主要可分为三部分内容：

* 1、validateOffsetsIfNeeded：检查TopicPartition的副本Leader及Epoch，向Leader发送异步请求获取lase consume offset，验证及更新；

* 2、refreshCommittedOffsetsIfNeeded：对FetchState为INITIALIZING的分区，向groupCoordinator发送请求OffsetFetchRequest，获取并更新TopicPartition对应的committed offsets；

* 3、resetOffsetsIfNeeded：经过第2步，若仍存在没有位移信息的分区，向groupCoordinator发送请求ListOffsetsRequest，按照auto.offset.reset执行offset重置。


### TopicPartitionState

consumer端会保存订阅的每个TopicPartition的消费进度信息，并在每次拉取后更新，存储信息的对象为`TopicPartitionState`：

```
private static class TopicPartitionState {

    private FetchState fetchState;
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
    
    ...//方法
}

```

#### 初始化

TopicPartitionState的初始化在JoinGroup完成后的onJoinComplete方法中完成，方法调用路径为：`ConsumerCoordinator#onJoinComplete()->SubscriptionState#assignFromSubscribed`。

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

* INITIALIZING：初始化状态
* FETCHING
* AWAIT_RESET
* AWAIT_VALIDATION


### OffsetFetchRequest

consumer在refreshCommittedOffsetsIfNeeded方法发起对GroupCoordinator的OffsetFetchRequest请求，为状态是INITIALIZING的分区获取last consumed offset，作为下次消息拉取的参数。

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
GroupCoordinator的请求响应由offsetFetchResponseHandler进行数据预处理并返回。

#### handleOffsetFetchRequest

请求有KafkaApis#handleOffsetFetchRequest方法处理，这里只贴出核心方法`getOffsets`代码。

```
  def getOffsets(groupId: String, requireStable: Boolean, topicPartitionsOpt: Option[Seq[TopicPartition]]): Map[TopicPartition, PartitionData] = {
    //获取group信息
    val group = groupMetadataCache.get(groupId)
    if (group == null) {
      ...// PartitionData(OffsetFetchResponse.INVALID_OFFSET,Optional.empty(), "", Errors.NONE)
    } else {
      group.inLock {
        if (group.is(Dead)) {
          ...// PartitionData(OffsetFetchResponse.INVALID_OFFSET,Optional.empty(), "", Errors.NONE)
        } else {
          //获取当前组的所有分区offset
          val topicPartitions = topicPartitionsOpt.getOrElse(group.allOffsets.keySet)

          topicPartitions.map { topicPartition =>
            if (requireStable && group.hasPendingOffsetCommitsForTopicPartition(topicPartition)) {
              //该TopicPartition有正在处理的位移提交请求
              topicPartition -> new PartitionData(OffsetFetchResponse.INVALID_OFFSET,Optional.empty(), "", Errors.UNSTABLE_OFFSET_COMMIT)
            } else {
              val partitionData = group.offset(topicPartition) match {
                case None =>
                  //不存在offset
                  new PartitionData(OffsetFetchResponse.INVALID_OFFSET,Optional.empty(), "", Errors.NONE)
                case Some(offsetAndMetadata) =>
                  //正常返回
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


### ListOffsetsRequest

OffsetFetchRequest中，出现不存在offset的情况有以下几种：

* consumer group为新建group；
* 所查找的TopicPartition为consumer新订阅的主题；
* 当_consumer_offsets主题中关于这个group的消费位移消息被删除后(例超过offsets.retention.minutes配置时间，过期删除)，则消费位移丢失。

此时仍没有获取到相应的offset的分区，则会根据配置的`auto.offset.reset`的值来决定从何处(offset)进行消费，auto.offset.reset共有3个可选配置项：

* earliest：**默认值**，从分区消息日志的起始处开始消费
* latest：从分区日志的末尾开始消费
* none：抛出NoOffsetForPartitionException异常

对于前两种配置，KafkaConsumer通过向GroupCoordinator发送ListOffsetsRequest来获取对应的位移信息，这里的方法调用链为Fetcher#resetOffsetsIfNeeded()->Fetcher#resetOffsetsAsync()->Fetcher#sendListOffsetRequest()。

#### sendListOffsetRequest

请求发送源码如下：

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

#### handleListOffsetRequest

Broker端请求处理的入口为KafkaApis#handleListOffsetRequest，整个的请求过程处理有4层：

* 1、KafkaApis#handleListOffsetRequest
* 2、ReplicaManager#fetchOffsetForTimestamp
* 3、Partition#fetchOffsetForTimestamp
* 4、Log#fetchOffsetByTimestamp

我们主要看下Partition#fetchOffsetForTimestamp()方法的实现：


```
  def fetchOffsetForTimestamp(timestamp: Long,
                              isolationLevel: Option[IsolationLevel],
                              currentLeaderEpoch: Optional[Integer],
                              fetchOnlyFromLeader: Boolean): Option[TimestampAndOffset] = inReadLock(leaderIsrUpdateLock) {
    
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
        maybeOffsetsError.map(e => throw e)
          .orElse(Some(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, lastFetchableOffset, Optional.of(leaderEpoch))))
      case ListOffsetsRequest.EARLIEST_TIMESTAMP =>
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