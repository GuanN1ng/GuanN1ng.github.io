---
layout: post
title:  Kafka Poll
date:   2021-10-12 17:18:31
categories: Kafka
---

前面几篇内容分析了`updateAssignmentMetadataIfNeeded()`的执行流程，包含两部分内容：

* ConsumerCoordinator#poll方法，获取GroupCoordinator，完成JoinGroup及主题分区方案获取，详情见[Kafka Consumer JoinGroup](https://guann1ng.github.io/kafka/2021/09/06/Kafka-Consumer-JoinGroup/)；
* KafkaConsumer#updateFetchPositions方法，更新consumer订阅的TopicPartition的有效offset，确认下次消息拉取的偏移量(offset)，详情见[Kafka Consumer UpdateFetchPosition](https://guann1ng.github.io/kafka/2021/09/17/Kafka-Consumer-UpdateFetchPosition/)。

本篇内容将分析KafkaConsumer#poll方法的后半部分内容，即消息拉取部分，核心方法为pollForFetches()。

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


## RequestFutureListener

FetchRequest请求发送时注册的Listener，会在获取到响应时触发回调，主要功能是将响应数据封装为CompletedFetch对象并放入`completedFetches`队列中以及从`nodesWithPendingFetchRequests`将对应的Broker标识移除，以便可以对Broker发起
下次Fetch请求的发送。

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

上一步的sendFetches方法中会异步的把消息拉取的结果放在completedFetches队列中，fetchedRecords方法主要有两部分作用：

* 遍历completedFetches队列中的数据，做进一步验证处理返回给consumer进行消费，如将已暂停消费的分区(使用pause(Collection<TopicPartition> partitions)的分区)的消息过滤等；
* 更新TopicPartitionState元信息，准备下一次拉取。

```
public Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords() {
    //初始化结果集
    Map<TopicPartition, List<ConsumerRecord<K, V>>> fetched = new HashMap<>();
    //临时保存分区被暂停消费的数据
    Queue<CompletedFetch> pausedCompletedFetches = new ArrayDeque<>();
    //剩余可拉取消息数
    int recordsRemaining = maxPollRecords;
    try {
        //可拉取数为0 退出
        while (recordsRemaining > 0) {
            //nextInLineFetch  Fetcher类的成员属性，表示当前正在处理的分区响应数据
            if (nextInLineFetch == null || nextInLineFetch.isConsumed) {
                //从completedFetches获取一个响应数据
                CompletedFetch records = completedFetches.peek();
                //缓存中所有的拉取响应已处理完
                if (records == null) break;
                if (records.notInitialized()) {
                    try {
                        //CompletedFetch中的数据未初始化，执
                        nextInLineFetch = initializeCompletedFetch(records);  //①
                    } catch (Exception e) {
                        FetchResponseData.PartitionData partition = records.partitionData;
                        if (fetched.isEmpty() && FetchResponse.recordsOrFail(partition).sizeInBytes() == 0) { 
                          //本轮第一次获取，且CompletedFetch内消息未空，抛出异常，丢弃当前CompletedFetch
                          completedFetches.poll(); 
                        }
                        throw e;
                    }
                } else {
                    //数据已初始化
                    nextInLineFetch = records;
                }
                //从队列去除，下一次循环将从nextInLineFetch中获取
                completedFetches.poll();
            } else if (subscriptions.isPaused(nextInLineFetch.partition)) {
                //已暂停消费的分区数据
                pausedCompletedFetches.add(nextInLineFetch);
                nextInLineFetch = null;
            } else {
                //获取消息
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

可以看到整个方法主要分为两部分：initializeCompletedFetch()方法及fetchRecords()方法，而这两个方法均是围绕CompletedFetch数据进行操作。


## CompletedFetch

CompletedFetch表示对某一主题分区执行的一次FetchRequest请求返回的消息数据，结构定义如下：

```
private class CompletedFetch {
        private final TopicPartition partition; //主题分区
        private final Iterator<? extends RecordBatch> batches;  //消息
        private final Set<Long> abortedProducerIds; //中止事务的producerId
        private final PriorityQueue<FetchResponse.AbortedTransaction> abortedTransactions; //本批次内中止事务的信息
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

initializeCompletedFetch()方法主要是确保响应数据有效，并更新本地SubscriptionState.TopicPartitionState元数据。

```
private CompletedFetch initializeCompletedFetch(CompletedFetch nextCompletedFetch) {
    TopicPartition tp = nextCompletedFetch.partition;
    FetchResponseData.PartitionData partition = nextCompletedFetch.partitionData;
    //FetchResponse的返回的消息起始位移
    long fetchOffset = nextCompletedFetch.nextFetchOffset;
    CompletedFetch completedFetch = null;
    Errors error = Errors.forCode(partition.errorCode());

    try {
        if (!subscriptions.hasValidPosition(tp)) {
            // this can happen when a rebalance happened while fetch is still in-flight
            log.debug("Ignoring fetched records for partition {} since it no longer has valid position", tp);
        } else if (error == Errors.NONE) {
            FetchPosition position = subscriptions.position(tp);
            if (position == null || position.offset != fetchOffset) {
                //响应的消息起始位移与本地元数据的拉取起始位移不一致
                return null;
            }
            
            Iterator<? extends RecordBatch> batches = FetchResponse.recordsOrFail(partition).batches().iterator();
            completedFetch = nextCompletedFetch;
            if (!batches.hasNext() && FetchResponse.recordsSize(partition) > 0) {
                //响应结果中没有完整的一条消息但有数据  ，低版本API可能发生 max.partition.fetch.bytes ， 抛出异常 
                //should not happen with brokers that support FetchRequest/Response V3 or higher (i.e. KIP-74)
                ... //异常抛出代码 
            }
            //本地主题分区元数据更新
            if (partition.highWatermark() >= 0) {
                subscriptions.updateHighWatermark(tp, partition.highWatermark());
            }
            if (partition.logStartOffset() >= 0) {
                subscriptions.updateLogStartOffset(tp, partition.logStartOffset());
            }
            if (partition.lastStableOffset() >= 0) {
                subscriptions.updateLastStableOffset(tp, partition.lastStableOffset());
            }
            
            //Fetch响应返回了PreferredReplica，更新本地元数据，下次poll从PreferredReplica读取消息
            if (FetchResponse.isPreferredReplica(partition)) {
                subscriptions.updatePreferredReadReplica(completedFetch.partition, partition.preferredReadReplica(), () -> {
                    //设置PreferredReplica过期时间，  值为metadata.max.age.ms  元数据有效时间
                    long expireTimeMs = time.milliseconds() + metadata.metadataExpireMs();
                    return expireTimeMs;
                });
            }
            //初始化完成
            nextCompletedFetch.initialized = true;
        } else if{
           ... //省略部分异常
        } else if (error == Errors.OFFSET_OUT_OF_RANGE) {
            //
            Optional<Integer> clearedReplicaId = subscriptions.clearPreferredReadReplica(tp);
            if (!clearedReplicaId.isPresent()) {
                //不存在PreferredReadReplica ，消息是从leader副本读取
                FetchPosition position = subscriptions.position(tp);
                if (position == null || fetchOffset != position.offset) {
                    //响应与本地记录不一致，丢弃
                    log.debug("Discarding stale fetch response for partition {} since the fetched offset {} " +
                            "does not match the current offset {}", tp, fetchOffset, position);
                } else {
                    //进行auto.offset.reset,重置消费位移，若未配置，抛出异常
                    handleOffsetOutOfRange(position, tp);
                }
            } else {
                log.debug("Unset the preferred read replica {} for partition {} since we got {} when fetching {}",
                        clearedReplicaId.get(), tp, error, fetchOffset);
            }
        } 
        ...// 省略部分异常处理
    } finally {
        if (completedFetch == null)
            nextCompletedFetch.metricAggregator.record(tp, 0, 0);
        if (error != Errors.NONE)
            // we move the partition to the end if there was an error. This way, it's more likely that partitions for
            // the same topic can remain together (allowing for more efficient serialization).
            subscriptions.movePartitionToEnd(tp);
    }

    return completedFetch;
}
```

## fetchRecords

CompletedFetch通过验证后，即可调用fetchRecords()方法从CompletedFetch中读取消息，实现如下：

```
private List<ConsumerRecord<K, V>> fetchRecords(CompletedFetch completedFetch, int maxRecords) {
    if (!subscriptions.isAssigned(completedFetch.partition)) {
        //再次验证是否订阅了该主题分区，防止有rebalance发生
    } else if (!subscriptions.isFetchable(completedFetch.partition)) {
        //再次判断分区是否被暂停消费
    } else {
        //consumer本地的消费进度
        FetchPosition position = subscriptions.position(completedFetch.partition);
        if (position == null) {
            throw new IllegalStateException("Missing position for fetchable partition " + completedFetch.partition);
        }
        if (completedFetch.nextFetchOffset == position.offset) {
            //位移验证一致，从CompletedFetch读取消息
            List<ConsumerRecord<K, V>> partRecords = completedFetch.fetchRecords(maxRecords);
            if (completedFetch.nextFetchOffset > position.offset) {
                //更新本地消费进度
                FetchPosition nextPosition = new FetchPosition(completedFetch.nextFetchOffset,completedFetch.lastEpoch,position.currentLeader);
                subscriptions.position(completedFetch.partition, nextPosition);
            }
            
            ... // 省略 FetchManagerMetrics 相关，统计服务
            //返回消息
            return partRecords;
        } else { //消费位移不正确 }
    }
    //无法消费，置为已消费，并关闭流
    completedFetch.drain();
    //返回空
    return emptyList();
}
```

Fetcher#fetchRecords()方法中主要是完成消息读取前的验证，如分区订阅关系是够改变，分区是否被暂停消费以及本地消费进度与响应是否一致等判断，消息的读取则是通过
CompletedFetch#fetchRecords()方法完成。

### CompletedFetch#fetchRecords

从CompletedFetch中读取消息的方法源码如下：

```
private List<ConsumerRecord<K, V>> fetchRecords(int maxRecords) {
  // Error when fetching the next record before deserialization.
  if (corruptLastRecord) //初始值为false
      throw new KafkaException("Received exception when fetching the next record from " + partition + ". If needed, please seek past the record to continue consumption.", cachedRecordException);

  if (isConsumed) //CompleteFetch中的数据已被全部读取
      return Collections.emptyList();

  List<ConsumerRecord<K, V>> records = new ArrayList<>();
  try {
      for (int i = 0; i < maxRecords; i++) {
          if (cachedRecordException == null) {
              corruptLastRecord = true;
              lastRecord = nextFetchedRecord(); //读取一条消息
              corruptLastRecord = false;
          }
          if (lastRecord == null)
              //已读取完毕
              break;
          //完成消息反序列化并添加到结果集  
          records.add(parseRecord(partition, currentBatch, lastRecord));
          //更新已读取消息树
          recordsRead++;
          //已读取字节数
          bytesRead += lastRecord.sizeInBytes();
          nextFetchOffset = lastRecord.offset() + 1;
          cachedRecordException = null;
      }
  } catch (SerializationException se) {
      cachedRecordException = se;
      if (records.isEmpty())
          throw se;
  } catch (KafkaException e) {
      cachedRecordException = e;
      if (records.isEmpty())
          throw new KafkaException(...);
  }
  return records;
}
```

方法可分为三部分：

* 调用nextFetchedRecord()方法读取一条消息；
* 调用parseRecord()方法完成消息反序列化；
* 更新读取进度，如`recordsRead`、`bytesRead`、`nextFetchOffset`。

#### nextFetchedRecord

nextFetchedRecord()源码如下：

```
private Record nextFetchedRecord() {
    while (true) {
        if (records == null || !records.hasNext()) {
            maybeCloseRecordStream();
            if (!batches.hasNext()) {
                //读取结束
                if (currentBatch != null)
                    nextFetchOffset = currentBatch.nextOffset();
                //更新isConsumed    
                drain();
                return null;
            }
            
            currentBatch = batches.next();
            lastEpoch = currentBatch.partitionLeaderEpoch() == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
                    Optional.empty() : Optional.of(currentBatch.partitionLeaderEpoch());

            maybeEnsureValid(currentBatch);
            //事务消息处理 读已提交
            if (isolationLevel == IsolationLevel.READ_COMMITTED && currentBatch.hasProducerId()) {
                //统计中止事务的第一条事务消息的offset <= 当前offset的所有pid，放入abortedProducerIds
                consumeAbortedTransactionsUpTo(currentBatch.lastOffset());
                long producerId = currentBatch.producerId();
                if (containsAbortMarker(currentBatch)) {
                    //判断当前bath是否为ABORT，若是移除对应的PID，防止ControlBatch重试，导致本批次存在多余的ControlBatch
                    abortedProducerIds.remove(producerId);
                } else if (isBatchAborted(currentBatch)) {
                   // 本条消息为中止事务的消息，跳过 abortedProducerIds.contains(batch.producerId())
                   //Skipping aborted record batc
                    nextFetchOffset = currentBatch.nextOffset();
                    continue;
                }
            }
            //
            records = currentBatch.streamingIterator(decompressionBufferSupplier);
        } else {
            Record record = records.next();
            // skip any records out of range
            if (record.offset() >= nextFetchOffset) {
                // we only do validation when the message should not be skipped.
                maybeEnsureValid(record);

                // control records are not returned to the user
                if (!currentBatch.isControlBatch()) {
                    return record;
                } else {
                    // Increment the next fetch offset when we skip a control batch.
                    nextFetchOffset = record.offset() + 1;
                }
            }
        }
    }
}
```

可以看到，**事务消息是在KafkaConsumer本地进行处理的**，consumer会过滤所有的ControlBatch，以及若事务隔离级别为READ_COMMITTED，还需要根据FetchResponse返回的中止事务信息对普通消息进行过滤。

#### parseRecord

消息的反序列化比较简单，就是使用配置`key.deserializer`和`value.deserializer`指定的反序列化器完成。

```
private ConsumerRecord<K, V> parseRecord(TopicPartition partition, RecordBatch batch, Record record) {
    try {
        long offset = record.offset();
        long timestamp = record.timestamp();
        Optional<Integer> leaderEpoch = maybeLeaderEpoch(batch.partitionLeaderEpoch());
        TimestampType timestampType = batch.timestampType();
        Headers headers = new RecordHeaders(record.headers());
        ByteBuffer keyBytes = record.key();
        byte[] keyByteArray = keyBytes == null ? null : Utils.toArray(keyBytes);
        //Key反序列化
        K key = keyBytes == null ? null : this.keyDeserializer.deserialize(partition.topic(), headers, keyByteArray);
        ByteBuffer valueBytes = record.value();
        byte[] valueByteArray = valueBytes == null ? null : Utils.toArray(valueBytes);
        //value反序列化
        V value = valueBytes == null ? null : this.valueDeserializer.deserialize(partition.topic(), headers, valueByteArray);
        //消息封装
        return new ConsumerRecord<>(partition.topic(), partition.partition(), offset,
                                    timestamp, timestampType,
                                    keyByteArray == null ? ConsumerRecord.NULL_SIZE : keyByteArray.length,
                                    valueByteArray == null ? ConsumerRecord.NULL_SIZE : valueByteArray.length,
                                    key, value, headers, leaderEpoch);
    } catch (RuntimeException e) {
        //反序列化失败异常
        throw new RecordDeserializationException(...);
    }
}

```

