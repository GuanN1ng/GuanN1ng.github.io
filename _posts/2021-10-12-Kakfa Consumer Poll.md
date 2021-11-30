---
layout: post
title:  Kafka Poll
date:   2021-10-12 17:18:31
categories: Kafka
---

前面几篇内容分析了`updateAssignmentMetadataIfNeeded()`的执行流程，包含两部分内容：

* ConsumerCoordinator#poll方法，获取GroupCoordinator，完成JoinGroup及主题分区方案获取，详情见[Kafka Consumer JoinGroup](https://guann1ng.github.io/kafka/2021/09/06/Kafka-Consumer-JoinGroup/)；
* KafkaConsumer#updateFetchPositions方法，更新consumer订阅的TopicPartition的有效offset，确认下次消息拉取的偏移量(offset)，详情见[Kafka Consumer UpdateFetchPosition](https://guann1ng.github.io/kafka/2021/09/17/Kafka-Consumer-UpdateFetchPosition/)。

下面继续分析KafkaConsumer#poll方法的后半部分内容，即消息拉取部分：。

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

### pollForFetches

pollForFetches方法的源码如下，方法可分为三部分：

* 发送消息拉取请求的**Fetcher#sendFetches()**；
* 触发网络读写时间的**ConsumerNetworkClient#poll()**，底层为Java NIO;
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

### sendFetches

sendFetches方法的作用是向consumer订阅的所有可发送的TopicPartition发送FetchRequest拉取消息，可分为以下三步：

* 1、prepareFetchRequests()方法中获取所有可发送FetchRequest的分区Broker。**可进行消息拉取的分区有以下三点要求**：
    * TopicPartition对应的分区副本Leader有效(连接正常)；
    * Leader所在节点没有待发送或挂起的请求，避免请求积压；
    * TopicPartition之前的拉取响应数据已全部处理(completedFetches中不存在对应的分区数据)。
* 2、遍历第一步返回的Broker节点列表，构建FetchRequest，并调用NetworkClient发送；
* 3、为请求Future对象设置响应处理的Listener。


```
public synchronized int sendFetches() {
    sensors.maybeUpdateAssignment(subscriptions);
    //获取可发送的Broker
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

#### sendFetchRequest

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

#### handleFetchRequest

Broker端处理FetchRequest的入口为KafkaApis#handleFetchRequest方法，以下为完成的方法调用流程：

* KafkaApis#handleFetchRequest
* ReplicaManager#fetchMessages
* Partition#readRecords
* Log#read
* LogSegment#read

这部分内容放在后面Broker端处理FetchRequest时再详细分析。


#### RequestFutureListener

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

### CompletedFetch

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


### fetchedRecords

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

#### initializeCompletedFetch

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

#### fetchRecords

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
