---
layout: post
title:  Kafka Consumer JoinGroup及Rebalance
date:   2021-09-06 14:41:42
categories: Kafka
---

KafkaConsumer通过poll方法执行消息拉取，但poll方法内不仅是拉取消息，还包括消费位移、消费者协调器、组协调器、消费者选举、TopicPartition分配、ConsumerRebalance、心跳等逻辑的处理。
本篇主要关注consumer与协调器之间的逻辑，如JoinGroup及Rebalance的流程。

### KafkaConsumer#poll

poll方法内主要可分为以下几步：

* 通过记录当前线程id抢占锁，确保KafkaConsumer实例不会被多线程并发访问，保证线程安全。

* 调用`updateAssignmentMetadataIfNeeded()`方法完成消费者拉取消息前的元数据获取。

* 调用pollForFetches(timer)拉取消息。

* 将经过ConsumerInterceptors处理过后的消息返回给消费者或拉取超时返回空集合。最后释放锁。

```
private ConsumerRecords<K, V> poll(final Timer timer, final boolean includeMetadataInTimeout) {
     //consumer不是线程安全的， CAS设置当前threadId获取锁，并确认consumer未关闭
    acquireAndEnsureOpen();
    try {
        //记录开始时间，用于超时返回
        this.kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());
        if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
            throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
        }
        //do-while 超时判断
        do {
            //是否有线程调用wakeup方法，若有则抛出异常WakeupException，退出poll方法。
            client.maybeTriggerWakeup();
            //includeMetadataInTimeout 拉取消息的超时时间是否包含更新元数据的时间
            if (includeMetadataInTimeout) {
                updateAssignmentMetadataIfNeeded(timer, false);
            } else {
                //更新元数据请求
                while (!updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE), true)) {
                    log.warn("Still waiting for metadata");
                }
            }
            //拉取消息
            Map<TopicPartition, List<ConsumerRecord<K, V>>> records = pollForFetches(timer);
            if (!records.isEmpty()) {
                //如果拉取到的消息集合不为空，再返回该批消息之前，如果还有挤压的拉取请求，可以继续发送拉取请求，
                //但此时会禁用warkup，主要的目的是用户在处理消息时，KafkaConsumer还可以继续向broker拉取消息。
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

### ConsumerCoordinator#poll

KafkaConsumer的poll方法内调用updateAssignmentMetadataIfNeeded完成消息拉取前的元数据获取，updateAssignmentMetadataIfNeeded方法非常简单，主要是两个方法的调用：

* ConsumerCoordinator#poll：JoinGroup及Rebalance的核心方法。

* updateFetchPositions：更新消费进度

```
boolean updateAssignmentMetadataIfNeeded(final Timer timer, final boolean waitForJoinGroup) {
    if (coordinator != null && !coordinator.poll(timer, waitForJoinGroup)) {
        return false;
    }
    return updateFetchPositions(timer);
}
```

本篇主要关注ConsumerCoordinator#poll方法的实现：

```
public boolean poll(Timer timer, boolean waitForJoinGroup) {
    //获取最新的元数据信息
    maybeUpdateSubscriptionMetadata();
    //执行队列中位移提交的回调任务
    invokeCompletedOffsetCommitCallbacks();
    //判断是否采用自动分区策略 即采用subscribe方法订阅主题，而非assign手动指定主题分区
    if (subscriptions.hasAutoAssignedPartitions()) {
        if (protocol == null) {
            throw new IllegalStateException("User configured " + ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG + " to empty while trying to subscribe for group protocol to auto assign partitions");
        }
        //根据heartbeatIntervalMs配置项判断是否发送心跳并更新发送时间
        pollHeartbeat(timer.currentTimeMs());
        //获取GroupCoordinator
        if (coordinatorUnknown() && !ensureCoordinatorReady(timer)) {
            return false;
        }
        if (rejoinNeededOrPending()) {  
            //通过pattern 正则订阅
            if (subscriptions.hasPatternSubscription()) {
                ...
            }
            //通过JoinGroup和SyncGroup进行rebalance，来保证达到STABLE状态
            if (!ensureActiveGroup(waitForJoinGroup ? timer : time.timer(0L))) {
                timer.update(time.milliseconds());
                return false;
            }
        }
    } else {
        //通过assign方式制定消费分区，不存在组和再平衡
        if (metadata.updateRequested() && !client.hasReadyNodes(timer.currentTimeMs())) {
            client.awaitMetadataUpdate(timer);
        }
    }
    //enable.auto.commit=true 尝试消费位移自动提交
    maybeAutoCommitOffsetsAsync(timer.currentTimeMs());
    return true;
}

```

其中主要包含以下部分内容：

* 1、消费位移自动提交及提交完成的回调处理，方法invokeCompletedOffsetCommitCallbacks()及maybeAutoCommitOffsetsAsync()。

* 2、心跳任务，通过pollHeartbeat()唤醒心跳线程，发送心跳并记录pollTimer。HeartbeatThread#run()方法会根据maxPollIntervalMs判断是否需要发送LeaveGroupRequest（主动触发rebalance）。

* 3、消费者入组及再平衡，我们通过以下几个阶段来进行分析：


#### 1、FIND_COORDINATOR

ensureCoordinatorReady()方法的作用是向LeastLoadNode(inFlightRequests.size最小)发送FindCoordinatorRequest，查找GroupCoordinator所在的Broker，并建立连接。
方法的调用流程为：ensureCoordinatorReady() –> lookupCoordinator() –> sendGroupCoordinatorRequest()

```
protected synchronized boolean ensureCoordinatorReady(final Timer timer) {
    if (!coordinatorUnknown())
        return true;
    //查找GroupCoordinator直至可用或超时
    do {
        ...
        final RequestFuture<Void> future = lookupCoordinator();
        client.poll(future, timer);
        if (!future.isDone()) { 
            break;
        }
        //异常及重试处理
        if (future.failed()) {
            ...   
        } else if (coordinator != null && client.isUnavailable(coordinator)) {
            ...
        }
    } while (coordinatorUnknown() && timer.notExpired());
    return !coordinatorUnknown();
}

protected synchronized RequestFuture<Void> lookupCoordinator() {
    if (findCoordinatorFuture == null) {
        //负载最小节点
        Node node = this.client.leastLoadedNode();
        if (node == null) {
            log.debug("No broker available to send FindCoordinator request");
            return RequestFuture.noBrokersAvailable();
        } else {
            findCoordinatorFuture = sendFindCoordinatorRequest(node);
        }
    }
    return findCoordinatorFuture;
}

private org.apache.kafka.clients.consumer.internals.RequestFuture<Void> sendFindCoordinatorRequest(Node node) {
    FindCoordinatorRequestData data = new FindCoordinatorRequestData().setKeyType(CoordinatorType.GROUP.id()).setKey(this.rebalanceConfig.groupId);
    FindCoordinatorRequest.Builder requestBuilder = new FindCoordinatorRequest.Builder(data);
    return client.send(node, requestBuilder).compose(new FindCoordinatorResponseHandler());
}

```



#### 2、JOIN_GROUP





#### 3、SYNC_GROUP



#### 4、HEARTBEAT