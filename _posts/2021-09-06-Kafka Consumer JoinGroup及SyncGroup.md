---
layout: post
title:  Kafka Consumer JoinGroup及SyncGroup
date:   2021-09-06 14:41:42
categories: Kafka
---

KafkaConsumer通过poll方法执行消息拉取，但poll方法内不仅是拉取消息，ConsumerJoinGroup、TopicPartition分配、ConsumerRebalance、心跳等逻辑的处理也均在poll方法内完成。这些逻辑涉及到
两个角色：ConsumerCoordinator与GroupCoordinator。


## GroupCoordinator简介

GroupCoordinator是Kafka Broker上的一个服务，每个Broker实例在运行时都会启动一个这样的服务。[KafkaConsumer概述](https://guann1ng.github.io/kafka/2021/09/02/Kafka-Consumer%E6%A6%82%E8%BF%B0/)中提到在Kafka Broker端有一个内部主题**`_consumer_offsets`**，负责存储每个ConsumerGroup的消费位移，
默认情况下该主题有50个partition，每个partition3个副本，Consumer通过groupId的hash值与`_consumer_offsets`的分区数取模得到对应的分区，如下：

```
def partitionFor(groupId: String): Int = Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount
```

获得对应的分区后，再寻找此分区的Leader副本所在的Broker节点，该Broker节点即为这个ConsumerGroup所对应的GroupCoordinator节点。

## KafkaConsumer#poll

poll方法内主要可分为以下几步：

* 通过记录当前线程id抢占锁，确保KafkaConsumer实例不会被多线程并发访问，保证线程安全。
* 调用`updateAssignmentMetadataIfNeeded()`方法完成消费者拉取消息前的元数据获取。
* 调用pollForFetches(timer)拉取消息。
* 将经过Consumer#Interceptors处理过后的消息返回给消费者或拉取超时返回空集合。最后释放锁。

其中第二步的方法主要内容为Consumer的JoinGroup及Rebalance。接下来我们以updateAssignmentMetadataIfNeeded方法为入口来分析KafkaConsumer如何实现JoinGroup及Rebalance。

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

## ConsumerCoordinator#poll

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

* 2、心跳任务，通过pollHeartbeat()唤醒心跳线程，发送心跳并记录pollTimer。

* 3、消费者入组及再平衡，我们通过以下几个阶段来进行分析：


### 1、FIND_COORDINATOR

ensureCoordinatorReady()方法的作用是向LeastLoadNode(inFlightRequests.size最小)发送FindCoordinatorRequest，查找GroupCoordinator所在的Broker，
并在请求回调方法中建立连接。


![Find GroupCoordinator](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/find%20groupcoordinator.png)


#### 1.1、SendFindCoordinatorRequest

请求发送的方法调用流程为：ensureCoordinatorReady() –> lookupCoordinator() –> sendFindCoordinatorRequest()，这一步完成请求的发送及回调函数注册。

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

private RequestFuture<Void> sendFindCoordinatorRequest(Node node) {
    FindCoordinatorRequestData data = new FindCoordinatorRequestData().setKeyType(CoordinatorType.GROUP.id()).setKey(this.rebalanceConfig.groupId);
    FindCoordinatorRequest.Builder requestBuilder = new FindCoordinatorRequest.Builder(data);
    return client.send(node, requestBuilder).compose(new FindCoordinatorResponseHandler());
}

```

#### 1.2、handleFindCoordinatorRequest

Broker端处理请求方法入口为handleFindCoordinatorRequest()，其中的业务逻辑可分为3步：

* 获取groupId所属的_consumer_offsets分区，Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount 
* 获取_consumer_offsets所有的分区元数据
* 从所有的分区元数据中过滤出第一步计算出的分区Leader副本节点信息返回

为节省篇幅，这里不再将代码全部贴出，我们主要看下核心方法getCoordinator()的实现。

```
private def getCoordinator(request: RequestChannel.Request, keyType: Byte, key: String): (Errors, Node) = {
    if (校验及认证...)
        //失败... 返回异常
    else {
      val (partition, internalTopicName) = CoordinatorType.forId(keyType) match {
        //GroupCoordinator
        case CoordinatorType.GROUP =>
        //计算分区 Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount 
        (groupCoordinator.partitionFor(key), GROUP_METADATA_TOPIC_NAME)
        //TransactionCoordinator  事务相关，具体见Kafka Producer 幂等与事务一文
        case CoordinatorType.TRANSACTION =>
          (txnCoordinator.partitionFor(key), TRANSACTION_STATE_TOPIC_NAME)
      }
      //获取_consumer_offsets所有分区的元数据信息  
      val topicMetadata = metadataCache.getTopicMetadata(Set(internalTopicName), request.context.listenerName)

      if (topicMetadata.headOption.isEmpty) {
        val controllerMutationQuota = quotas.controllerMutation.newPermissiveQuotaFor(request)
        autoTopicCreationManager.createTopics(Seq(internalTopicName).toSet, controllerMutationQuota, None)
        (Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
      } else {
        if (topicMetadata.head.errorCode != Errors.NONE.code) {
          (Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
        } else {
          //获取Leader副本节点返回
          val coordinatorEndpoint = topicMetadata.head.partitions.asScala
            .find(_.partitionIndex == partition)
            .filter(_.leaderId != MetadataResponse.NO_LEADER_ID)
            .flatMap(metadata => metadataCache.
                getAliveBrokerNode(metadata.leaderId, request.context.listenerName))
          //返回数据
          coordinatorEndpoint match {
            case Some(endpoint) =>
              (Errors.NONE, endpoint)
            case _ =>
              (Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
          }
        }
      }
    }
  }
```

### 2、JOIN_GROUP

成功找到GroupCoordinator后，Consumer进入JoinGroup阶段，此阶段的Consumer会向GroupCoordinator发送JoinGroupRequest请求，GroupCoordinator会确认ConsumerGroup的Leader及分区分配策略，并响应给
消费者。

![JOIN GROUP](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/join%20group.png)

入口方法为`AbstractCoordinator#ensureActiveGroup()`，实现如下：

```
boolean ensureActiveGroup(final Timer timer) {
    //确保groupCoordinator连接正常
    if (!ensureCoordinatorReady(timer)) { return false; }
    //启动HeartBeatThread(只是启动线程，满足其他条件后才会发送心跳)
    startHeartbeatThreadIfNeeded(); 
    return joinGroupIfNeeded(timer); // 发送joingroup请求
}
```

#### 2.1、SendJoinGroupRequest

继续看joinGroupIfNeeded()方法，通过代码可知发送JoinGroupRequest请求是在initiateJoinGroup()方法中实现的。

```
boolean joinGroupIfNeeded(final Timer timer) {
    while (rejoinNeededOrPending()) {
        if (!ensureCoordinatorReady(timer)) {
            return false;
        }
        if (needsJoinPrepare) {
            //首次为true
            needsJoinPrepare = false;
            //触发ConsumerRebalanceListener，如果自动提交为true，尝试提交
            onJoinPrepare(generation.generationId, generation.memberId);
        }
        //发送JoinGroupRequest请求
        final RequestFuture<ByteBuffer> future = initiateJoinGroup();
        client.poll(future, timer);
        if (!future.isDone()) {
            //超时
            return false;
        }
        if (future.succeeded()) {
            Generation generationSnapshot;
            MemberState stateSnapshot;
            
            synchronized (AbstractCoordinator.this) {
                generationSnapshot = this.generation;
                stateSnapshot = this.state;
            }

            if (!generationSnapshot.equals(Generation.NO_GENERATION) && stateSnapshot == MemberState.STABLE) {
                // Duplicate the buffer in case `onJoinComplete` does not complete and needs to be retried.
                ByteBuffer memberAssignment = future.value().duplicate();
                //此时JoinGroup及SyncGroup已经成功。更新订阅的TopicPartition及对应的metadata
                //执行回调 PartitionAssignor#onAssignment, ConsumerRebalanceListener#onPartitionsAssigned
                onJoinComplete(generationSnapshot.generationId, generationSnapshot.memberId, generationSnapshot.protocolName, memberAssignment);
                resetJoinGroupFuture();
                needsJoinPrepare = true;
            } else {
                final String reason = String.format("rebalance failed since the generation/state was " + "modified by heartbeat thread to %s/%s before the rebalance callback triggered",generationSnapshot, stateSnapshot);
                resetStateAndRejoin(reason); //重试
                resetJoinGroupFuture(); 
            }
        } else {
            final RuntimeException exception = future.exception();
            resetJoinGroupFuture();
            if (可重试异常)
                continue;
            else if (!future.isRetriable())
                throw exception;
            //重试的backoff
            resetStateAndRejoin(String.format("rebalance failed with retriable error %s", exception));
            timer.sleep(rebalanceConfig.retryBackoffMs);
        }
    }
    return true;
}
```
initiateJoinGroup()方法中又调用了sendJoinGroupRequest()方法完成JoinGroupRequest请求的发送，这里的代码比较简单，其中参数rebalanceTimeoutMs的值为max.poll.interval.ms，
而joinGroupTimeoutMs为max.poll.interval.ms加5s。protocolType为"consumer"，generation.memberId初始值为`""`空字符串。

```
private synchronized RequestFuture<ByteBuffer> initiateJoinGroup() {
    if (joinFuture == null) {
        state = MemberState.PREPARING_REBALANCE;
        if (lastRebalanceStartMs == -1L)
            lastRebalanceStartMs = time.milliseconds();
        joinFuture = sendJoinGroupRequest();
        joinFuture.addListener(new RequestFutureListener<ByteBuffer>() {
            @Override
            public void onSuccess(ByteBuffer value) {
                // do nothing since all the handler logic are in SyncGroupResponseHandler already
            }
            @Override
            public void onFailure(RuntimeException e) {
                synchronized (AbstractCoordinator.this) {
                    sensors.failedRebalanceSensor.record();
                }
            }
        });
    }
    return joinFuture;
}

RequestFuture<ByteBuffer> sendJoinGroupRequest() {
    if (coordinatorUnknown())
        return org.apache.kafka.clients.consumer.internals.RequestFuture.coordinatorNotAvailable();
    log.info("(Re-)joining group");
    JoinGroupRequest.Builder requestBuilder = new JoinGroupRequest.Builder(
            new JoinGroupRequestData()
                    .setGroupId(rebalanceConfig.groupId)
                    .setSessionTimeoutMs(this.rebalanceConfig.sessionTimeoutMs)
                    .setMemberId(generation.memberId)
                    .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                    .setProtocolType(protocolType())
                    .setProtocols(metadata())
                    .setRebalanceTimeoutMs(this.rebalanceConfig.rebalanceTimeoutMs)
    );
    log.debug("Sending JoinGroup ({}) to coordinator {}", requestBuilder, this.coordinator);
    int joinGroupTimeoutMs = Math.max(client.defaultRequestTimeoutMs(),Math.max(rebalanceConfig.rebalanceTimeoutMs + JOIN_GROUP_TIMEOUT_LAPSE,rebalanceConfig.rebalanceTimeoutMs) );
    return client.send(coordinator, requestBuilder, joinGroupTimeoutMs).compose(new JoinGroupResponseHandler(generation));
}

```

#### 2.2、HandleJoinGroupRequest

JoinGroupRequest由对应的GroupCoordinator所在的broker处理，此阶段的入口方法为handleJoinGroupRequest，这里没有贴出，我们主要关注它的下级方法handleJoinGroup的实现，handleJoinGroup方法的实现如下，GroupCoordinator
通过`groupMetadataCache = new Pool[String, GroupMetadata]`缓存所有groupId与GroupMetadata的对应关系，若groupId对应的GroupMetadata为空，就新建一个放入缓存。
后续根据memberId判断是执行doNewMemberJoinGroup()或doCurrentMemberJoinGroup()。

```
def handleJoinGroup(...): Unit = {
    validateGroupStatus(groupId, ApiKeys.JOIN_GROUP).foreach { error =>
      responseCallback(JoinGroupResult(memberId, error))
      return
    }
    // sessionTimeoutMs默认为即6s-5min之间，可通过Broker端的参数`group.min.session.timeout.ms`和`group.min.session.timeout.ms`配置
    if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
      sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) {
      responseCallback(JoinGroupResult(memberId, Errors.INVALID_SESSION_TIMEOUT))
    } else {
      val isUnknownMember = memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID
      //第一个组消费者请求时，memberId=""，group不存在，此时会创建一个新的GroupMetadata
      groupManager.getOrMaybeCreateGroup(groupId, isUnknownMember) match {
        case None =>
           // memberId不为空，groupMetadate为空，请求错误
          responseCallback(JoinGroupResult(memberId, Errors.UNKNOWN_MEMBER_ID))
        case Some(group) =>
          group.inLock {
            //判断当前组是否可以加入，容量状态等因素
            if (!acceptJoiningMember(group, memberId)) {
              //移除
              group.remove(memberId)
              responseCallback(JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.GROUP_MAX_SIZE_REACHED))
            } else if (isUnknownMember) {
                //新的消费者入组
                doNewMemberJoinGroup(...)
            } else {
              //重新加入
              doCurrentMemberJoinGroup(...)
            }
            if (group.is(PreparingRebalance)) {
              rebalancePurgatory.checkAndComplete(GroupJoinKey(group.groupId))
            }
          }
      }
    }
  }
```

doNewMemberJoinGroup或doCurrentMemberJoinGroup均调用addMemberAndRebalance方法完成新消费者的加入。此时GroupCoordinator会为memberId为空的消费者自动生成
id，方法如下：

```
clientId + GroupMetadata.MemberIdDelimiter + UUID.randomUUID().toString
```
下面来继续介绍addMemberAndRebalance方法：

```
private def addMemberAndRebalance(...): Unit = {
    //为新的消费者创建元数据
    val member = new MemberMetadata(memberId, groupInstanceId, clientId, clientHost,
      rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols)
    ...
    group.add(member, callback)
    ...

    maybePrepareRebalance(group, s"Adding new member $memberId with group instance id $groupInstanceId")
}
```

主要是两个方法的调用：add方法和maybePrepareRebalance方法，接下来通过3点内容来分析这两个方法的内容：

##### 选举消费者组的Leader

GroupCoordinator中消费者的信息是以HashMap的形式存储的，其中key为消费者的memberId，而value是消费者相关的元数据信息，add方法只是将新消费者加入members这个Map中。
这里主要关注leaderId的赋值，如果消费组内还没有leader，那么第一个加入消费组的消费者即为消费组的leader。

```
def add(member: MemberMetadata, callback: JoinCallback = null): Unit = {
    ...
    if (leaderId.isEmpty)
      leaderId = Some(member.memberId)
    members.put(member.memberId, member)
    ...
}
```

补充：如果后续leaderId对应的消息者发生离组，新的leaderId为members Map中的第一个键值对的key。

```
def remove(memberId: String): Unit = {
    ...
    if (isLeader(memberId))
      leaderId = members.keys.headOption
    ...
}
```

##### 延迟Join

maybePrepareRebalance方法中通过判断当前group状态若是Stable、CompletingRebalance、Empty其中之一，即可调用prepareRebalance方法，进行Rebalance。
prepareRebalance方法中并不是直接触发再平衡的，**为了避免多个consumer短时间内均发起JoinGroup请求（如应用启动时），导致频繁的rebalance**，这里Kafka通过DelayedJoin来进行优化：

* 当ConsumerGroup为空时，即第一个消费者加入，创建InitialDelayedJoin，等待时长为group.initial.rebalance.delay.ms；
* 后续消费者加入时，创建DelayedJoin，等待时长rebalanceTimeoutMs的值为max.poll.interval.ms

同时group的状态也会转为PreparingRebalance。

```
private def maybePrepareRebalance(group: GroupMetadata, reason: String): Unit = {
    group.inLock {
      if (group.canRebalance)
        prepareRebalance(group, reason)
    }
  }

private[group] def prepareRebalance(group: GroupMetadata, reason: String): Unit = {
if (group.is(CompletingRebalance))
  resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)

 removeSyncExpiration(group)
//当consumerGroup为空时，InitialDelayedJoin
val delayedRebalance = if (group.is(Empty))
  new InitialDelayedJoin(this,
    rebalancePurgatory,
    group,
    groupConfig.groupInitialRebalanceDelayMs,
    groupConfig.groupInitialRebalanceDelayMs,
    max(group.rebalanceTimeoutMs - groupConfig.groupInitialRebalanceDelayMs, 0))
else
  new DelayedJoin(this, group, group.rebalanceTimeoutMs)
  // 状态转变为PreparingRebalance
  group.transitionTo(PreparingRebalance)

  val groupKey = GroupJoinKey(group.groupId)
  rebalancePurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
}
```


InitialDelayedJoin的父类是DelayedJoin，DelayedJoin的onComplete会调用GroupCoordinator的onCompleteJoin方法响应请求。

```
private[group] class InitialDelayedJoin(...) extends DelayedJoin(）

rivate[group] class DelayedJoin(...) extends DelayedRebalance(...) {
  override def tryComplete(): Boolean = coordinator.tryCompleteJoin(group, forceComplete _)

  override def onExpiration(): Unit = {
    tryToCompleteDelayedAction()
  }
  override def onComplete(): Unit = coordinator.onCompleteJoin(group)
  private def tryToCompleteDelayedAction(): Unit = coordinator.groupManager.replicaManager.tryCompleteActions()
}
```

##### 分区分配策略选举及请求响应

onCompleteJoin中主要完成了两部分工作：

* 选举分区分配策略
* 对延时期间发生JoinGroup请求的Consumer做出响应，这里**返回给leader角色consumer的数据与普通consumer不同，leader consumer还是获取到currentMemberMetadata(所有组成员的元信息)**。

```
def onCompleteJoin(group: GroupMetadata): Unit = {
    group.inLock {
      val notYetRejoinedDynamicMembers = group.notYetRejoinedMembers.filterNot(_._2.isStaticMember)
      if (notYetRejoinedDynamicMembers.nonEmpty) {
        //移除尚未加入组  及 心跳超时的 consumer 
        notYetRejoinedDynamicMembers.values.foreach { failedMember =>
          group.remove(failedMember.memberId)
          removeHeartbeatForLeavingMember(group, failedMember.memberId)
        }
      }
          ...
          选举分区分配策略
          group.initNextGeneration()
          ...
          
          for (member <- group.allMemberMetadata) {
            val joinResult = JoinGroupResult(
              //只有leader consumer的响应有consumerGroup的元数据
              members = if (group.isLeader(member.memberId)) {
                group.currentMemberMetadata
              } else {
                List.empty
              },
              memberId = member.memberId,
              generationId = group.generationId,
              protocolType = group.protocolType,
              protocolName = group.protocolName,
              leaderId = group.leaderOrNull,
              error = Errors.NONE)
              ...
  }

```

initNextGeneration方法：

* 调用selectProtocol完成分区分配策略的选举。因为每个consumer的分配策略可能不一样，这里需要投票选举一个PartitionAssignor
* 组状态由PreparingRebalance转变为了CompletingRebalance，也就是所有消费者都进入组内了，等待GroupCoordinator分配分区

```
   def initNextGeneration() = {
       assert(notYetRejoinedMembers == List.empty[MemberMetadata])
       if (members.nonEmpty) {
         generationId += 1
         protocol = Some(selectProtocol) //分区策略选举
         transitionTo(CompletingRebalance)  //group状态转移
       } else {
         generationId += 1
         protocol = None
         transitionTo(Empty)
       }
       receivedConsumerOffsetCommits = false
       receivedTransactionalOffsetCommits = false
   } 
 
  def selectProtocol: String = {
    if (members.isEmpty)
      throw new IllegalStateException("Cannot select protocol for empty group")
    val candidates = candidateProtocols
    val (protocol, _) = allMemberMetadata
      .map(_.vote(candidates))
      .groupBy(identity)
      .maxBy { case (_, votes) => votes.size }
    protocol
  }

```

#### 2.3、JoinGroupResponseHandler

Consumer在sendJoinGroupRequest方法中，除了发送请求，还定义了响应的处理器JoinGroupResponseHandler，方法中根据broker返回的leader memberId判断，如果当前consumer就是leader，
调用onJoinLeader，否则调用onJoinFollower。

```
private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer> {
        
        @Override
        public void handle(JoinGroupResponse joinResponse, org.apache.kafka.clients.consumer.internals.RequestFuture<ByteBuffer> future) {
            Errors error = joinResponse.error();
            if (error == Errors.NONE) {
                ... //校验内容省略
                synchronized (AbstractCoordinator.this) {
                    if (state != MemberState.PREPARING_REBALANCE) {
                        future.raise(new UnjoinedGroupException());
                    } else {
                        state = MemberState.COMPLETING_REBALANCE;
                        if (heartbeatThread != null)
                            heartbeatThread.enable(); //开启心跳
                        ////初始化了generation，版本号
                        AbstractCoordinator.this.generation = new Generation(
                            joinResponse.data().generationId(),
                            joinResponse.data().memberId(), joinResponse.data().protocolName());
                        if (joinResponse.isLeader()) {
                            onJoinLeader(joinResponse).chain(future);
                        } else {
                            onJoinFollower().chain(future);
                        }
                    }
                }
            }
        }else{
         ...  //不同错误处理 省略
        }
    }
```

onJoinLeader与OnJoinFollower方法都是发送了一个SyncGroupRequest请求，唯一的区别是，onJoinLeader会计算分配方案，传给SyncGroupRequest请求，而onJoinFollower传入的是一个emptyMap。
sendSyncGroupRequest分析见下一节。

```
private RequestFuture<ByteBuffer> onJoinFollower() {
    SyncGroupRequest.Builder requestBuilder =new SyncGroupRequest.Builder(...);
    return sendSyncGroupRequest(requestBuilder);
}

private RequestFuture<ByteBuffer> onJoinLeader(JoinGroupResponse joinResponse) {
    try {
        //计算分区分配
        Map<String, ByteBuffer> groupAssignment = performAssignment(joinResponse.data().leader(), joinResponse.data().protocolName(),
                joinResponse.data().members());

        List<SyncGroupRequestData.SyncGroupRequestAssignment> groupAssignmentList = new ArrayList<>();
        for (Map.Entry<String, ByteBuffer> assignment : groupAssignment.entrySet()) {
            groupAssignmentList.add(new SyncGroupRequestData.SyncGroupRequestAssignment()
                    .setMemberId(assignment.getKey())
                    .setAssignment(Utils.toArray(assignment.getValue()))
            );
        }
        SyncGroupRequest.Builder requestBuilder = new SyncGroupRequest.Builder(...);
        //发送SyncGroupRequest
        return sendSyncGroupRequest(requestBuilder);
    } catch (RuntimeException e) {
        return oRequestFuture.failure(e);
    }
}

```

### 3、SYNC_GROUP

上一阶段JOIN_GROUP阶段的最后，leader consumer会根据GroupCoordinator返回的分区分配策略及member metadata完成具体的分区分配。如何将分区分配的结果同步给其它consumer，这里Kafka并没有
让leader consumer直接将分配结果同步给其它消费者，而是通过GroupCoordinator来实现中转，减少复杂性。此阶段即SYNC_GROUP阶段，**各个消费者会向GroupCoordinator发送SyncGroupRequest请求来同步分配方案**。

![SYNC GROUP](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/sync%20group.png)

#### 3.1 sendSyncGroupRequest

Consumer端发送SyncGroupRequest请求的方法如下，发送时会注册一个回调函数SyncGroupResponseHandler。

```
private RequestFuture<ByteBuffer> sendSyncGroupRequest(SyncGroupRequest.Builder requestBuilder) {
    if (coordinatorUnknown())
        return RequestFuture.coordinatorNotAvailable();
    return client.send(coordinator, requestBuilder).compose(new SyncGroupResponseHandler(generation));
}
```

#### 3.2 handleSyncGroupRequest

GroupCoordinator处理SyncGroupRequest的入口方法为KafkaApis#handleSyncGroupRequest，调用链为KafkaApis#handleSyncGroupRequest->GroupCoordinator#handleSyncGroup
->GroupCoordinator#doSyncGroup，这里主要关注doSyncGroup方法：

```
  private def doSyncGroup(...): Unit = {
    group.inLock {
      val validationErrorOpt = validateSyncGroup(group,generationId,memberId,protocolType,protocolName,groupInstanceId)

      validationErrorOpt match {
          //省略其他case joingroup阶段后消费者组状态为CompletingRebalance
          case Empty | Dead => // 省略 ...
          case PreparingRebalance => // 省略 ...
          case CompletingRebalance =>
            group.get(memberId).awaitingSyncCallback = responseCallback
            removePendingSyncMember(group, memberId)
            //只处理leader consumer
            if (group.isLeader(memberId)) {

              val missing = group.allMembers.diff(groupAssignment.keySet)
              val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

              if (missing.nonEmpty) {
                warn(s"Setting empty assignments for members $missing of ${group.groupId} for generation ${group.generationId}")
              }
              //分区方案持久化保存到_consumer_offset
              groupManager.storeGroup(group, assignment, (error: Errors) => {
                group.inLock {
                  if (group.is(CompletingRebalance) && generationId == group.generationId) {
                    if (error != Errors.NONE) {
                      resetAndPropagateAssignmentError(group, error)
                      maybePrepareRebalance(group, s"Error when storing group assignment during SyncGroup (member: $memberId)")
                    } else {
                       //业务处理 
                      setAndPropagateAssignment(group, assignment)
                      // 组状态变为Stable
                      group.transitionTo(Stable)
                    }
                  }
                }
              }, requestLocal)
              groupCompletedRebalanceSensor.record()
            }
        }
      }
    }
  }

```

doSyncGroup方法只处理leader consumer的SyncGroupRequest，并将元数据存入了_consumer_offsets中，之后的业务处理在setAndPropagateAssignment方法，处理完成后将组状态转换为Stable。
propagateAssignment方法调用回调方法，响应每个consumer，响应的内容是每个consumer的assignment(分配方案)，并在之后开始执行定时任务监控member的心跳.


```
  private def setAndPropagateAssignment(group: GroupMetadata, assignment: Map[String, Array[Byte]]): Unit = {
    assert(group.is(CompletingRebalance)) //校验状态
    //将每个member的分配方案保存到了allMemberMetadata
    group.allMemberMetadata.foreach(member => member.assignment = assignment(member.memberId))
    propagateAssignment(group, Errors.NONE)
  }
 
  private def propagateAssignment(group: GroupMetadata, error: Errors): Unit = {
    val (protocolType, protocolName) = if (error == Errors.NONE)
      (group.protocolType, group.protocolName)
    else
      (None, None)
    for (member <- group.allMemberMetadata) {
      if (member.assignment.isEmpty && error == Errors.NONE) {
        warn(s"Sending empty assignment to member ${member.memberId} of ${group.groupId} for generation ${group.generationId} with no errors")
      }
      if (group.maybeInvokeSyncCallback(member, SyncGroupResult(protocolType, protocolName, member.assignment, error))) {
        completeAndScheduleNextHeartbeatExpiration(group, member)
      }
    }
  }  

```

#### 3.3 SyncGroupResponseHandler

收到SyncGroupResponse由SyncGroupResponseHandler进行处理，并调用ConsumerCoordinator#onJoinComplete完成元数据及分区信息更新，并触发rebalanceListener。

```
protected void onJoinComplete(int generation,String memberId,String assignmentStrategy,ByteBuffer assignmentBuffer) {
    
    // Only the leader is responsible for monitoring for metadata changes (i.e. partition changes)
    if (!isLeader)
        assignmentSnapshot = null;

    ConsumerPartitionAssignor assignor = lookupAssignor(assignmentStrategy);
    if (assignor == null)
        throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);
    groupMetadata = new ConsumerGroupMetadata(rebalanceConfig.groupId, generation, memberId, rebalanceConfig.groupInstanceId);
    Set<TopicPartition> ownedPartitions = new HashSet<>(subscriptions.assignedPartitions());
    if (assignmentBuffer.remaining() < 2)
        throw new IllegalStateException();

    Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);

    Set<TopicPartition> assignedPartitions = new HashSet<>(assignment.partitions());
    
    if (!subscriptions.checkAssignmentMatchedSubscription(assignedPartitions)) {
        //获取的主题分区与订阅不一致
        final String reason = "";
        requestRejoin(reason);
        return;
    }

    final AtomicReference<Exception> firstException = new AtomicReference<>(null);
    Set<TopicPartition> addedPartitions = new HashSet<>(assignedPartitions);
    addedPartitions.removeAll(ownedPartitions);

    if (protocol == RebalanceProtocol.COOPERATIVE) {
        Set<TopicPartition> revokedPartitions = new HashSet<>(ownedPartitions);
        revokedPartitions.removeAll(assignedPartitions);


        if (!revokedPartitions.isEmpty()) {
            firstException.compareAndSet(null, invokePartitionsRevoked(revokedPartitions));
            final String reason = 
            requestRejoin(reason);
        }
    }
    //正则模式订阅主题，判断是否有新的主题
    maybeUpdateJoinedSubscription(assignedPartitions);

    firstException.compareAndSet(null, invokeOnAssignment(assignor, assignment));
    //充值自动提交时间
    if (autoCommitEnabled)
        this.nextAutoCommitTimer.updateAndReset(autoCommitIntervalMs);
    //设置订阅的主题分区列表
    subscriptions.assignFromSubscribed(assignedPartitions);
    //触发ConsumerRebalanceListener
    firstException.compareAndSet(null, invokePartitionsAssigned(addedPartitions));

    if (firstException.get() != null) {
        if (firstException.get() instanceof KafkaException) {
            throw (KafkaException) firstException.get();
        } else {
            throw new KafkaException("User rebalance callback throws an error", firstException.get());
        }
    }
}
```

