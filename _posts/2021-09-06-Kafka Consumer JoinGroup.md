---
layout: post
title:  Kafka Consumer JoinGroup
date:   2021-09-06 14:41:42
categories: Kafka
---

[KafkaConsumer 概述](https://guann1ng.github.io/kafka/2021/09/02/Kafka-Consumer%E6%A6%82%E8%BF%B0/)中介绍了消费者组及消费位移的概念以及分区分配的算法，KafkaConsumer需要完成JoinGroup、TopicPartition分区分配等工作，才可进行消息的拉取及消费，那么组内不同消费者间
是如何相互协同完成这些工作的呢？这一切都在两个角色：ConsumerCoordinator和GroupCoordinator间完成。下面将从源码来分析ConsumerCoordinator和GroupCoordinator的作用，以及一个KafkaConsumer如何完成JoinGroup。


## GroupCoordinator简介

GroupCoordinator是Kafka Server端的一个实例对象，该类的注释为：
```
GroupCoordinator handles general group membership and offset management
Each Kafka server instantiates a coordinator which is responsible for a set of groups. 
Groups are assigned to coordinators based on their group names.
```

可以看出，GroupCoordinator负责消费者组的成员及消费位移管理，每个Broker实例在运行时都会初始化该对象，负责管理多个消费者组。那么，consumer如何确定与哪个Broker实例的GroupCoordinator进行交互呢？上一篇内容介绍消费位移时说到
Kafka Broker端有一个内部主题**`_consumer_offsets`**，负责存储每个ConsumerGroup的消费位移，该主题默认情况下有50个partition，每个partition3个副本：

```
bin/kafka-topics.sh  --zookeeper localhost:2181  --topic  __consumer_offsets  --describe
Topic:__consumer_offsets	PartitionCount:50	ReplicationFactor:3	Configs:segment.bytes=104857600,cleanup.policy=compact,compression.type=producer
	Topic: __consumer_offsets	Partition: 0	Leader: 5	Replicas: 4,6,5	Isr: 5
	Topic: __consumer_offsets	Partition: 1	Leader: 5	Replicas: 5,4,6	Isr: 5
	Topic: __consumer_offsets	Partition: 2	Leader: 5	Replicas: 6,5,4	Isr: 5
	Topic: __consumer_offsets	Partition: 3	Leader: 5	Replicas: 4,5,6	Isr: 5
	...
	Topic: __consumer_offsets	Partition: 49	Leader: 5	Replicas: 5,4,6	Isr: 5
```

Consumer通过配置groupId的hash值与`_consumer_offsets`的分区数取模得到对应的分区，如下：

```
def partitionFor(groupId: String): Int = Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount
```

获得对应的分区后，再寻找此分区的Leader副本所在的Broker节点，该Broker节点即为这个ConsumerGroup内所有consumer所对应的GroupCoordinator节点。

## KafkaConsumer#poll

KafkaConsumer#poll方法是消费者拉取消息的入口方法，实现如下：

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

poll()方法主要包含以下内容：

* 通过记录当前线程id抢占锁，确保KafkaConsumer实例不会被多线程并发访问，保证线程安全。
* 调用`updateAssignmentMetadataIfNeeded()`方法完成消费者拉取消息前的元数据准备。
* 调用pollForFetches(timer)拉取消息。
* 将经过Consumer#Interceptors处理过后的消息返回给消费者或拉取超时返回空集合，最后释放锁。

updateAssignmentMetadataIfNeeded()方法中将会完成consumer拉取消息前所有的准备工作，源码如下：

```
boolean updateAssignmentMetadataIfNeeded(final Timer timer, final boolean waitForJoinGroup) {
    if (coordinator != null && !coordinator.poll(timer, waitForJoinGroup)) {
        return false;
    }
    return updateFetchPositions(timer);
}
```

源码比较简单，主要是两个方法的调用：

* ConsumerCoordinator#poll：Kafka Consumer完成JoinGroup、Rebalance的核心方法。
* updateFetchPositions：更新消费进度

这里出现了Consumer完成JoinGroup的核心角色之一：ConsumerCoordinator，接下来从ConsumerCoordinator#poll()方法继续分析。


## ConsumerCoordinator#poll

ConsumerCoordinator负责KafkaConsumer与GroupCoordinator的交互及本地元信息的维护，poll方法实现如下：

```
public boolean poll(Timer timer, boolean waitForJoinGroup) {
    
    maybeUpdateSubscriptionMetadata();
    //执行队列中位移提交的回调任务
    invokeCompletedOffsetCommitCallbacks();
    if (subscriptions.hasAutoAssignedPartitions()) {
        ////采用自动分区策略 即采用subscribe方法订阅主题，
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
            if (subscriptions.hasPatternSubscription()) {
                ... //通过pattern 正则订阅
            }
            //当前消费组正常，joined and synced
            if (!ensureActiveGroup(waitForJoinGroup ? timer : time.timer(0L))) {
                timer.update(time.milliseconds());
                return false;
            }
        }
    } else {
        //通过assign方式制定消费分区
        if (metadata.updateRequested() && !client.hasReadyNodes(timer.currentTimeMs())) {
            client.awaitMetadataUpdate(timer);
        }
    }
    //enable.auto.commit=true 尝试消费位移自动提交
    maybeAutoCommitOffsetsAsync(timer.currentTimeMs());
    return true;
}

```

其中主要包含以下几部分内容：

* 1、消费位移自动提交及提交完成的回调处理，方法invokeCompletedOffsetCommitCallbacks()及maybeAutoCommitOffsetsAsync()；
* 2、心跳任务，通过pollHeartbeat()唤醒心跳线程，发送心跳并记录pollTimer；
* 3、消费者入组及再平衡：ensureCoordinatorReady()及ensureActiveGroup()。

接来下通过ensureCoordinatorReady()及ensureActiveGroup()方法的源码实现来分析本文的核心内容：消费组如何完成JoinGroup，可分为以下阶段：

### FIND_COORDINATOR

ensureCoordinatorReady()方法的作用是向LeastLoadNode(inFlightRequests.size最小)发送FindCoordinatorRequest，查找GroupCoordinator所在的Broker，
并在请求回调方法中建立连接。


![Find GroupCoordinator](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/find%20groupcoordinator.png)


#### SendFindCoordinatorRequest

请求发送的方法调用流程为：ensureCoordinatorReady() –> lookupCoordinator() –> sendFindCoordinatorRequest()。

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
            // ran out of time
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

#### handleFindCoordinatorRequest

Broker端处理FindCoordinatorRequest请求方法入口为KafkaApis#handleFindCoordinatorRequest()，实现如下：


```
  def handleFindCoordinatorRequest(request: RequestChannel.Request): Unit = {
    val findCoordinatorRequest = request.body[FindCoordinatorRequest]

    ...//校验
    else {
      val (partition, internalTopicName) = CoordinatorType.forId(findCoordinatorRequest.data.keyType) match {
        case CoordinatorType.GROUP =>
          //根据groupId获取对应分区
          (groupCoordinator.partitionFor(findCoordinatorRequest.data.key), GROUP_METADATA_TOPIC_NAME)

        case CoordinatorType.TRANSACTION =>
          (txnCoordinator.partitionFor(findCoordinatorRequest.data.key), TRANSACTION_STATE_TOPIC_NAME)
      }
      //获取_consumer_offsets主题的元数据
      val topicMetadata = metadataCache.getTopicMetadata(Set(internalTopicName), request.context.listenerName)
      //响应定义
      def createFindCoordinatorResponse(...}

      if (topicMetadata.headOption.isEmpty) {
          ...// 元数据为空，返回异常 COORDINATOR_NOT_AVAILABLE
      } else {
        def createResponse(requestThrottleMs: Int): AbstractResponse = {
          val responseBody = if (topicMetadata.head.errorCode != Errors.NONE.code) {
            createFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode, requestThrottleMs)
          } else {
            
            val coordinatorEndpoint = topicMetadata.head.partitions.asScala
              //获取指定的分区
              .find(_.partitionIndex == partition)
              //获取leader副本
              .filter(_.leaderId != MetadataResponse.NO_LEADER_ID)
              //leader副本所在节点
              .flatMap(metadata => metadataCache.getAliveBroker(metadata.leaderId))
              .flatMap(_.endpoints.get(request.context.listenerName.value()))
              .filterNot(_.isEmpty)

            coordinatorEndpoint match {
              case Some(endpoint) =>
                //返回节点信息
                createFindCoordinatorResponse(Errors.NONE, endpoint, requestThrottleMs)
              case _ =>
                createFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode, requestThrottleMs)
            }
          }
          trace("Sending FindCoordinator response %s for correlation id %d to client %s."
            .format(responseBody, request.header.correlationId, request.header.clientId))
          responseBody
        }
        requestHelper.sendResponseMaybeThrottle(request, createResponse)
      }
    }
```

可概括为3步：

* 获取groupId所属的_consumer_offsets分区，Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount 
* 获取_consumer_offsets所有的分区元数据
* 从所有的分区元数据中过滤出第一步计算出的分区Leader副本节点信息返回

### JOIN_GROUP

成功找到GroupCoordinator后，Consumer进入JoinGroup阶段，此阶段的Consumer会向GroupCoordinator发送JoinGroupRequest请求，GroupCoordinator会选出ConsumerGroup中leader的消费者及分区分配策略，并响应给
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

#### SendJoinGroupRequest

joinGroupIfNeeded()方法源码如下：

```
boolean joinGroupIfNeeded(final Timer timer) {
    while (rejoinNeededOrPending()) {
        if (!ensureCoordinatorReady(timer)) {
            return false;
        }
        if (needsJoinPrepare) {
            //首次为true
            needsJoinPrepare = false;
            //Joingroup前的准备工作，如位移提交，回调ConsumerRebalanceListener#onPartitionsRevoked
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
                //重置状态
                needsJoinPrepare = true;
            } else {
                final String reason = String.format("rebalance failed since the generation/state was " + "modified by heartbeat thread to %s/%s before the rebalance callback triggered",generationSnapshot, stateSnapshot);
                resetStateAndRejoin(reason); //重试
                resetJoinGroupFuture(); 
            }
        } else {
            ...//异常处理
        }
    }
    return true;
}
```

方法可分为以下3步：

* 1、调用onJoinPrepare()方法，发送JoinGroupRequest前完成消费位移的提交及回调ConsumerRebalanceListener#onPartitionsRevoked()方法；
* 2、调用initiateJoinGroup()方法完成请求的发送；
* 3、阻塞等待响应，若超时返回false，成功收到响应后，通过onJoinComplete()完成元数据更新并回调ConsumerRebalanceListener#onPartitionsAssigned()方法；


请求发送源码如下：

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

initiateJoinGroup()方法中先更新consumer状态为PREPARING_REBALANCE，又调用了sendJoinGroupRequest()方法完成JoinGroupRequest请求的发送，其中参数rebalanceTimeoutMs的值为max.poll.interval.ms，
而joinGroupTimeoutMs为max.poll.interval.ms加5s。protocolType为"consumer"，generation.memberId初始值为`""`空字符串。

#### HandleJoinGroupRequest

JoinGroupRequest由对应的GroupCoordinator所在的broker处理，入口方法为KafkaApis#handleJoinGroupRequest()，主要的业务由GroupCoordinator#handleJoinGroup()方法完成，GroupCoordinator
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
            //判断当前组是否可以加入，group状态 及 group.size 默认Int.MAX_VALUE ,可通过group.max.size配置
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

maybePrepareRebalance方法中通过判断当前group状态若是Stable、CompletingRebalance、Empty其中之一，即可调用prepareRebalance方法，进行Rebalance，
同时group的状态也会转为PreparingRebalance。。

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

  // 组状态转变为PreparingRebalance
  group.transitionTo(PreparingRebalance)

  val groupKey = GroupJoinKey(group.groupId)
  rebalancePurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
}
```

prepareRebalance方法中并不是直接触发再平衡的，**为了避免多个consumer短时间内均发起JoinGroup请求（如应用启动时），导致频繁的rebalance**，这里Kafka通过DelayedJoin来进行优化：

* 当ConsumerGroup不为空时，即当前组内已存在消费者，创建DelayedJoin，等待时长rebalanceTimeoutMs的值为max.poll.interval.ms，任务到期后执行GroupCoordinator#onCompleteJoin()方法：

```
private[group] class DelayedJoin(...) extends DelayedRebalance(...) {
  override def tryComplete(): Boolean = coordinator.tryCompleteJoin(group, forceComplete _)

  override def onExpiration(): Unit = {
    tryToCompleteDelayedAction()
  }
  //延时到期后执行GroupCoordinator#onCompleteJoin
  override def onComplete(): Unit = coordinator.onCompleteJoin(group)
  private def tryToCompleteDelayedAction(): Unit = coordinator.groupManager.replicaManager.tryCompleteActions()
}
```

* 当ConsumerGroup为空时，当前消费者为该组的第一个消费者，创建InitialDelayedJoin实现延时任务，该对象有2个时间配置：remainingMs(等待时间)和延时时间(delayMs)，延时到期触发后，先判断是否
等待时间已耗尽，若耗尽，则执行DelayedJoin#onComplete()，否则更新时间，再次方法延时队列。

```
private[group] class InitialDelayedJoin(
  coordinator: GroupCoordinator,
  purgatory: DelayedOperationPurgatory[DelayedRebalance],
  group: GroupMetadata,
  configuredRebalanceDelay: Int,  //group.initial.rebalance.delay.ms  默认3s
  delayMs: Int,
  remainingMs: Int ) extends DelayedJoin( coordinator, group, delayMs ) {  //父类为DelayedJoin
  override def tryComplete(): Boolean = false

  override def onComplete(): Unit = {
    group.inLock {
      if (group.newMemberAdded && remainingMs != 0) {
        // 有新成员加入 且等待时间不为0，继续等待
        // 重置的等待时间和延时时间
        group.newMemberAdded = false
        val delay = min(configuredRebalanceDelay, remainingMs)
        val remaining = max(remainingMs - delayMs, 0)
        purgatory.tryCompleteElseWatch(new InitialDelayedJoin(coordinator,
          purgatory,
          group,
          configuredRebalanceDelay,
          delay,
          remaining
        ), Seq(GroupJoinKey(group.groupId)))
      } else
        //执行
        super.onComplete()
    }
  }
}
```






InitialDelayedJoin的父类是DelayedJoin，DelayedJoin的onComplete会调用GroupCoordinator的onCompleteJoin方法响应请求。



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
    //遍历所有消费者，投票选举
    val (protocol, _) = allMemberMetadata
      .map(_.vote(candidates))
      .groupBy(identity)
      .maxBy { case (_, votes) => votes.size }
    protocol
  }

```

#### 2.3、JoinGroupResponseHandler

Consumer在sendJoinGroupRequest方法中，除了发送请求，还定义了响应的处理器JoinGroupResponseHandler，方法中首先consumer的状态更新为COMPLETING_REBALANCE，然后根据broker
返回的leader memberId判断，如果当前consumer就是leader，调用onJoinLeader，否则调用onJoinFollower。

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
                        //consumer 状态更新为COMPLETING_REBALANCE
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

### SYNC_GROUP

上一阶段JOIN_GROUP阶段的最后，leader consumer会根据GroupCoordinator返回的分区分配策略及member metadata完成具体的分区分配。如何将分区分配的结果同步给其它consumer，这里Kafka并没有
让leader consumer直接将分配结果同步给其它消费者，而是通过GroupCoordinator来实现中转，**减少复杂性**。此阶段即SYNC_GROUP阶段，**各个消费者会向GroupCoordinator发送SyncGroupRequest请求来同步分配方案**。

![SYNC GROUP](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/sync%20group.png)

#### sendSyncGroupRequest

Consumer端发送SyncGroupRequest请求的方法如下，发送时会注册一个回调函数SyncGroupResponseHandler。

```
private RequestFuture<ByteBuffer> sendSyncGroupRequest(SyncGroupRequest.Builder requestBuilder) {
    if (coordinatorUnknown())
        return RequestFuture.coordinatorNotAvailable();
    return client.send(coordinator, requestBuilder).compose(new SyncGroupResponseHandler(generation));
}
```

#### handleSyncGroupRequest

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
            //暂存回调，在延迟任务完成时触发
            group.get(memberId).awaitingSyncCallback = responseCallback
            removePendingSyncMember(group, memberId)
            
            if (group.isLeader(memberId)) {
               //只处理leader consumer携带的数据
              val missing = group.allMembers.diff(groupAssignment.keySet)
              val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

              if (missing.nonEmpty) {
                warn(s"Setting empty assignments for members $missing of ${group.groupId} for generation ${group.generationId}")
              }
              groupManager.storeGroup(group, assignment, (error: Errors) => {
                group.inLock {
                  if (group.is(CompletingRebalance) && generationId == group.generationId) {
                    if (error != Errors.NONE) {
                      resetAndPropagateAssignmentError(group, error)
                      maybePrepareRebalance(group, s"Error when storing group assignment during SyncGroup (member: $memberId)")
                    } else {
                       //将分区方案持久化保存到groupMetadata中，并响应给组内消费者
                      setAndPropagateAssignment(group, assignment)
                      // 组状态变为Stable
                      group.transitionTo(Stable)
                    }
                  }
                }
              }, requestLocal)
              groupCompletedRebalanceSensor.record()
            }
          case Stable =>
            removePendingSyncMember(group, memberId)
            // if the group is stable, we just return the current assignment
            val memberMetadata = group.get(memberId)
            responseCallback(SyncGroupResult(group.protocolType, group.protocolName, memberMetadata.assignment, Errors.NONE))
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))
        }
      }
    }
  }

```

doSyncGroup

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
      //回调，通知组内所有消费者
      if (group.maybeInvokeSyncCallback(member, SyncGroupResult(protocolType, protocolName, member.assignment, error))) {
        completeAndScheduleNextHeartbeatExpiration(group, member)
      }
    }
  }  

```

#### SyncGroupResponseHandler

收到SyncGroupResponse由SyncGroupResponseHandler#handle()进行处理，响应数据校验通过后，consumer的状态更新为STABLE。

```
public void handle(SyncGroupResponse syncResponse,
                   org.apache.kafka.clients.consumer.internals.RequestFuture<ByteBuffer> future) {
    Errors error = syncResponse.error();
    if (error == Errors.NONE) {
        if (isProtocolTypeInconsistent(syncResponse.data().protocolType())) {
            future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL);
        } else {
            log.debug("Received successful SyncGroup response: {}", syncResponse);
            sensors.syncSensor.record(response.requestLatencyMs());
            synchronized (AbstractCoordinator.this) {
                if (!generation.equals(Generation.NO_GENERATION) && state == MemberState.COMPLETING_REBALANCE) {
                    final String protocolName = syncResponse.data().protocolName();
                    final boolean protocolNameInconsistent = protocolName != null && !protocolName.equals(generation.protocolName);
                    if (protocolNameInconsistent) {
                        future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL);
                    } else {
                        //处理相应，consuemr stat转为STABLE
                        log.info("Successfully synced group in generation {}", generation);
                        state = MemberState.STABLE;
                        rejoinNeeded = false;
                        lastRebalanceEndMs = time.milliseconds();
                        sensors.successfulRebalanceSensor.record(lastRebalanceEndMs - lastRebalanceStartMs);
                        lastRebalanceStartMs = -1L;
                        //callback
                        future.complete(ByteBuffer.wrap(syncResponse.data().assignment()));
                    }
                } else {
                    future.raise(Errors.ILLEGAL_GENERATION);
                }
            }
        }
    } else {
        ...//异常处理
    }
}
```

### JoinComplete

以上3个阶段后，consumer已完成入组并获取到主题分区，回到joinGroupIfNeeded方法，initiateJoinGroup方法的请求结果由ConsumerCoordinator#onJoinComplete处理，方法中完成了分区信息更新并触发rebalanceListener。
至此，consumer已完成入组。

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

## Consumer状态机

consumer从创建到可以正常消费消息共有以下4种状态：

```
UNJOINED             // the client is not part of a group
PREPARING_REBALANCE  // the client has sent the join group request, but have not received response
COMPLETING_REBALANCE // the client has received join group response, but have not received assignment
STABLE               // the client has joined and is sending heartbeats
```

UNJOINED为初始状态，join group失败或leave group后consumer会重置为UNJOINED。
