---
layout: post
title:  Kafka Consumer Heartbeat
date:   2021-09-14 13:58:38
categories: Kafka
---

KafkaConsumer完成JoinGroup及SyncGroup后，就已经处于正常工作状态。此后consumer需要向GroupCoordinator定时发送心跳请求，心跳主要有以下几点功能：

* 心跳是GroupCoordinator判断consumer是否存活的依据，若一个consumer未在指定时间内发送心跳，group将移除该consumer，并触发rebalance；

* 心跳是GroupCoordinator通知consumer其所属的group状态变化的通道。如发生consumer JoinGroup或LeaveGroup时，group状态将置为PreparingRebalance，当group内已处于stable的consumer发送心跳时,GroupCoordinator会响应Errors.REBALANCE_IN_PROGRESS
异常告知consumer进行rejoin group；

* consumer的心跳线程会检测两次两次poll()方法(消息拉取)的间隔时间，若超过max.poll.interval.ms，consumer会主动发送leave group请求。


KafkaConsumer关于心跳发送的实现有两个重要类：Heartbeat和HeartbeatThread，Heartbeat类负责记录当前consumer与GroupCoordinator的交互信息，如心跳、poll、session的时间，HeartbeatThread是
心跳线程，负责完成心跳发送。


## Heartbeat

Heartbeat中的时间信息通过Timer类进行维护，并提供了查询更新的方法。以下为类的定义（成员方法后续使用时再介绍）；

```
public final class Heartbeat {
    private final int maxPollIntervalMs;
    private final GroupRebalanceConfig rebalanceConfig;
    //获取系统时间
    private final Time time;
    //心跳时间信息
    private final Timer heartbeatTimer; 
    //session
    private final Timer sessionTimer;
    //消息拉取时间信息
    private final Timer pollTimer;
    private final Logger log;
    //上次心跳发送时间
    private volatile long lastHeartbeatSend = 0L;
    //是否已发送了心跳，发送时更新为true,发送失败或收到响应后会再更新为false
    private volatile boolean heartbeatInFlight = false;
    ...
}

//org.apache.kafka.common.utils.Timer
public class Timer {
    //获取系统时间
    private final Time time; 
    private long startMs; 
    private long currentTimeMs;
    private long deadlineMs;
    private long timeoutMs;
    ...
}
```


## HeartbeatThread

**HeartbeatThread**是KafkaConsumer中的一个单独线程，负责发送心跳，定义如下：

```
private class HeartbeatThread extends KafkaThread implements AutoCloseable {
        private boolean enabled = false; // 是否开启发送心跳
        private boolean closed = false; //是否关闭
        private final AtomicReference<RuntimeException> failed = new AtomicReference<>(null);
        
        private HeartbeatThread() {
            //Thread name
            super(HEARTBEAT_THREAD_PREFIX + (rebalanceConfig.groupId.isEmpty() ? "" : " | " + rebalanceConfig.groupId), true);
        }
        //JoinGroupResponseHandler#handle方法中调用，即JoinGroup后开启
        public void enable() {
            synchronized (AbstractCoordinator.this) {
                log.debug("Enabling heartbeat thread");
                this.enabled = true;
                heartbeat.resetTimeouts();
                AbstractCoordinator.this.notify();
            }
        }
        //未加入组前
        public void disable() {
            synchronized (AbstractCoordinator.this) {
                log.debug("Disabling heartbeat thread");
                this.enabled = false;
            }
        }
        //consumer关闭时
        public void close() {
            synchronized (AbstractCoordinator.this) {
                this.closed = true;
                AbstractCoordinator.this.notify();
            }
        }
        //异常
        private boolean hasFailed() {return failed.get() != null;}
        private RuntimeException failureCause() {return failed.get();}
}
```

### run方法

HeartbeatThread#run方法内是具体的心跳发送逻辑，心跳发送前会经过以下判断：

* 1、获取AbstractCoordinator对象锁，保证线程安全；
* 2、coordinator未知(为null或无法连接)时，会去查找coordinator，若还是失败，则等待重试，retryBackoffMs表示重试间隔；
* 3、consumer端计算sessionTimeout，超时后标记coordinator未知；
* 4、consumer的两次poll间隔超过了maxPollIntervalMs，发起Leave Group请求；
* 5、Heartbeat#shouldHeartbeat，判断是否到达心跳发送时间。

通过后，才会发送心跳，方法源码如下：

```
@Override
public void run() {
    try {
        log.debug("Heartbeat thread started");
        while (true) {
            synchronized (AbstractCoordinator.this) {
                if (closed) return; //KafkaConsuemr已关闭

                if (!enabled) { //还未加入消费组 挂起线程
                    AbstractCoordinator.this.wait();
                    continue;
                }
                //not part of a group yet;have fatal error, the client will be crashed soon
                if (state.hasNotJoinedGroup() || hasFailed()) {
                    disable();
                    continue;
                }
                // networkclient 
                client.pollNoWakeup();
                long now = time.milliseconds();
                if (coordinatorUnknown()) {
                    //groupCoordinator为null
                    if (findCoordinatorFuture != null) {
                        clearFindCoordinatorFuture();//清理上次FindCoordinator请求
                        //重试
                        AbstractCoordinator.this.wait(rebalanceConfig.retryBackoffMs);
                    } else {
                        lookupCoordinator(); //发送FindCoordinator请求
                    }
                } else if (heartbeat.sessionTimeoutExpired(now)) {
                    markCoordinatorUnknown("session timed out without receiving a "+ "heartbeat response");
                } else if (heartbeat.pollTimeoutExpired(now)) {
                    //poll时间间隔超过max.poll.interval.ms，意味着处理消息花费太多时间
                    //increasing max.poll.interval.ms or  reducing  max.poll.records
                    maybeLeaveGroup("consumer poll timeout has expired.");
                } else if (!heartbeat.shouldHeartbeat(now)) {
                    //未到心跳发送时间
                    AbstractCoordinator.this.wait(rebalanceConfig.retryBackoffMs);
                } else {
                    //更新心跳时间
                    heartbeat.sentHeartbeat(now);
                    //心跳发送
                    final RequestFuture<Void> heartbeatFuture = sendHeartbeatRequest();
                    heartbeatFuture.addListener(new RequestFutureListener<Void>() {
                        ... //心跳响应处理 见下方HeartbeatResponse响应处理
                    });
                }
            }
        }
    } catch (...Exception e) {
        //省略部分错误处理
        log.error("An authentication error occurred in the heartbeat thread", e);
        this.failed.set(e);
    } finally {
        log.debug("Heartbeat thread has closed");
    }
}
```

run()方法中与心跳相关的核心请求有两种：HeartbeatRequest和LeaveGroupRequest。

## HeartbeatRequest

HeartbeatRequest处理流程如下：

### SendHeartbeatRequest

AbstractCoordinator#sendHeartbeatRequest()方法中完成请求的构建及发送，并添加HeartbeatResponseHandler响应处理器。

```
synchronized RequestFuture<Void> sendHeartbeatRequest() {
   //请求构建
    HeartbeatRequest.Builder requestBuilder =
            new HeartbeatRequest.Builder(new HeartbeatRequestData()
                    .setGroupId(rebalanceConfig.groupId)
                    .setMemberId(this.generation.memberId)
                    .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                    .setGenerationId(this.generation.generationId)); 
    //注册响应处理 
    return client.send(coordinator, requestBuilder).compose(new HeartbeatResponseHandler(generation));
}
```


### HandleHeartbeatRequest

Broker端对应的API为KafkaApis#handleHeartbeatRequest，调用链为：KafkaApis#handleHeartbeatRequest()->GroupCoordinator.handleHeartbeat()
->GroupCoordinator#completeAndScheduleNextHeartbeatExpiration()->GroupCoordinator#completeAndScheduleNextExpiration()。下面我们主要分析下
handleHeartbeat()及completeAndScheduleNextExpiration()方法。

```
def handleHeartbeat(groupId: String,
                      memberId: String,
                      groupInstanceId: Option[String],
                      generationId: Int,
                      responseCallback: Errors => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.HEARTBEAT).foreach { error =>
      if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS)
        responseCallback(Errors.NONE)
      else
        responseCallback(error)
      return
    }
    groupManager.getGroup(groupId) match {
      case None =>
        //组不存在
        responseCallback(Errors.UNKNOWN_MEMBER_ID)
      case Some(group) => group.inLock {
        val validationErrorOpt = validateHeartbeat(
          group,
          generationId,
          memberId,
          groupInstanceId
        )

        if (validationErrorOpt.isDefined) {
          responseCallback(validationErrorOpt.get)
        } else {
          group.currentState match {
            case Empty =>
              // memberId不存在
              responseCallback(Errors.UNKNOWN_MEMBER_ID)

            case CompletingRebalance =>
              //Join group后的心跳视为成功
              val member = group.get(memberId)
              completeAndScheduleNextHeartbeatExpiration(group, member)
              responseCallback(Errors.NONE)

            case PreparingRebalance =>
                val member = group.get(memberId)
                completeAndScheduleNextHeartbeatExpiration(group, member)
                responseCallback(Errors.REBALANCE_IN_PROGRESS)

            case Stable =>
                val member = group.get(memberId)
                completeAndScheduleNextHeartbeatExpiration(group, member)
                responseCallback(Errors.NONE)

            case Dead =>
              throw new IllegalStateException(s"Reached unexpected condition for Dead group $groupId")
          }
        }
      }
    }
  }
```
handleHeartbeat方法内根据group的状态对heartbeatRequest做出响应：

* Empty时，group内无成员，返回Errors.UNKNOWN_MEMBER_ID；
* Dead时，group处于死亡状态，直接抛出异常；
* PreparingRebalance时，心跳正常处理，但需返回Errors.REBALANCE_IN_PROGRESS，通知组内消费者rejoin group；
* CompletingRebalance及Stable，心跳正常处理

#### DelayedHeartbeat

completeAndScheduleNextExpiration方法中完成该consumer本次的DelayedHeartbeat心跳延时任务，并构建新的延时任务监控下次心跳情况。

```
  private def completeAndScheduleNextExpiration(group: GroupMetadata, member: MemberMetadata, timeoutMs: Long): Unit = {
    val memberKey = MemberKey(group.groupId, member.memberId)

    member.heartbeatSatisfied = true
    //心跳任务
    heartbeatPurgatory.checkAndComplete(memberKey)

    member.heartbeatSatisfied = false
    //创建下一次心跳超时任务
    val delayedHeartbeat = new DelayedHeartbeat(this, group, member.memberId, isPending = false, timeoutMs)
    //放入延时队列
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
  }

```

DelayedHeartbeat#tryComplete是指在**sessionTimeoutMs**时间间隔内收到心跳，则取消该延时任务。若超时未收到心跳，则调用onExpiration()。

```
private[group] class DelayedHeartbeat(coordinator: GroupCoordinator,
                                      group: GroupMetadata,
                                      memberId: String,
                                      isPending: Boolean,
                                      timeoutMs: Long)
  extends DelayedOperation(timeoutMs, Some(group.lock)) {
 
  override def tryComplete(): Boolean = coordinator.tryCompleteHeartbeat(group, memberId, isPending, forceComplete _)
  //心跳超时执行
  override def onExpiration(): Unit = coordinator.onExpireHeartbeat(group, memberId, isPending)
  override def onComplete(): Unit = {}
}
```

对于心跳超时的consumer，会被group移除，若移除的是leader，需重新选举consumer leader，然后调用maybePrepareRebalance()判断否进行group rebalance。

```
//心跳超时处理
def onExpireHeartbeat(group: GroupMetadata, memberId: String, isPending: Boolean): Unit = {
    group.inLock {
      if (group.is(Dead)) {
      } else if (isPending) {
        removePendingMemberAndUpdateGroup(group, memberId)  //正在执行JoinGroup
      } else if (!group.has(memberId)) {
      } else {
        val member = group.get(memberId)
        if (!member.hasSatisfiedHeartbeat) {
          info(s"Member ${member.memberId} in group ${group.groupId} has failed, removing it from the group")
          removeMemberAndUpdateGroup(group, member, s"removing member ${member.memberId} on heartbeat expiration")
        }
      }
   }
}

private def removeMemberAndUpdateGroup(group: GroupMetadata, member: MemberMetadata, reason: String): Unit = {
  group.maybeInvokeJoinCallback(member, JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.UNKNOWN_MEMBER_ID))
  //从group内移除
  group.remove(member.memberId)

  group.currentState match {
    //
    case Dead | Empty =>
    //触发rebalance
    case Stable | CompletingRebalance => maybePrepareRebalance(group, reason)  
    //检查是否可以将JoinGroup请求任务完成
    case PreparingRebalance => rebalancePurgatory.checkAndComplete(GroupJoinKey(group.groupId))
  }
}
```
maybePrepareRebalance()方法源码如下：

```
  private def maybePrepareRebalance(group: GroupMetadata, reason: String): Unit = {
    group.inLock {
      //group 的状态为 Stable || CompletingRebalance || Empty 
      if (group.canRebalance)
        prepareRebalance(group, reason)
    }
  }

  private[group] def prepareRebalance(group: GroupMetadata, reason: String): Unit = {
    
    if (group.is(CompletingRebalance))
      //group正在进行rebalance,废弃当前分配方案，返回Errors.REBALANCE_IN_PROGRESS，使组内消费者rejoin
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)

    // 取消SYNC_GROUP请求响应
    removeSyncExpiration(group)
    //创建rejoin任务
    val delayedRebalance = if (group.is(Empty))
      new InitialDelayedJoin(this,
        rebalancePurgatory,
        group,
        groupConfig.groupInitialRebalanceDelayMs,
        groupConfig.groupInitialRebalanceDelayMs,
        max(group.rebalanceTimeoutMs - groupConfig.groupInitialRebalanceDelayMs, 0))
    else
      new DelayedJoin(this, group, group.rebalanceTimeoutMs)
    //组状态
    group.transitionTo(PreparingRebalance)

    info(s"Preparing to rebalance group ${group.groupId} in state ${group.currentState} with old generation " +
      s"${group.generationId} (${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)}) (reason: $reason)")

    val groupKey = GroupJoinKey(group.groupId)
    rebalancePurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
  }
```

若有consumer因心跳超时被移除，则需要进行rebalance，可概括为三部分内容：

* 1、若group状态为CompletingRebalance，即group内所有consumer已完成JoinGroup阶段，正在进行SyncGroup，此时废弃当前分配方案，返回Errors.REBALANCE_IN_PROGRESS，使组内消费者rejoin；
* 2、创建JoinGroup任务，等待consumer再次发起JoinGroup请求，进行rebalance；
* 3、将group状态置为PreparingRebalance。

### HandleHeartbeatResponse

对心跳响应的处理主要有两部分，发送请求时注册的HeartbeatResponseHandler以及对heartbeatFuture添加的RequestFutureListener。

#### HeartbeatResponseHandler

HeartbeatResponseHandler中主要是对异常分类出路，其中**Errors.REBALANCE_IN_PROGRESS**会触发consumer的rejoin group，进行rebalance。

```
public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
    sensors.heartbeatSensor.record(response.requestLatencyMs());
    Errors error = heartbeatResponse.error();

    if (error == Errors.NONE) {
        log.debug("Received successful Heartbeat response");
        future.complete(null);
    } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
            || error == Errors.NOT_COORDINATOR) {
        //groupCoordinator不可用异常
        markCoordinatorUnknown(error);
        future.raise(error);
    } else if (error == Errors.REBALANCE_IN_PROGRESS) {
        //group正在进行rebalance
        synchronized (AbstractCoordinator.this) {
            if (state == MemberState.STABLE) {
                //标记，需重新发起joingroup
                requestRejoin("group is already rebalancing");
                future.raise(error);
            } else {
                log.debug("Ignoring heartbeat response with error {} during {} state", error, state);
                future.complete(null);
            }
        }
    } else if (error == Errors.ILLEGAL_GENERATION || error == Errors.UNKNOWN_MEMBER_ID || error == Errors.FENCED_INSTANCE_ID) {
        if (generationUnchanged()) {
            log.info("Attempt to heartbeat with {} and group instance id {} failed due to {}, resetting generation",
                sentGeneration, rebalanceConfig.groupInstanceId, error);
            resetGenerationOnResponseError(ApiKeys.HEARTBEAT, error);
            future.raise(error);
        } else {
            // if the generation has changed, then ignore this error
            log.info("Attempt to heartbeat with stale {} and group instance id {} failed due to {}, ignoring the error",
                sentGeneration, rebalanceConfig.groupInstanceId, error);
            future.complete(null);
        }
    } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
        future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
    } else {
        future.raise(new KafkaException("Unexpected error in heartbeat response: " + error.message()));
    }
}

```


#### RequestFutureListener

RequestFutureListener主要是完成心跳时间的更新。

```
final RequestFuture<Void> heartbeatFuture = sendHeartbeatRequest();
heartbeatFuture.addListener(new RequestFutureListener<Void>() {
    @Override
    public void onSuccess(Void value) {
        synchronized (AbstractCoordinator.this) {
            //更新心跳时间
            heartbeat.receiveHeartbeat();
        }
    }

    @Override
    public void onFailure(RuntimeException e) {
        synchronized (AbstractCoordinator.this) {
            if (e instanceof RebalanceInProgressException) {
                //rebalance期间，心跳正常处理，该异常通知consuemr进行rejoin group 
                heartbeat.receiveHeartbeat();
            } else if (e instanceof FencedInstanceIdException) {
                log.error("Caught fenced group.instance.id {} error in heartbeat thread", rebalanceConfig.groupInstanceId);
                //设置异常，停止心跳 the client will be crashed soon
                heartbeatThread.failed.set(e);
            } else {
                //
                heartbeat.failHeartbeat();
                //唤醒线程
                AbstractCoordinator.this.notify();
            }
        }
    }
});

```

## LeaveGroupRequest

KafkaConsumer可通过主动发送LeaveGroupRequest，表明中止消息消费，触发group rebalance，且KafkaConsumer状态会重置为`UNJOINED`。KafkaConsumer主动发送LeaveGroupRequest存在三种情况：

* 心跳线程检测到KafkaConsumer两次poll时间间隔超过`max.poll.interval.ms`；
* KafkaConsumer调用`KafkaConsumer#unsubscribe()`方法，取消对所有Topic的消费；
* KafkaConsumer配置项`internal.leave.group.on.close`为true，执行close()方法时主动发送，否则等待session.timeout.ms，有GroupCoordinator移除。


### SendLeaveGroupRequest

请求发送的入口方法为：`AbstractCoordinator#maybeLeaveGroup()`，源码如下：

```
public synchronized RequestFuture<Void> maybeLeaveGroup(String leaveReason) {
    RequestFuture<Void> future = null;

    // Starting from 2.3, only dynamic members will send LeaveGroupRequest to the broker,
    // consumer with valid group.instance.id is viewed as static member that never sends LeaveGroup,
    // and the membership expiration is only controlled by session timeout.
    if (isDynamicMember() && !coordinatorUnknown() && state != MemberState.UNJOINED && generation.hasMemberId()) {
        // this is a minimal effort attempt to leave the group. we do not
        // attempt any resending if the request fails or times out.
        //LeaveGroupRequest不会进行重试
        LeaveGroupRequest.Builder request = new LeaveGroupRequest.Builder(rebalanceConfig.groupId,Collections.singletonList(new MemberIdentity().setMemberId(generation.memberId)));
        //请求发送
        future = client.send(coordinator, request).compose(new LeaveGroupResponseHandler(generation));
        client.pollNoWakeup();
    }
    //Consumer状态会重置为UNJOINED 及rejoinNeeded = true;
    resetGenerationOnLeaveGroup();

    return future;
}
```

若KafkaConsumer配置了`group.instance.id`，则此consumer被视为该group的**static member**，永远不会发送LeaveGroup请求，**可结合较大的`session.timeout.ms`配置，以避免由暂时不可用（例如进程重新启动）引起的group rebalance。**

### HandleLeaveGroupRequest

GroupCoordinator处理LeaveGroupRequest的流程如下：

```
   def handleLeaveGroup(groupId: String,
                       leavingMembers: List[MemberIdentity],
                       responseCallback: LeaveGroupResult => Unit): Unit = {
    //执行移除成员函数
    def removeCurrentMemberFromGroup(group: GroupMetadata, memberId: String): Unit = {
      val member = group.get(memberId)
      //当memberId从group移除，并触发再平衡  同心跳任务超时，源码见上方
      removeMemberAndUpdateGroup(group, member, s"Removing member $memberId on LeaveGroup")
      //移除该成员的心跳任务
      removeHeartbeatForLeavingMember(group, member.memberId)
      info(s"Member $member has left group $groupId through explicit `LeaveGroup` request")
    }

    validateGroupStatus(groupId, ApiKeys.LEAVE_GROUP) match {
      case Some(error) =>
        responseCallback(leaveError(error, List.empty))
      case None =>
        groupManager.getGroup(groupId) match {
          case None =>
            responseCallback(leaveError(Errors.NONE, leavingMembers.map {leavingMember =>
              memberLeaveError(leavingMember, Errors.UNKNOWN_MEMBER_ID)
            }))
          case Some(group) =>
            group.inLock {
              if (group.is(Dead)) {
                responseCallback(leaveError(Errors.COORDINATOR_NOT_AVAILABLE, List.empty))
              } else {
                val memberErrors = leavingMembers.map { leavingMember =>
                  val memberId = leavingMember.memberId
                  val groupInstanceId = Option(leavingMember.groupInstanceId)

                  if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
                    groupInstanceId.flatMap(group.currentStaticMemberId) match {
                      case Some(currentMemberId) =>
                        removeCurrentMemberFromGroup(group, currentMemberId)
                        memberLeaveError(leavingMember, Errors.NONE)
                      case None =>
                        memberLeaveError(leavingMember, Errors.UNKNOWN_MEMBER_ID)
                    }
                  } else if (group.isPendingMember(memberId)) {
                    //正在JoinGrup的consumre
                    removePendingMemberAndUpdateGroup(group, memberId)
                    //取消心跳任务
                    heartbeatPurgatory.checkAndComplete(MemberKey(group.groupId, memberId))
                    memberLeaveError(leavingMember, Errors.NONE)
                  } else {
                    val memberError = validateCurrentMember(
                      group,
                      memberId,
                      groupInstanceId,
                      operation = "leave-group"
                    ).getOrElse {
                      //将consumer移除
                      removeCurrentMemberFromGroup(group, memberId)
                      Errors.NONE
                    }
                    memberLeaveError(leavingMember, memberError)
                  }
                }
                responseCallback(leaveError(Errors.NONE, memberErrors))
              }
            }
        }
    }
  }
```

除去一些检验逻辑，和consumer因心跳超时被GroupCoordinator移除的逻辑一致，核心方法均为`removeMemberAndUpdateGroup()`(源码详解见上方)。

### HandleLeaveGroupResponse

LeaveGroupResponse的处理十分简单，若存在异常，仅做记录，不会自行对LeaveGroupRequest进行重试。

```
private class LeaveGroupResponseHandler extends CoordinatorResponseHandler<LeaveGroupResponse, Void> {
    private LeaveGroupResponseHandler(final Generation generation) {
        super(generation);
    }

    @Override
    public void handle(LeaveGroupResponse leaveResponse, RequestFuture<Void> future) {
        final List<MemberResponse> members = leaveResponse.memberResponses();
        if (members.size() > 1) {
            future.raise(new IllegalStateException("The expected leave group response " +
                                                       "should only contain no more than one member info, however get " + members));
        }
        final Errors error = leaveResponse.error();
        if (error == Errors.NONE) {
            log.debug("LeaveGroup response with {} returned successfully: {}", sentGeneration, response);
            future.complete(null);
        } else {
            log.error("LeaveGroup request with {} failed with error: {}", sentGeneration, error.message());
            future.raise(error);
        }
    }
}
```

## 相关参数

* 消费者的心跳间隔时间由参数heartbeat.interval.ms指定，这个参数必须比session.timeout.ms参数设定的值要小，一般情况下heartbeat.interval.ms的配置值不能超过session.timeout.ms配置值的1/3。这个参数可以调整得更低，以控制正常重新平衡的预期时间。

* GroupCoordinator端**心跳超时并触发rebalance的时间间隔为session.timeout.ms**。session.timeout.ms的值必须在Broker端配置的参数`group.min.session.timeout.ms`和`group.min.session.timeout.ms`的值之前。默认6s~5min。

