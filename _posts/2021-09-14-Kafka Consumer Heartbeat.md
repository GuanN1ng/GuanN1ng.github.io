---
layout: post
title:  Kafka Consumer Heartbeat
date:   2021-09-14 13:58:38
categories: Kafka
---

KafkaConsumer完成JoinGroup及SyncGroup后，就已经处于正常工作状态。此后consumer需要向GroupCoordinator定时发送心跳请求，心跳主要有以下几点功能：

* 心跳是GroupCoordinator判断consumer是否存活的依据，若一个consumer未在指定时间内发送心跳，则会被从group内移除，并进行rebalance；

* 心跳是GroupCoordinator通知consumer其所属的group状态变化的通道。如有新的消费者入组导致rebalance，当group内已处于stable的consumer发送心跳时,GroupCoordinator会通过Errors.REBALANCE_IN_PROGRESS
异常告知consumer进行rejoin group；

* consumer的心跳线程超时时，consumer会主动发送leave group请求。


## SendHeartbeatRequest

KafkaConsumer关于心跳发送的实现有两个重要类：Heartbeat和HeartbeatThread，Heartbeat类负责记录当前consumer与GroupCoordinator的交互信息，如心跳、poll时间、session，HeartbeatThread是
心跳线程，负责完成心跳发送。


### Heartbeat

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


### HeartbeatThread

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

#### run方法

HeartbeatThread#run方法内是具体的心跳发送逻辑，心跳发送前会经过以下判断：

* 1、获取AbstractCoordinator对象锁，保证线程安全；
* 2、coordinator未知(为null或无法连接)时，会去查找coordinator，若还是失败，则等待重试，retryBackoffMs表示重试间隔；
* 3、consumer端计算sessionTimeout，超时后标记coordinator未知；
* 4、consumer的两次poll间隔超过了maxPollIntervalMs，发起Leave Group请；
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
### 心跳请求

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


## HandleHeartbeatRequest

Broker端对应的API为KafkaApis#handleHeartbeatRequest，调用链为：KafkaApis#handleHeartbeatRequest()->GroupCoordinator.handleHeartbeat()
->GroupCoordinator#completeAndScheduleNextHeartbeatExpiration()->GroupCoordinator#completeAndScheduleNextExpiration()。下面我们主要分析下
handleHeartbeat及completeAndScheduleNextExpiration方法。

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

### DelayedHeartbeat

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
  override def onExpiration(): Unit = coordinator.onExpireHeartbeat(group, memberId, isPending)
  override def onComplete(): Unit = {}
}
```

对于心跳超时的consumer，会被group移除，若移除的是leader，需重新选举consumer leader，然后将group的状态转化为PreparingRebalance，触发其它consumer的rejoin group和rebalance。

```
//心跳超时处理
def onExpireHeartbeat(group: GroupMetadata, memberId: String, isPending: Boolean): Unit = {
    group.inLock {
      if (group.is(Dead)) {
      } else if (isPending) {
        ...
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
    case PreparingRebalance => rebalancePurgatory.checkAndComplete(GroupJoinKey(group.groupId))
  }
}
```

## HeartbeatResponse响应处理

对心跳响应的处理主要有两部分，发送请求时注册的HeartbeatResponseHandler以及对heartbeatFuture添加的RequestFutureListener。

### HeartbeatResponseHandler

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
                requestRejoin("group is already rebalancing");
                future.raise(error);
            } else {
                log.debug("Ignoring heartbeat response with error {} during {} state", error, state);
                future.complete(null);
            }
        }
    } else if (error == Errors.ILLEGAL_GENERATION ||
               error == Errors.UNKNOWN_MEMBER_ID ||
               error == Errors.FENCED_INSTANCE_ID) {
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


### RequestFutureListener

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

## 相关参数

* 消费者的心跳间隔时间由参数heartbeat.interval.ms指定，这个参数必须比session.timeout.ms参数设定的值要小，一般情况下heartbeat.interval.ms的配置值不能超过session.timeout.ms配置值的1/3。这个参数可以调整得更低，以控制正常重新平衡的预期时间。

* GroupCoordinator端**心跳超时并触发rebalance的时间间隔为session.timeout.ms**。session.timeout.ms的值必须在Broker端配置的参数`group.min.session.timeout.ms`和`group.min.session.timeout.ms`的值之前。默认6s~5min。

