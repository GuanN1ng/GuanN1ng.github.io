---
layout: post
title:  Kafka Consumer Heartbeat
date:   2021-09-14 13:58:38
categories: Kafka
---

KafkaConsumer完成JoinGroup及SyncGroup后，就已经处于正常工作状态。**此后consumer需要向GroupCoordinator定时发送心跳来证明存活，以保证不会被移除group，维持对现有TopicPartition的所有权**。
这部分的内容主要涉及到两个类：Heartbeat及HeartbeatThread，下面我们先看下相关源码。

### Heartbeat

Heartbeat类负责记录当前consumer与GroupCoordinator的交互时间信息，详情通过Timer类进行维护，并提供了查询更新的方法。以下为类的定义（成员方法后续使用时再介绍）；

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

HeartbeatThread#run方法内是具体的心跳发送逻辑，方法源码如下：

```
@Override
public void run() {
    try {
        log.debug("Heartbeat thread started");
        while (true) {
            synchronized (AbstractCoordinator.this) {
                if (closed) return;

                if (!enabled) {
                    AbstractCoordinator.this.wait();
                    continue;
                }

                if (state.hasNotJoinedGroup() || hasFailed()) {
                    disable();
                    continue;
                }

                client.pollNoWakeup();
                long now = time.milliseconds();
                if (coordinatorUnknown()) {
                    if (findCoordinatorFuture != null) {
                        clearFindCoordinatorFuture();
                        AbstractCoordinator.this.wait(rebalanceConfig.retryBackoffMs);
                    } else {
                        lookupCoordinator();
                    }
                } else if (heartbeat.sessionTimeoutExpired(now)) {
                    markCoordinatorUnknown("session timed out without receiving a "+ "heartbeat response");
                } else if (heartbeat.pollTimeoutExpired(now)) {
                    maybeLeaveGroup("consumer poll timeout has expired.");
                } else if (!heartbeat.shouldHeartbeat(now)) {
                    AbstractCoordinator.this.wait(rebalanceConfig.retryBackoffMs);
                } else {
                    heartbeat.sentHeartbeat(now);
                    final org.apache.kafka.clients.consumer.internals.RequestFuture<Void> heartbeatFuture = sendHeartbeatRequest();
                    heartbeatFuture.addListener(new org.apache.kafka.clients.consumer.internals.RequestFutureListener<Void>() {
                        @Override
                        public void onSuccess(Void value) {
                            synchronized (AbstractCoordinator.this) {
                                heartbeat.receiveHeartbeat();
                            }
                        }
                        @Override
                        public void onFailure(RuntimeException e) {
                            synchronized (AbstractCoordinator.this) {
                                if (e instanceof RebalanceInProgressException) {
                                    heartbeat.receiveHeartbeat();
                                } else if (e instanceof FencedInstanceIdException) {
                                    heartbeatThread.failed.set(e);
                                } else {
                                    heartbeat.failHeartbeat();
                                    AbstractCoordinator.this.notify();
                                }
                            }
                        }
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


消费者的心跳间隔时间由参数heartbeat.interval.ms指定，这个参数必须比session.timeout.ms参数设定的值要小，
一般情况下heartbeat.interval.ms的配置值不能超过session.timeout.ms配置值的1/3。这个参数可以调整得更低，以控制正常重新平衡的预期时间。

如果一个消费者突然崩溃，GroupCoordinator会等待一段时间，确认消费者死亡后再触发rebalance，这段时间即为session.timeout.ms。session.timeout.ms的值必须在Broker端配置的
参数`group.min.session.timeout.ms`和`group.min.session.timeout.ms`的值之前。默认6s~5min。

HeartbeatThread#run()方法也会根据maxPollIntervalMs判断是否poll超时，若超时则发送LeaveGroupRequest（主动触发rebalance）。