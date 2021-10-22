---
layout: post
title:  Kafka Consumer HeartBeat
date:   2021-09-14 13:58:38
categories: Kafka
---



### 4、HEARTBEAT

此时，consumer已完成JoinGroup以及Rebalance，处于正常工作状态。**consumer需要向GroupCoordinator定时发送心跳来证明存活，以保证不会被移除group，维持对现有TopicPartition的所有权**，如果消费者停
发送心跳的时间足够长，则整个会话就被判定为过期， GroupCoordinator会认为这个消费者己经死亡，就会触发一次再均衡行为。

Kafka中有一个单独的线程**HeartbeatThread**负责发送心跳,消费者的心跳间隔时间由参数heartbeat.interval.ms指定，这个参数必须比session.timeout.ms参数设定的值要小，
一般情况下heartbeat.interval.ms的配置值不能超过session.timeout.ms配置值的1/3。这个参数可以调整得更低，以控制正常重新平衡的预期时间。

如果一个消费者突然崩溃，GroupCoordinator会等待一段时间，确认消费者死亡后再触发rebalance，这段时间即为session.timeout.ms。session.timeout.ms的值必须在Broker端配置的
参数`group.min.session.timeout.ms`和`group.min.session.timeout.ms`的值之前。默认6s~5min。

HeartbeatThread#run()方法也会根据maxPollIntervalMs判断是否poll超时，若超时则发送LeaveGroupRequest（主动触发rebalance）。