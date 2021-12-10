---
layout: post 
title:  Kafka Broker
date:   2021-10-15 16:21:59 
categories: Kafka
---

Broker是指Kafka服务的代理节点，Brokers实例提供了消息存储、集群控制、消费者管理和请求处理等一系列功能。本文将从Broker的启动开始进行Broker端源码分析。

# Kafka#main

Kafka Broker启动的入口方法为Kafka#main()方法，源码如下：

```
  def main(args: Array[String]): Unit = {
    try {
      //获取配置
      val serverProps = getPropsFromArgs(args)
      //构建KakfaServer实例
      val server = buildServer(serverProps)

      try {
        if (!OperatingSystem.IS_WINDOWS && !Java.isIbmJdk)
          //log termination due to SIGTERM, SIGHUP and SIGINT
          new LoggingSignalHandler().register()
      } catch {
        case e: ReflectiveOperationException =>
          warn("Failed to register optional signal handler that logs a message when the process is terminated by a signal. Reason for registration failure is: $e", e)
      }

      // attach shutdown handler to catch terminating signals as well as normal termination
      //关闭的钩子函数
      Exit.addShutdownHook("kafka-shutdown-hook", { try server.shutdown() ...//异常 Exit.exit(1))
      //调用startup
      try server.startup()
      catch { ... // 异常 Exit.exit(1) }
      server.awaitShutdown()
    }
    catch {  ... //异常Exit.exit(1) }
    Exit.exit(0)
  }
```

main()方法的主要作用是获取配置完成KafkaServer的构建，并调用其startup()方法实现启动。负责创建KafkaServer的buildServer()方法实现如下：

```
  private def buildServer(props: Properties): Server = {
    val config = KafkaConfig.fromProps(props, false)
    //判断配置是否需要Zookeeper
    if (config.requiresZookeeper) {
      new KafkaServer( config, Time.SYSTEM, threadNamePrefix = None, enableForwarding = false )
    } else {
      new KafkaRaftServer( config, Time.SYSTEM, threadNamePrefix = None )
    }
  }
```

Kafka提供了两种实现：KafkaServer和KafkaRaftServer，KafkaServer实现下的集群依赖于Zookeeper进行管理，即Kafka的可靠性又依赖于Zookeeper的高可用，
显然不是一个优雅的方案，因此，Kafka社区在2.8.0版本中添加了基于Raft协议实现的KafkaRaftServer，移除了Zookeeper的依赖。具体可见[KIP-500: Replace ZooKeeper with a Self-Managed Metadata Quorum](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum) 和 [KIP-595: A Raft Protocol for the Metadata Quorum](https://cwiki.apache.org/confluence/display/KAFKA/KIP-595%3A+A+Raft+Protocol+for+the+Metadata+Quorum) 。

# KafkaServer


# KafkaRaftServer