---
layout: post 
title:  Kafka Broker 概述
date:   2021-10-15 16:21:59 
categories: Kafka
---

Broker是指Kafka服务的代理节点，Brokers实例提供了消息存储、副本管理、集群控制、消费者管理和请求处理等一系列功能，而这些功能分别由Broker端的不同服务对象提供，如负责消费者组管理的`GroupCoordinator`，负责事务的`TransactionCoordinator`，
负责请求分发处理的`KafkaApis`等等。 本篇内容主要分析Broker实例及负责集群内分区和副本的状态管理对象`KafkaController`(控制器)的启动和停止的源码实现。


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

Kafka提供了两种实现：**KafkaServer和KafkaRaftServer**，KafkaServer实现下的集群依赖于Zookeeper进行管理，即Kafka的可靠性又依赖于Zookeeper的高可用，
这显然不是一个优雅的方案，因此，Kafka在2.8.0版本中添加了**基于Raft协议实现的KafkaRaftServer**，移除了对Zookeeper的依赖，但**目前(3.1)仍不建议在生产环境使用**。具体可见[KIP-500: Replace ZooKeeper with a Self-Managed Metadata Quorum](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum) 和 [KIP-595: A Raft Protocol for the Metadata Quorum](https://cwiki.apache.org/confluence/display/KAFKA/KIP-595%3A+A+Raft+Protocol+for+the+Metadata+Quorum) 。

下面对KafkaServer和KafkaRaftServer分别进行分析。

# KafkaServer

KafkaServer是Kafka 2.8.0版本前Kafka服务端的唯一实现，负责管理Kafka节点的生命周期，即处理单个Kafka Broker启动和关闭时的所有工作。

## startup

KafkaServer的startup()方法的主要工作是完成Broker端所有服务组件的初始化及启动的，如`TransactionCoordinator`、`GroupCoordinator`、`KafkaApis`、`SocketServer`等对象，
也包括`KafkaController`对象，**`KafkaController`对象体现了Kafka集群内Broker节点间的角色差异**，Kafka集群内所有Broker实例都会完成`KafkaController`对象的初始化，但**整个集群内同时只会有一个Broker实例的`KafaController.isActive`属性为true**，
表示该Broker节点的`KafkaController`被选举为当前**集群的控制器**，负责管理集群中所有分区和副本的状态。

startup()方法源码如下(省略了其它组件的初始化)：

```
  override def startup(): Unit = {
    try {
      if (isShuttingDown.get)
        throw new IllegalStateException("Kafka server is still shutting down, cannot re-start!")
      if (startupComplete.get)
        return
      val canStartup = isStartingUp.compareAndSet(false, true)
      if (canStartup) {
        _brokerState = BrokerState.STARTING

        /* setup zookeeper */
        initZkClient(time)
        configRepository = new ZkConfigRepository(new AdminZkClient(zkClient))
        ...
        /* Get or create cluster_id */
        _clusterId = getOrGenerateClusterId(zkClient)

        /* generate brokerId */
        config.brokerId = getOrGenerateBrokerId(preloadedBrokerMetadataCheckpoint)
        ...//other obj

        clientToControllerChannelManager = BrokerToControllerChannelManager(...)
        clientToControllerChannelManager.start()
        ...
        /* start kafka controller */
        _kafkaController = new KafkaController(config, zkClient, time, metrics, brokerInfo, brokerEpoch, tokenManager, brokerFeatures, featureCache, threadNamePrefix)
        kafkaController.startup()

        _brokerState = BrokerState.RUNNING
        shutdownLatch = new CountDownLatch(1)
        startupComplete.set(true)
        isStartingUp.set(false)
        AppInfoParser.registerAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics, time.milliseconds())
        info("started")
      }
    }
    catch {
      ...//异常处理
  }
```



# KafkaRaftServer



## startup



```
  override def startup(): Unit = {
    Mx4jLoader.maybeLoad()
    raftManager.startup()
    controller.foreach(_.startup())
    broker.foreach(_.startup())
    AppInfoParser.registerAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics, time.milliseconds())
    info(KafkaBroker.STARTED_MESSAGE)
  }
```