---
layout: post 
title:  Kafka Broker 概述
date:   2021-10-15 16:21:59 
categories: Kafka
---

Broker是指Kafka服务的代理节点，Broker实例提供了消息存储、副本管理、集群控制、消费者管理和请求处理等一系列功能，而这些功能分别由Broker端的不同服务对象提供，如负责消费者组管理的`GroupCoordinator`，负责事务的`TransactionCoordinator`，
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

KafkaServer的startup()方法的主要工作是完成Broker端所有服务组件的初始化及启动的，如`TransactionCoordinator`、`GroupCoordinator`、`KafkaApis`、`ReplicaManager`等对象，
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
        //从zk获取节点 /cluster/id/${clusterId} 信息  若没有则自动生成并写入
        _clusterId = getOrGenerateClusterId(zkClient)
        /* load metadata */
        //读取meta.properties文件
        val (preloadedBrokerMetadataCheckpoint, initialOfflineDirs) =
          BrokerMetadataCheckpoint.getBrokerMetadataAndOfflineDirs(config.logDirs, ignoreMissing = true)
          
        /* check cluster id */
        if (preloadedBrokerMetadataCheckpoint.clusterId.isDefined && preloadedBrokerMetadataCheckpoint.clusterId.get != clusterId)
          throw new InconsistentClusterIdException( s"The Cluster ID ${clusterId} doesn't match stored clusterId ${preloadedBrokerMetadataCheckpoint.clusterId} in meta.properties.  The broker is trying to join the wrong cluster. Configured zookeeper.connect may be wrong.")  
        ...
        /* generate brokerId */
        // 配置broker.id > 0 且 meta.properties文件不存在或与meta.properties中的记录一致
        config.brokerId = getOrGenerateBrokerId(preloadedBrokerMetadataCheckpoint)
        ...
        //zk节点注册  /brokers/ids/${brokerId}  
        val brokerInfo = createBrokerInfo
        val brokerEpoch = zkClient.registerBroker(brokerInfo)
        
        // Now that the broker is successfully registered, checkpoint its metadata
        //更新meta.properties
        checkpointBrokerMetadata(ZkMetaProperties(clusterId, config.brokerId))
        
        ...//other obj
        /* start kafka controller */
        _kafkaController = new KafkaController(config, zkClient, time, metrics, brokerInfo, brokerEpoch, tokenManager, brokerFeatures, featureCache, threadNamePrefix)
        kafkaController.startup()
        ...
        //状态置为RUNNING
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

KafkaServer在初始化服务组件对象前，需先完成Broker节点的元数据恢复校验及向Zookeeper注册当前Broker实例的工作，可分为三部分：

* 获取clusterId，首先查询ZK节点`/cluster/id/`下是否有内容，若有则返回，若无则自动生成clusterId写入ZK并返回；

* 节点元数据恢复，读取配置`log.dirs`或`log.dir`目录下的`meta.properties`文件内容并返回，若无返回RawMetaProperties对象。`meta.properties`文件保存了上次Broker启动时
  的参数，如果Broker启动时`meta.properties`文件存在，且对应的参数与`broker.id`与`cluster.id`记录不一致，将抛出异常，表明当前Broker数据异常，可修改meta.properties或将logs.dir数据
  全部移除，则当前Broker将以一个新的Broker实例启动。`meta.properties`文件内容如下：

```
#Thu Jul 01 14:44:31 GMT+08:00 2021
version=0
broker.id=5
cluster.id=NzU2NTYzNTYtNzQ2MS00Nzk0LTg2M2EtN2NjZDQzZDkzYTlm
``` 

* 注册Broker节点。首先获取当前Broker的配置的`broker.id`并校验是否与`meta.properties`文件中的记录一致，不一致则抛出异常。获取到brokerId后，完成ZK节点`/brokers/ids/`的写入。



## KafkaController





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