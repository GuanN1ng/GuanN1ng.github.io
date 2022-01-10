---
layout: post 
title:  Kafka Broker 概述
date:   2021-10-15 16:21:59 
categories: Kafka
---

Broker是指Kafka服务的代理节点，Broker实例提供了消息存储、副本管理、集群控制、消费者管理和请求处理等一系列功能，而这些功能分别由Broker端的不同服务对象提供，如负责消费者组管理的`GroupCoordinator`，负责事务的`TransactionCoordinator`，
负责请求分发处理的`KafkaApis`等等。 本篇内容主要分析Broker实例和其中负责集群内分区和副本的状态管理的`KafkaController`(控制器)对象的启动和停止的源码实现。


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

KafkaServer的startup()方法的主要工作是完成Broker端所有服务组件的初始化及启动的，如`TransactionCoordinator`、`GroupCoordinator`、`KafkaApis`、`ReplicaManager`和`KafkaController`等对象。
startup()方法源码如下(省略了除KafkaController对象外其他对象的初始化)：

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

**`KafkaController`对象体现了Kafka集群内Broker节点间的角色差异**，Kafka集群内所有Broker实例都会完成`KafkaController`对象的创建，但**整个集群内同时只会有一个Broker实例的`KafaController.isActive`属性为true**，
表示该Broker节点的`KafkaController`被选举为当前**集群的控制器**，负责管理集群中所有分区和副本的状态。


### startup

KafkaController#startup()方法源码如下：

```
 def startup() = {
   //注册ZK 连接事件监听器
    zkClient.registerStateChangeHandler(new StateChangeHandler {
      override val name: String = StateChangeHandlers.ControllerHandler
      //建立新的ZK连接后调用
      override def afterInitializingSession(): Unit = {
        //放入RegisterBrokerAndReelect事件
        eventManager.put(RegisterBrokerAndReelect)
      }
      //关闭当前ZK连接前调用
      override def beforeInitializingSession(): Unit = {
        val queuedEvent = eventManager.clearAndPut(Expire)  
        // 创建新的session前阻塞等待 直至所有等待在队列中的事件处理完
        queuedEvent.awaitProcessing()
      }
    })
    //将StartUp事件放入对列
    eventManager.put(Startup)
    //启动时间处理
    eventManager.start()
  }
```

startup()方法包含两部分内容：

* 注册ZK连接监听器，当ZK连接发生变化时调用：
  * 断开连接前，阻塞线程，直至eventManager队列中的任务全部处理完成；
  * 新建连接后，将RegisterBrokerAndReelect事件放入eventManager队列中，执行broker节点的注册及controller选举；

* 将Startup事件放入eventManager队列中，并启动eventManager线程处理任务；

下面介绍下eventManager，即ControllerEventManager如何工作。

### ControllerEventManager

ControllerEventManager类的核心定义如下：

```
class ControllerEventManager(controllerId: Int, processor: ControllerEventProcessor, time: Time, rateAndTimeMetrics: Map[ControllerState, KafkaTimer], eventQueueTimeTimeoutMs: Long = 300000) extends KafkaMetricsGroup {
  import ControllerEventManager._
  
  @volatile private var _state: ControllerState = ControllerState.Idle
  private val putLock = new ReentrantLock()
  //事件队列
  private val queue = new LinkedBlockingQueue[QueuedEvent]
  // 负责处理事件的线程
  private[controller] var thread = new ControllerEventThread(ControllerEventThreadName)
  
  def put(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    val queuedEvent = new QueuedEvent(event, time.milliseconds())
    queue.put(queuedEvent)
    queuedEvent
  }
  //ZK连接重置或 Broker关闭时执行
  def clearAndPut(event: ControllerEvent): QueuedEvent = inLock(putLock){
    val preemptedEvents = new ArrayList[QueuedEvent]()
    queue.drainTo(preemptedEvents)
    //只执行事件的preempt方法。
    preemptedEvents.forEach(_.preempt(processor))
    //放入ShutdownEventThread 或 Expire事件
    put(event)
  }
  
  ...//other codes 
}  
```

ControllerEventManager的实现与Java线程池思想一致，使用队列缓存任务，并初始化ControllerEventThread负责任务的处理。

#### ControllerEvent

任务队列中存储的对象类型是QueuedEvent，定义如下：

```
class QueuedEvent(val event: ControllerEvent, //事件
                  val enqueueTimeMs: Long)  //记录入队列的时间，统计
```

可知,真正的待执行对象类型为ControllerEvent，ControllerEvent为事件接口，具体的事件通过继承ControllerEvent实现，如`Startup`、`BrokerChange`等等。

```
sealed trait ControllerEvent {
  def state: ControllerState
  // preempt() is not executed by `ControllerEventThread` but by the main thread.
  //ZK连接重置或Broker关闭时执行
  def preempt(): Unit
}

case object Startup extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerChange
  override def preempt(): Unit = {}
}

case object BrokerChange extends ControllerEvent {
  override def state: ControllerState = ControllerState.BrokerChange
  override def preempt(): Unit = {}
}

... // other event
```

#### ControllerEventThread

ControllerEventManager#start()方法的作用是完成ControllerEventThread的启动。

```
def start(): Unit = thread.start()
```

线程启动后，会循环执行`doWork()`方法执行事件任务，实现如下：

```
override def doWork(): Unit = {
  //从队列中获取任务
  val dequeued = pollFromEventQueue()
  dequeued.event match {
    case ShutdownEventThread => // The shutting down of the thread has been initiated at this point. Ignore this event.
    case controllerEvent =>
      _state = controllerEvent.state
      eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)
      try {
        //执行任务
        def process(): Unit = dequeued.process(processor)
        rateAndTimeMetrics.get(state) match {
          case Some(timer) => timer.time { process() }
          case None => process()
        }
      } catch {
        case e: Throwable => error(s"Uncaught error processing event $controllerEvent", e)
      }
      _state = ControllerState.Idle
  }
}
```

process()方法的作用是根据具体的事件任务执行相应的方法：

```
  override def process(event: ControllerEvent): Unit = {
    try {
      event match {
        case event: MockEvent =>
          // Used only in test cases
          event.process()
        case ShutdownEventThread =>
          error("Received a ShutdownEventThread event. This type of event is supposed to be handle by ControllerEventThread")
        case AutoPreferredReplicaLeaderElection =>
          processAutoPreferredReplicaLeaderElection()    
        case BrokerChange =>
          processBrokerChange()
        case ControllerChange =>
          processControllerChange()
        case Reelect =>
          processReelect()
        ...// 省略other event
         
        case Startup =>
          //KafkaController启动事件
          processStartup()
      }
    } catch {
      ...// exception
    } finally {
      updateMetrics()
    }
  }
```

可以看到，KafkaController的Startup执行的是processStartup()方法。

### processStartup

processStartup()方法源码如下：

```
private def processStartup(): Unit = {
  //注册监听/controller节点的监听器
  zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler)
  //controller选举
  elect()
}
```

注册的ControllerChangeHandler主要负责监听`/controller`节点的创建，删除，以及节点内数据改变事件。

```
class ControllerChangeHandler(eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  override val path: String = ControllerZNode.path

  override def handleCreation(): Unit = eventManager.put(ControllerChange)
  override def handleDeletion(): Unit = eventManager.put(Reelect)
  override def handleDataChange(): Unit = eventManager.put(ControllerChange)
}
```


### Controller选举

elect()方法是完成KafkaController选举的核心方法，实现如下：

```
private def elect(): Unit = {
  //获取ZK /controller节点下的数据，即当前集群的controllerId，若不存在，则返回-1
  activeControllerId = zkClient.getControllerId.getOrElse(-1)
  
  //controllerId存在 当前集群内已有节点的KafkaController对象被选举为集群的Controller
  if (activeControllerId != -1) {
    debug(s"Broker $activeControllerId has been elected as the controller, so stopping the election process.")
    return
  }
 
  try {
    //创建 /controller节点 尝试以当前节点的Controller作为集群的Controller
    val (epoch, epochZkVersion) = zkClient.registerControllerAndIncrementControllerEpoch(config.brokerId)
    controllerContext.epoch = epoch
    controllerContext.epochZkVersion = epochZkVersion
    activeControllerId = config.brokerId
    info(s"${config.brokerId} successfully elected as the controller. Epoch incremented to ${controllerContext.epoch} " + s"and epoch zk version is now ${controllerContext.epochZkVersion}")
    //创建成功，成为集群Controller
    onControllerFailover()
  } catch {
    ...// 异常处理  节点已有值
  }
}
```

KafkaController的选举是通过**各个Broker抢占式的写入`/controller`节点**实现，写入成功，则当前Broker的KafkaController即为集群的Controller。写入方法实现如下：

```
def tryCreateControllerZNodeAndIncrementEpoch(): (Int, Int) = {
  val response = retryRequestUntilConnected(
    MultiRequest(Seq(
      //节点创建请求
      CreateOp(ControllerZNode.path, ControllerZNode.encode(controllerId, timestamp), defaultAcls(ControllerZNode.path), CreateMode.EPHEMERAL),
      //epoch CAS
      SetDataOp(ControllerEpochZNode.path, ControllerEpochZNode.encode(newControllerEpoch), expectedControllerEpochZkVersion)))
  )
  response.resultCode match {
    ...// 写入结果
  }
}
```

主要涉及两个ZK node的写入，通过zooKeeper.multi() API确保多个操作原子执行：

* `/controller`，存储当前集群controller所在Broker的id，即为controllerId;
* `/controller_epoch`,CAS操作，不匹配则为非法请求，表示当前集群controller纪元，每次集群controller发生变化，自增1。


### Controller启动

KafkaController当选集群Controller后，通过调用onControllerFailover()方法完成Controller的真正启动，源码如下：

```
 private def onControllerFailover(): Unit = {
    maybeSetupFeatureVersioning()

    info("Registering handlers")

    // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
    //注册相关zk节点下监听器
    val childChangeHandlers = Seq(brokerChangeHandler, topicChangeHandler, topicDeletionHandler, logDirEventNotificationHandler, isrChangeNotificationHandler)
    childChangeHandlers.foreach(zkClient.registerZNodeChildChangeHandler)
    //注册/admin/preferred_replica_election, /admin/reassign_partitions节点事件处理
    val nodeChangeHandlers = Seq(preferredReplicaElectionHandler, partitionReassignmentHandler)
    nodeChangeHandlers.foreach(zkClient.registerZNodeChangeHandlerAndCheckExistence)
    zkClient.deleteLogDirEventNotifications(controllerContext.epochZkVersion)
    zkClient.deleteIsrChangeNotifications(controllerContext.epochZkVersion)
    
    info("Initializing controller context")
    initializeControllerContext()
    
    //初始化主题删除管理器 delete.topic.enable为true时才会删除
    val (topicsToBeDeleted, topicsIneligibleForDeletion) = fetchTopicDeletionsInProgress()
    topicDeletionManager.init(topicsToBeDeleted, topicsIneligibleForDeletion)

    //向集群内的其他Broker发送UpdateMetadata请求，
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set.empty)
    
    //启动副本状态机，初始化所有 Replica 的状态信息
    replicaStateMachine.startup()
    //启动分区状态机，初始化所有 Partition 的状态信息
    partitionStateMachine.startup()
    info(s"Ready to serve as the new controller with epoch $epoch")
    //如果有需要，触发分区副本重分配(迁移) 
    initializePartitionReassignments()
    //主题删除
    topicDeletionManager.tryTopicDeletion()
    //触发分区副本leader选举
    val pendingPreferredReplicaElections = fetchPendingPreferredReplicaElections()
    onReplicaElection(pendingPreferredReplicaElections, ElectionType.PREFERRED, ZkTriggered)
    info("Starting the controller scheduler")
    kafkaScheduler.startup()
    if (config.autoLeaderRebalanceEnable) {
      //leader副本自动均衡,
      scheduleAutoLeaderRebalanceTask(delay = 5, unit = TimeUnit.SECONDS)
    }

    if (config.tokenAuthEnabled) {
     ...//tokenManager 
    }
  }
```

onControllerFailover()方法的主要内容可分为以下几部分：

* 注册相关ZK节点变动的时间处理器，见下表格；
* 调用initializeControllerContext()方法初始化初始化ControllerContext；
* 初始化主题删除管理器`TopicDeletionManager` ，当配置`delete.topic.enable`为true时，负责删除`/admin/delete_topics`节点下的topic；
* 调用`sendUpdateMetadataRequest()`方法向集群内所有在线Broker发送UpdateMetadataRequest，获取在线节点元数据；
* 启动ReplicaStateMachine(副本状态机)，初始化所有副本的状态；
* 启动PartitionStateMachine(分区状态机)，初始化所有主题分区的状态；
* 如有需要，触发分区副本重分配以及leader选举；
* 若`auto.leader.rebalance.enable`配置为true，开启定时任务，维护分区优先副本均衡。


#### 监听ZK节点

KafkaController选举成功后需监听的ZK节点列表如下：

| 事件Handler | 节点路径  | 事件(ControllerEvent) | 作用  |
|----------|-------|----------|----------|
|BrokerChangeHandler| /brokers/ids | BrokerChange |处理Broker节点上、下线事件 |
|TopicChangeHandler|/brokers/topics| TopicChange |处理消息Topic的增减事件 |
|topicDeletionHandler|	/admin/delete_topics| TopicDeletion |处理Topic的删除事件|
|logDirEventNotificationHandler|	/log_dir_event_notification	|LogDirEventNotification| 处理LogDir异常事件(如某个Broker读取日志失败时，会添加通知) |
|isrChangeNotificationHandler|	/isr_change_notification	|IsrChangeNotification| 处理Partition ISR集合变动事件|
|partitionReassignmentHandler|	/admin/reassign_partitions	|PartitionReassignment| 处理分区副本重分配(迁移) |
|preferredReplicaElectionHandler|	/admin/preferred_replica_election	|PreferredReplicaLeaderElection| 处理Partition优先副本(leader)选举 |

#### ControllerContext初始化

ControllerContext的作用是读取Kafka集群Zookeeper相关节点的信息并缓存到本地，包括Broker、Topic、Partition以及Replica等，初始化方法源码如下：

```
  private def initializeControllerContext(): Unit = {
    //获取 /brokers/ids 节点下所有的Broker数据
    val curBrokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster
    
    //版本兼容性
    val (compatibleBrokerAndEpochs, incompatibleBrokerAndEpochs) = partitionOnFeatureCompatibility(curBrokerAndEpochs)
    if (!incompatibleBrokerAndEpochs.isEmpty) {
      warn("Ignoring registration of new brokers due to incompatibilities with finalized features: incompatibleBrokerAndEpochs.map { case (broker, _) => broker.id }.toSeq.sorted.mkString(","))
    }
    //缓存所有在线Broker
    controllerContext.setLiveBrokers(compatibleBrokerAndEpochs)
    //缓存  /brokers/topics 节点下记录的Topic
    controllerContext.setAllTopics(zkClient.getAllTopicsInCluster(true))
    //为每个Topic节点注册监听器  /brokers/topics/${topic}
    registerPartitionModificationsHandlers(controllerContext.allTopics.toSeq)
    //获取所有Topic的分区数据 
    val replicaAssignmentAndTopicIds = zkClient.getReplicaAssignmentAndTopicIdForTopics(controllerContext.allTopics.toSet)
    //判断是否使用topicId,自动生成topicId  
    //Create topic IDs for topics missing them if we are using topic IDs Otherwise, maintain what we have in the topicZNode
    processTopicIds(replicaAssignmentAndTopicIds)

    //缓存主题分区元数据
    replicaAssignmentAndTopicIds.foreach { case TopicIdReplicaAssignment(_, _, assignments) =>
      assignments.foreach { case (topicPartition, replicaAssignment) =>
        controllerContext.updatePartitionFullReplicaAssignment(topicPartition, replicaAssignment)
        if (replicaAssignment.isBeingReassigned) 
          //addingReplicas.nonEmpty || removingReplicas.nonEmpty
          //添加待重分配的主题分区 
          controllerContext.partitionsBeingReassigned.add(topicPartition)
      }
    }
    //初始化partitionLeadershipInfo和shuttingDownBrokerIds集合
    controllerContext.clearPartitionLeadershipInfo()
    controllerContext.shuttingDownBrokerIds.clear()
    //注册broker节点监听器 /brokers/ids/${id}
    registerBrokerModificationsHandler(controllerContext.liveOrShuttingDownBrokerIds)
    // update the leader and isr cache for all existing partitions from Zookeeper
    updateLeaderAndIsrCache()
    // start the channel manager
    controllerChannelManager.startup()
  }
```

### ControllerChannelManager

ControllerContext初始化完成后，调用ControllerChannelManager#startup()方法完成ControllerChannelManager对象启动，ControllerChannelManager**负责Controller与
集群内其它Broker节点间的通信**，类定义如下：

```
class ControllerChannelManager(controllerContext: ControllerContext, config: KafkaConfig, time: Time, metrics: Metrics, stateChangeLogger: StateChangeLogger, threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {
  import ControllerChannelManager._
  //<brokerId,ControllerBrokerStateInfo>
  protected val brokerStateInfo = new HashMap[Int, ControllerBrokerStateInfo]
  //对象锁
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "
  newGauge("TotalQueueSize", () => brokerLock synchronized { brokerStateInfo.values.iterator.map(_.messageQueue.size).sum } )
  
  ...//other method
```

ControllerChannelManager中为每个Broker都维护了一个ControllerBrokerStateInfo对象，定义如下：

```
case class ControllerBrokerStateInfo(networkClient: NetworkClient, 
                                     brokerNode: Node,
                                     messageQueue: BlockingQueue[QueueItem],
                                     requestSendThread: RequestSendThread,
                                     queueSizeGauge: Gauge[Int],
                                     requestRateAndTimeMetrics: Timer,
                                     reconfigurableChannelBuilder: Option[Reconfigurable])
```

重要属性如下：

* NetworkClient：与节点的网络连接对象；
* Node： Broker节点信息；
* MessageQueue(BlockingQueue)：请求队列
* RequestSendThread：请求发送线程；

#### startup

ControllerChannelManager的启动方法实现如下：

```
def startup() = {
  //为每个在线的broker初始化ControllerBrokerStateInfo
  controllerContext.liveOrShuttingDownBrokers.foreach(addNewBroker)

  brokerLock synchronized {
    //创建发送线程
    brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
  }
}
```

startup()方法分为两步：

* 遍历在线broker，调用addNewBroker()方法完成对应的ControllerBrokerStateInfo对象初始化；
* 为ControllerBrokerStateInfo对象启动请求发送线程RequestSendThread。

addNewBroker()方法源码如下：

```
  private def addNewBroker(broker: Broker): Unit = {
    //请求队列
    val messageQueue = new LinkedBlockingQueue[QueueItem]
    val controllerToBrokerListenerName = config.controlPlaneListenerName.getOrElse(config.interBrokerListenerName)
    val controllerToBrokerSecurityProtocol = config.controlPlaneSecurityProtocol.getOrElse(config.interBrokerSecurityProtocol)
    val brokerNode = broker.node(controllerToBrokerListenerName)
    val logContext = new LogContext(s"[Controller id=${config.brokerId}, targetBrokerId=${brokerNode.idString}] ")
    //初始化client
    val (networkClient, reconfigurableChannelBuilder) = {
      val channelBuilder = ChannelBuilders.clientChannelBuilder(...//channel配置)
      val reconfigurableChannelBuilder = channelBuilder match {
        case reconfigurable: Reconfigurable =>
          config.addReconfigurable(reconfigurable)
          Some(reconfigurable)
        case _ => None
      }
      // nio selector
      val selector = new Selector(...)
      //
      val networkClient = new NetworkClient(...)
      (networkClient, reconfigurableChannelBuilder)
    }
    val threadName = threadNamePrefix match {
      case None => s"Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
      case Some(name) => s"$name:Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
    }

    val requestRateAndQueueTimeMetrics = newTimer(
      RequestRateAndQueueTimeMetricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS, brokerMetricTags(broker.id)
    )

    val requestThread = new RequestSendThread(config.brokerId, controllerContext, messageQueue, networkClient, brokerNode, config, time, requestRateAndQueueTimeMetrics, stateChangeLogger, threadName)
    requestThread.setDaemon(false)

    val queueSizeGauge = newGauge(QueueSizeMetricName, () => messageQueue.size, brokerMetricTags(broker.id))
    //
    brokerStateInfo.put(broker.id, ControllerBrokerStateInfo(networkClient, brokerNode, messageQueue,
      requestThread, queueSizeGauge, requestRateAndQueueTimeMetrics, reconfigurableChannelBuilder))
  }
```

#### sendRequest

请求发送实现如下：

```
def sendRequest(brokerId: Int, request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                callback: AbstractResponse => Unit = null): Unit = {
  brokerLock synchronized {
    val stateInfoOpt = brokerStateInfo.get(brokerId)
    stateInfoOpt match {
      case Some(stateInfo) =>
        stateInfo.messageQueue.put(QueueItem(request.apiKey, request, callback, time.milliseconds()))
      case None =>
        warn(s"Not sending request $request to broker $brokerId, since it is offline.")
    }
  }
}
```

sendRequest()方法只是将请求放入对应Broker的MessageQueue中，而后由RequestSendThread从MessageQueue中获取并进行发送处理。


## BrokerChange

Broker上线或下线时，相应的在`/brokers/ids/`ZK节点下的数据也会发生增加或减少，KafkaController为`/brokers/ids/`ZK节点注册的BrokerChangeHandler监听到变化后，会将
**BrokerChange**事件放入ControllerEventManager中处理，如下：

```
class BrokerChangeHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = BrokerIdsZNode.path
  //监听节点下数据项变化
  override def handleChildChange(): Unit = {
    eventManager.put(BrokerChange)
  }
}
```

ControllerEventThread会从事件队列中取出**BrokerChange**事件，调用`processBrokerChange()`方法进行处理，源码如下：

```
  private def processBrokerChange(): Unit = {
    //确认Controller有效
    if (!isActive) return
    //获取ZK /brokers/ids节点下所有的broker信息
    val curBrokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster
    val curBrokerIdAndEpochs = curBrokerAndEpochs map { case (broker, epoch) => (broker.id, epoch) }
    val curBrokerIds = curBrokerIdAndEpochs.keySet
    //获取controllerContext中缓存的在线broker信息
    val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
    //新上线的brokerids
    val newBrokerIds = curBrokerIds.diff(liveOrShuttingDownBrokerIds)
    //已下线的brokerids
    val deadBrokerIds = liveOrShuttingDownBrokerIds.diff(curBrokerIds)
    //重启的brokerids
    val bouncedBrokerIds = (curBrokerIds & liveOrShuttingDownBrokerIds)
      .filter(brokerId => curBrokerIdAndEpochs(brokerId) > controllerContext.liveBrokerIdAndEpochs(brokerId))
    val newBrokerAndEpochs = curBrokerAndEpochs.filter { case (broker, _) => newBrokerIds.contains(broker.id) }
    val bouncedBrokerAndEpochs = curBrokerAndEpochs.filter { case (broker, _) => bouncedBrokerIds.contains(broker.id) }
    val newBrokerIdsSorted = newBrokerIds.toSeq.sorted
    val deadBrokerIdsSorted = deadBrokerIds.toSeq.sorted
    val liveBrokerIdsSorted = curBrokerIds.toSeq.sorted
    val bouncedBrokerIdsSorted = bouncedBrokerIds.toSeq.sorted
  
    //为新上线的broker初始化ControllerBrokerStateInfo对象
    newBrokerAndEpochs.keySet.foreach(controllerChannelManager.addBroker)
    //重启的broker,移除旧的，重新初始化ControllerBrokerStateInfo对象
    bouncedBrokerIds.foreach(controllerChannelManager.removeBroker)
    bouncedBrokerAndEpochs.keySet.foreach(controllerChannelManager.addBroker)
    //移除下线的broker
    deadBrokerIds.foreach(controllerChannelManager.removeBroker)

    if (newBrokerIds.nonEmpty) {
      //处理新上线的broker
      val (newCompatibleBrokerAndEpochs, newIncompatibleBrokerAndEpochs) = partitionOnFeatureCompatibility(newBrokerAndEpochs)
      if (!newIncompatibleBrokerAndEpochs.isEmpty) {
        warn("Ignoring registration of new brokers due to incompatibilities with finalized features: " +
          newIncompatibleBrokerAndEpochs.map { case (broker, _) => broker.id }.toSeq.sorted.mkString(","))
      }
      controllerContext.addLiveBrokers(newCompatibleBrokerAndEpochs)
      onBrokerStartup(newBrokerIdsSorted)
    }
    if (bouncedBrokerIds.nonEmpty) {
      controllerContext.removeLiveBrokers(bouncedBrokerIds)
      onBrokerFailure(bouncedBrokerIdsSorted)
      val (bouncedCompatibleBrokerAndEpochs, bouncedIncompatibleBrokerAndEpochs) = partitionOnFeatureCompatibility(bouncedBrokerAndEpochs)
      if (!bouncedIncompatibleBrokerAndEpochs.isEmpty) {
        warn("Ignoring registration of bounced brokers due to incompatibilities with finalized features: " + bouncedIncompatibleBrokerAndEpochs.map { case (broker, _) => broker.id }.toSeq.sorted.mkString(","))
      }
      controllerContext.addLiveBrokers(bouncedCompatibleBrokerAndEpochs)
      onBrokerStartup(bouncedBrokerIdsSorted)
    }
    if (deadBrokerIds.nonEmpty) {
      //移除下线broker
      controllerContext.removeLiveBrokers(deadBrokerIds)
      onBrokerFailure(deadBrokerIdsSorted)
    }

    if (newBrokerIds.nonEmpty || deadBrokerIds.nonEmpty || bouncedBrokerIds.nonEmpty) {
      info(s"Updated broker epochs cache: ${controllerContext.liveBrokerIdAndEpochs}")
    }
  }
```

processBrokerChange()方法主要可分为以下几部分：

* 读取ZK节点`/brokers/ids`下所有的broker元数据curBrokerAndEpochs；
* 对比curBrokerAndEpochs与ControllerContext中broker缓存liveOrShuttingDownBrokerIds，共分为三类，如下：
  * 新增节点newBrokerIds，即新上线的broker节点，先调用**ControllerChannelManager#addBroker()方法**与新上线的broker节点初始化ControllerBrokerStateInfo对象(网络连接及请求发送队列及线程)，然后再调用**onBrokerStartup()方法**进行上线处理；
  * 下线节点deadBrokerIds，即已下线的broker节点，先调用**ControllerChannelManager#removeBroker()方法**移除broker节点的ControllerBrokerStateInfo对象，然后再调用**onBrokerFailure()方法**进行上线处理；；
  * 重启节点bouncedBrokerIds，及发生重启的broker节点(epoch)，先进行下线操作，再进行上线操作；
  
可知，负责处理Broker节点上线和下线的核心方法是onBrokerStartup()和onBrokerFailure()方法。


### Broker节点上线

KafkaServer启动时，会写入`/brokers/ids/${id}`的节点，触发BrokerChange事件，负责处理Broker节点上线的核心方法onBrokerStartup()的实现如下：

```
private def onBrokerStartup(newBrokers: Seq[Int]): Unit = {
  
  newBrokers.foreach(controllerContext.replicasOnOfflineDirs.remove)
  val newBrokersSet = newBrokers.toSet
  //此前已存在的broker
  val existingBrokers = controllerContext.liveOrShuttingDownBrokerIds.diff(newBrokersSet)
  // Send update metadata request to all the existing brokers in the cluster so that they know about the new brokers
  // via this update. No need to include any partition states in the request since there are no partition state changes.
  //发送UpdateMetadataRequest给已存在broker, 使broker知道有新的broker节点上线
  sendUpdateMetadataRequest(existingBrokers.toSeq, Set.empty)
  // Send update metadata request to all the new brokers in the cluster with a full set of partition states for initialization.
  // In cases of controlled shutdown leaders will not be elected when a new broker comes up. So at least in the
  // common controlled shutdown case, the metadata will reach the new brokers faster.
  sendUpdateMetadataRequest(newBrokers, controllerContext.partitionsWithLeaders)
  
  //获取broker上的所有 replica
  val allReplicasOnNewBrokers = controllerContext.replicasOnBrokers(newBrokersSet)
  //将新上线broker上的副本的状态设置为 OnlineReplica
  replicaStateMachine.handleStateChanges(allReplicasOnNewBrokers.toSeq, OnlineReplica)
  // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions
  // to see if these brokers can become leaders for some/all of those
  partitionStateMachine.triggerOnlinePartitionStateChange()
  // 如果需要副本进行迁移的话,就执行副本迁移操作
  maybeResumeReassignments { (_, assignment) =>
    assignment.targetReplicas.exists(newBrokersSet.contains)
  }
  // 检查是否需要恢复主题删除。如果新重启的broker上至少存在一个属于被删除主题的副本，则主题删除有可能恢复
  val replicasForTopicsToBeDeleted = allReplicasOnNewBrokers.filter(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))
  if (replicasForTopicsToBeDeleted.nonEmpty) {
    //进行主题删除
    topicDeletionManager.resumeDeletionForTopics(replicasForTopicsToBeDeleted.map(_.topic))
  }
  //为新上线的broker注册监听器  /brokers/ids/${id}
  registerBrokerModificationsHandler(newBrokers)
}
```


### Broker节点下线

Broker节点下线可分为正常下线(调用 shutdown API)和突然掉线(服务宕机或ZK网络连接失败)，两种情况都会因为`/brokers/ids/${id}`节点的消失触发onBrokerFailure()方法，但Broker正常下线
时还会向集群Controller发送**ControlledShutdownRequest**，该请求由KafkaApis#handleControlledShutdownRequest()方法进行处理。

#### handleControlledShutdownRequest

集群Controller节点收到ControlledShutdownRequest后，会封装为**ControlledShutdown**事件放入事件队列。

```
  def handleControlledShutdownRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldNeverReceive(request)
    val controlledShutdownRequest = request.body[ControlledShutdownRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    //结果回调
    def controlledShutdownCallback(controlledShutdownResult: Try[Set[TopicPartition]]): Unit = {...} 
    zkSupport.controller.controlledShutdown(controlledShutdownRequest.data.brokerId, controlledShutdownRequest.data.brokerEpoch, controlledShutdownCallback)
  }
  
  def controlledShutdown(id: Int, brokerEpoch: Long, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit): Unit = {
    val controlledShutdownEvent = ControlledShutdown(id, brokerEpoch, controlledShutdownCallback)
    //放入controlledShutdownEvent
    eventManager.put(controlledShutdownEvent)
  }
```

ControllerEventThread会从事件队列中取出**ControlledShutdown**事件，调用`processControlledShutdown()`方法进行处理，源码如下：

```
  private def processControlledShutdown(id: Int, brokerEpoch: Long, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit): Unit = {
    //调用doControlledShutdown方法处理
    val controlledShutdownResult = Try { doControlledShutdown(id, brokerEpoch) }
    controlledShutdownCallback(controlledShutdownResult)
  }

  private def doControlledShutdown(id: Int, brokerEpoch: Long): Set[TopicPartition] = {
    if (!isActive) {... //异常，当前节点非集群Controller}

    if (brokerEpoch != AbstractControlRequest.UNKNOWN_BROKER_EPOCH) {
      val cachedBrokerEpoch = controllerContext.liveBrokerIdAndEpochs(id)
      if (brokerEpoch < cachedBrokerEpoch) {... //broker epoch失效 异常}
    }

    info(s"Shutting down broker $id")

    if (!controllerContext.liveOrShuttingDownBrokerIds.contains(id))
      throw new BrokerNotAvailableException(s"Broker id $id does not exist.")
    //上下文更新
    controllerContext.shuttingDownBrokerIds.add(id)
    
   //需处理分区
    val partitionsToActOn = controllerContext.partitionsOnBroker(id).filter { partition =>
      controllerContext.partitionReplicaAssignment(partition).size > 1 && //主题分区数 > 1
        controllerContext.partitionLeadershipInfo(partition).isDefined &&  //存在leader副本
        !topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic)  //主题非待删除主题
    }
    //分类 leader副本是否在该broker上
    val (partitionsLedByBroker, partitionsFollowedByBroker) = partitionsToActOn.partition { partition =>
      controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr.leader == id
    }
    //下线节点上存在副本leader 通过重置分区状态为OnlinePartition 触发副本leader选举
    //选举策略为 ControlledShutdownPartitionLeaderElectionStrategy
    partitionStateMachine.handleStateChanges(partitionsLedByBroker.toSeq, OnlinePartition, Some(ControlledShutdownPartitionLeaderElectionStrategy))
    try {
      brokerRequestBatch.newBatch()
      //非leader副本，发送 StopReplica 请求，停止副本同步
      partitionsFollowedByBroker.foreach { partition =>
        brokerRequestBatch.addStopReplicaRequestForBrokers(Seq(id), partition, deletePartition = false)
      }
      brokerRequestBatch.sendRequestsToBrokers(epoch)
    } catch {
      case e: IllegalStateException =>
        handleIllegalState(e)
    }
    // If the broker is a follower, updates the isr in ZK and notifies the current leader
    replicaStateMachine.handleStateChanges(partitionsFollowedByBroker.map(partition =>
      PartitionAndReplica(partition, id)).toSeq, OfflineReplica)
    trace(s"All leaders = ${controllerContext.partitionsLeadershipInfo.mkString(",")}")
    controllerContext.partitionLeadersOnBroker(id)
  }
```

processControlledShutdown()方法是在Broker下线前，对有副本在下线broker上的分区预处理，分两种情况：

* 下线broker节点上的副本为leader副本，通过重置分区状态为OnlinePartition，触发副本leader选举；
* 下线broker节点上的副本为follower副本，发送StopReplicaRequest停止副本同步，并将该副本状态设置为OfflineReplica状态。

正在执行关机的Broker继续进行其它资源的关闭操作，最后，ZK节点`/brokers/ids/${id}`消失，Controller监听到节点变化，继续Broker下线后的其他工作。

#### onBrokerFailure

负责处理Broker下线的方法为onBrokerFailure()，源码如下：

```
private def onBrokerFailure(deadBrokers: Seq[Int]): Unit = {
  //移除缓存中下线的broker上的分区
  deadBrokers.foreach(controllerContext.replicasOnOfflineDirs.remove)
  val deadBrokersThatWereShuttingDown =
    deadBrokers.filter(id => controllerContext.shuttingDownBrokerIds.remove(id))
  if (deadBrokersThatWereShuttingDown.nonEmpty)
    info(s"Removed ${deadBrokersThatWereShuttingDown.mkString(",")} from list of shutting down brokers.")
  //下线broker上的副本
  val allReplicasOnDeadBrokers = controllerContext.replicasOnBrokers(deadBrokers.toSet)
  //处理下线broker节点的副本
  onReplicasBecomeOffline(allReplicasOnDeadBrokers)
  //取消/brokers/ids/${id} 节点的监听
  unregisterBrokerModificationsHandler(deadBrokers)
}
```

onBrokerFailure()方法主要是完成ControllerContext的更新以及取消`/brokers/ids/${id}`节点的监听，下线副本的进一步操作则由onReplicasBecomeOffline()方法负责：

```
private def onReplicasBecomeOffline(newOfflineReplicas: Set[PartitionAndReplica]): Unit = {
    //分类，是否为待删除的离线副本
    val (newOfflineReplicasForDeletion, newOfflineReplicasNotForDeletion) =
      newOfflineReplicas.partition(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))

    //leader副本离线的分区
    val partitionsWithOfflineLeader = controllerContext.partitionsWithOfflineLeader

    //leader副本离线的分区标记为OfflinePartition状态
    partitionStateMachine.handleStateChanges(partitionsWithOfflineLeader.toSeq, OfflinePartition)
    
    // trigger OnlinePartition state changes for offline or new partitions
    //使用OfflinePartitionLeaderElectionStrategy策略进行leader选举
    val onlineStateChangeResults = partitionStateMachine.triggerOnlinePartitionStateChange()
    
    // trigger OfflineReplica state change for those newly offline replicas
    //副本状态修改为离线，
    replicaStateMachine.handleStateChanges(newOfflineReplicasNotForDeletion.toSeq, OfflineReplica)

    // fail deletion of topics that are affected by the offline replicas
    if (newOfflineReplicasForDeletion.nonEmpty) {
      //broker离线，无法进行副本删除，需要将副本标记为ReplicaDeletionIneligible状态，
      // 防止副本无限期地处于ReplicaDeletionStarted状态
      topicDeletionManager.failReplicaDeletion(newOfflineReplicasForDeletion)
    }

  // If no partition has changed leader or ISR, no UpdateMetadataRequest is sent through PartitionStateMachine
  // and ReplicaStateMachine. In that case, we want to send an UpdateMetadataRequest explicitly to
  // propagate the information about the new offline brokers.
  if (newOfflineReplicasNotForDeletion.isEmpty && onlineStateChangeResults.values.forall(_.isLeft)) {
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set.empty)
  }
}
```

onBrokerFailure()方法中同样会判断离线broker上是否有leader副本，若有则使用OfflinePartitionLeaderElectionStrategy策略进行副本leader选举，以及
Replica状态修改，即正常下线时发送的ControlledShutdownRequest只是提前触发一部分副本工作。至此，Controller端对于Broker下线的处理已全部完成。

# KafkaRaftServer

KafkaRaftServer实现中，移除了对Zookeeper集群的依赖，基于raft协议实现集群控制，可通过配置文件中的`process.roles`属性指定一个Broker节点角色，可供使用的配置项有：

* broker，负责消息存储及请求处理；
* controller，负责集群元数据管理；

两个配置项即可只配置其中任意一个，只承担一种角色，也可两个均配置，同时承担两种角色。

KafkaRaftServer定义如下：

```
class KafkaRaftServer( config: KafkaConfig, time: Time, threadNamePrefix: Option[String] ) extends Server with Logging {

  KafkaMetricsReporter.startReporters(VerifiableProperties(config.originals))
  KafkaYammerMetrics.INSTANCE.configure(config.originals)
  
  //初始化配置的日志目录，包括消息日志目录 'log.dirs' 或 'log.dir'   节点元数据目录 'metadata.log.dir'
  //验证所有目录均可访问 和 meta.properties内容与配置一致
  private val (metaProps, offlineDirs) = KafkaRaftServer.initializeLogDirs(config)

  private val metrics = Server.initializeMetrics( config, time, metaProps.clusterId )
  
  //初始化raft 集群连接   
  //配置为 controller.quorum.voters=1@localhost:9092,2@localhost:9093,3@localhost:9094;
  private val controllerQuorumVotersFuture = CompletableFuture.completedFuture(
    RaftConfig.parseVoterConnections(config.quorumVoters))
  
  //创建KafkaRaftManager
  private val raftManager = new KafkaRaftManager[ApiMessageAndVersion]( metaProps, config, new MetadataRecordSerde, KafkaRaftServer.MetadataPartition, KafkaRaftServer.MetadataTopicId, time, metrics, threadNamePrefix, controllerQuorumVotersFuture )

  //`process.roles`配置中包含 broker 项 创建BrokerServer
  private val broker: Option[BrokerServer] = if (config.processRoles.contains(BrokerRole)) {
    Some(new BrokerServer( config, metaProps, raftManager, time, metrics, threadNamePrefix, offlineDirs, controllerQuorumVotersFuture, Server.SUPPORTED_FEATURES ))
  } else {
    None
  }
  
  //`process.roles`配置中包含 controller 项  创建ControllerServer
  private val controller: Option[ControllerServer] = if (config.processRoles.contains(ControllerRole)) {
    Some(new ControllerServer( metaProps, config, raftManager, time, metrics, threadNamePrefix, controllerQuorumVotersFuture ))
  } else {
    None
  }
  ...// other code
}

```

## startup

KafkaRaftServer#startup()方法如下：

```
  override def startup(): Unit = {
    Mx4jLoader.maybeLoad()
    //kafka raft集群管理
    raftManager.startup()
    //如有，启动Controller服务
    controller.foreach(_.startup())
    //如有，启动Broker服务
    broker.foreach(_.startup())
    AppInfoParser.registerAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics, time.milliseconds())
    info(KafkaBroker.STARTED_MESSAGE)
  }
```
startup() ：**KafkaRaftManager、ControllerServer以及BrokerServer**，


### ControllerServer


### BrokerServer


### KafkaRaftManager

