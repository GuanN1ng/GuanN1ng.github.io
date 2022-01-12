---
layout: post 
title:  Kafka Topic生命周期
date:   2021-10-21 15:31:03 
categories: Kafka
---

Kafka中的消息是以主题(Topic)为单位进行管理，本篇主要分析Kafka Topic的创建、修改、删除等功能的源码部分。

# CreateTopic

Topic的创建共有两种方式：自动创建和通过脚本工具kafka-topics.sh手动创建。自动创建是指server.properties中配置项**`auto.create.topics.enable`**为true(默认值为true)时，当客户端向未知Topic发送元数据请求，Broker会自动创建一个分区数为`num.partitions`,副本因子为`default.replication.factor`的Topic，
自动创建Topic的行为都是非预期的，会增加主题的管理与维护的难度，如无特殊的应用需求，生产环境应将`auto.create.topics.enable`设置为false。


## 命令行工具创建

Kafka安装目下的bin目录中提供了很多脚本工具，其中kafka-topics.sh脚本可用来完成主题相关的操作，如创建主题：

```
# 指定topic分区数及副本因子
bin/kafka-topics.sh --create --bootstrap-server localhost:9092  --topic my-topic-name  --replication-factor 3 --partitions 3 
  --config max.message.bytes=64000

# 指定副本分配方案
bin/kafka-topics.sh --create --bootstrap-server localhost:9092  --topic my-topic-name  --replica-assignment 0:1:2,0:1:2,0:1:2
  --config max.message.bytes=64000
```


### SendCreateTopicsRequest

kafka-topics.sh脚本内容如下：

```
exec $(dirname $0)/kafka-run-class.sh kafka.admin.TopicCommand "$@"
```

可以看到，实际上是执行kafka.admin.TopicCommand对象的main方法，main方法会根据参数`--create`调用TopicCommand.TopicService#createTopic()方法，源码如下：

```
//kafka.admin.TopicCommand.TopicService#createTopic
def createTopic(topic: CommandTopicPartition): Unit = {
  //判断副本因子  最小1 最大 2^15-1
  if (topic.replicationFactor.exists(rf => rf > Short.MaxValue || rf < 1))
    throw new IllegalArgumentException(s"The replication factor must be between 1 and ${Short.MaxValue} inclusive")
  //副本数大于1  
  if (topic.partitions.exists(partitions => partitions < 1))
    throw new IllegalArgumentException(s"The partitions must be greater than 0")
  try {
    val newTopic = if (topic.hasReplicaAssignment)
      //已指定副本的分配方案
      new NewTopic(topic.name, asJavaReplicaReassignment(topic.replicaAssignment.get))
    else {
      //未指定副本分配方案
      new NewTopic( topic.name, topic.partitions.asJava, topic.replicationFactor.map(_.toShort).map(Short.box).asJava)
    }
    //配置参数
    val configsMap = topic.configsToAdd.stringPropertyNames() .asScala .map(name => name -> topic.configsToAdd.getProperty(name)) .toMap.asJava
    newTopic.configs(configsMap)
    //创建主题
    val createResult = adminClient.createTopics(Collections.singleton(newTopic), new CreateTopicsOptions().retryOnQuotaViolation(false))
    createResult.all().get()
    println(s"Created topic ${topic.name}.")
  } catch {
    ...//error
  }
}
```

KafkaAdminClient#createTopics()方法源码如下：

```
public CreateTopicsResult createTopics(final Collection<NewTopic> newTopics, final CreateTopicsOptions options) {
    final Map<String, KafkaFutureImpl<TopicMetadataAndConfig>> topicFutures = new HashMap<>(newTopics.size());
    final CreatableTopicCollection topics = new CreatableTopicCollection();
    ...//other code
    if (!topics.isEmpty()) {
        final long now = time.milliseconds();
        final long deadline = calcDeadlineMs(now, options.timeoutMs());
        final Call call = getCreateTopicsCall(options, topicFutures, topics, Collections.emptyMap(), now, deadline);
        runnable.call(call, now);
    }
    return new CreateTopicsResult(new HashMap<>(topicFutures));
}
```

getCreateTopicsCall()方法会将待创建的Topic封装为一个请求任务：

```
private Call getCreateTopicsCall(...) {
    //待发送的请求任务，目标节点是ControllerNode
    return new Call("createTopics", deadline, new ControllerNodeProvider()) {
        @Override
        public CreateTopicsRequest.Builder createRequest(int timeoutMs) {
            return new CreateTopicsRequest.Builder(new CreateTopicsRequestData().setTopics(topics).setTimeoutMs(timeoutMs).setValidateOnly(options.shouldValidateOnly()));
        }
        ...//other code
}
```

待创建的Topic会被封装为CreateTopicsRequest，稍后由NetworkClient发向kafka集群的controller节点进行处理。

### HandleCreateTopicsRequest

controller节点收到CreateTopicsRequest后，由KafkaApis#handleCreateTopicsRequest()方法进行处理，源码如下：

```
  def handleCreateTopicsRequest(request: RequestChannel.Request): Unit = {
      ...// other code
      zkSupport.adminManager.createTopics(
        createTopicsRequest.data.timeoutMs,
        createTopicsRequest.data.validateOnly,
        toCreate,
        authorizedForDescribeConfigs,
        controllerMutationQuota, handleCreateTopicsResults)
    }
  }
```

handleCreateTopicsRequest()方法的作用是完成数据准备，最终调用ZkAdminManager#createTopics()方法完成主题创建。


## 自动创建

Producer向Topic发送消息或Consumer从Topic读取消息前，均会向Broker发送MetadataRequest获取Topic的元数据，Broker处理MetadataRequest时，若有Topic不存在且允许自动创建Topic，则会进行Topic的自动创建。入口方法为KafkaApis#handleTopicMetadataRequest()，源码如下：

```
  def handleTopicMetadataRequest(request: RequestChannel.Request): Unit = {
    //元数据请求
    val metadataRequest = request.body[MetadataRequest]
    val requestVersion = request.header.apiVersion
    ...//other code
    //是否允许自动创建Topic   auto.create.topics.enable为 true  且  消费者端配置项 allow.auto.create.topics为true，默认为true
    //消费者订阅方式非正则pattern方式 
    val allowAutoCreation = config.autoCreateTopicsEnable && metadataRequest.allowAutoTopicCreation && !metadataRequest.isAllTopics
    //获取元数据
    val topicMetadata = getTopicMetadata(request, metadataRequest.isAllTopics, allowAutoCreation, authorizedTopics,
      request.context.listenerName, errorUnavailableEndpoints, errorUnavailableListeners)

    ...// other code   
```

可以看到，Broker自动创建Topic需要满足三个条件：

* config.autoCreateTopicsEnable，即Broker端配置`auto.create.topics.enable`为true；
* metadataRequest.allowAutoTopicCreation，即客户端发送的MetadataRequest请求中的allowAutoTopicCreation值为true:
  * **Producer**端发送的MetadataRequest请求中的allowAutoTopicCreation值**始终为true**；
  * **Consumer**端发送的MetadataRequest请求中的allowAutoTopicCreation值为配置**`allow.auto.create.topics`**的值，默认为true；
* !metadataRequest.isAllTopics，Consumer端请求的不是全部主题的元数据。

主题元数据获取通过getTopicMetadata()方法执行，源码如下：

```
  private def getTopicMetadata(...): Seq[MetadataResponseTopic] = {
    //从缓存中获取主题元数据
    val topicResponses = metadataCache.getTopicMetadata(topics, listenerName,
      errorUnavailableEndpoints, errorUnavailableListeners)

    if (topics.isEmpty || topicResponses.size == topics.size || fetchAllTopics) {
      //已获取到请求中全部主题的元数据
      topicResponses
    } else {
      //不存在的主题
      val nonExistingTopics = topics.diff(topicResponses.map(_.name).toSet)
      val nonExistingTopicResponses = if (allowAutoTopicCreation) {
        //进行自动创建
        val controllerMutationQuota = quotas.controllerMutation.newPermissiveQuotaFor(request)
        autoTopicCreationManager.createTopics(nonExistingTopics, controllerMutationQuota, Some(request.context))
      } else {
        //不允许自动创建，返回异常 Errors.UNKNOWN_TOPIC_OR_PARTITION
        nonExistingTopics.map { topic =>
          val error = try {
            Topic.validate(topic)
            Errors.UNKNOWN_TOPIC_OR_PARTITION
          } catch {
            case _: InvalidTopicException =>
              Errors.INVALID_TOPIC_EXCEPTION
          }
          metadataResponseTopic( error, topic, metadataCache.getTopicId(topic), Topic.isInternal(topic), util.Collections.emptyList() )
        }
      }
      topicResponses ++ nonExistingTopicResponses
    }
  }
```

### AutoTopicCreationManager

自动创建主题的核心功能由AutoTopicCreationManager实现，createTopics()方法源码如下：

```
  override def createTopics(
    topics: Set[String],
    controllerMutationQuota: ControllerMutationQuota,
    metadataRequestContext: Option[RequestContext]
  ): Seq[MetadataResponseTopic] = {
    
    //主题名校验 ，长度最大249，不能为 .或.. 等条件
    val (creatableTopics, uncreatableTopicResponses) = filterCreatableTopics(topics)

    val creatableTopicResponses = if (creatableTopics.isEmpty) {
      Seq.empty
    } else if (controller.isEmpty || !controller.get.isActive && channelManager.isDefined) {
      //当前broker 不为 controller角色
      sendCreateTopicRequest(creatableTopics, metadataRequestContext)
    } else {
      //当前broker是controller
      createTopicsInZk(creatableTopics, controllerMutationQuota)
    }
    uncreatableTopicResponses ++ creatableTopicResponses
  }
```

自动创建主题前，会先进行Topic Name的合法性校验，若当前Broker实例的角色为controller，则调用createTopicsInZk()方法完成主题创建，否则向当前集群的controller发送CreateTopicRequest请求，
CreateTopicRequest的处理流程见命令行创建主题流程，createTopicsInZk()方法实现如下：

```
  private def createTopicsInZk(
    creatableTopics: Map[String, CreatableTopic],
    controllerMutationQuota: ControllerMutationQuota
  ): Seq[MetadataResponseTopic] = {
    val topicErrors = new AtomicReference[Map[String, ApiError]]()
    try {
      //调用ZkAdminManager#createTopics创建主题
      adminManager.get.createTopics(
        timeout = 0,
        validateOnly = false,
        creatableTopics,
        Map.empty,
        controllerMutationQuota,
        topicErrors.set
      )
      val creatableTopicResponses = Option(topicErrors.get) match {...}
      creatableTopicResponses
    } finally {
      clearInflightRequests(creatableTopics)
    }
  }
```

可以看到，createTopicsInZk()方法也是调用ZkAdminManager#createTopics()方法完成主题创建。

## ZkAdminManager#createTopics

**无论是采用自动创建或命令行创建方式，最终均是调用ZkAdminManager#createTopics()方法完成Topic创建**。createTopics()方法源码如下：

```
def createTopics(...): Unit = {
  // 1. map over topics creating assignment and calling zookeeper
  val brokers = metadataCache.getAliveBrokers()
  val metadata = toCreate.values.map(topic =>
    try {
      //副本数与副本因子
      val resolvedNumPartitions = if (topic.numPartitions == NO_NUM_PARTITIONS)
        defaultNumPartitions else topic.numPartitions
      val resolvedReplicationFactor = if (topic.replicationFactor == NO_REPLICATION_FACTOR)
        defaultReplicationFactor else topic.replicationFactor

      val assignments = if (topic.assignments.isEmpty) {
        //未指定，计算副本分配方案
        AdminUtils.assignReplicasToBrokers(brokers, resolvedNumPartitions, resolvedReplicationFactor)
      } else {
        //使用用户指定的分配方案
        val assignments = new mutable.HashMap[Int, Seq[Int]]
        topic.assignments.forEach { assignment =>
          assignments(assignment.partitionIndex) = assignment.brokerIds.asScala.map(a => a: Int)
        }
        assignments
      }

      val configs = new Properties()
      topic.configs.forEach(entry => configs.setProperty(entry.name, entry.value))
      //创建参数校验
      adminZkClient.validateTopicCreate(topic.name, assignments, configs)
      validateTopicCreatePolicy(topic, resolvedNumPartitions, resolvedReplicationFactor, assignments)
      maybePopulateMetadataAndConfigs(includeConfigsAndMetadata, topic.name, configs, assignments)
      //将Topic数据写入zookeeper
      adminZkClient.createTopicWithAssignment(topic.name, configs, assignments, validate = false, config.usesTopicId)
      //填充topicId uuid
      populateIds(includeConfigsAndMetadata, topic.name)
      //元数据创建
      CreatePartitionsMetadata(topic.name, assignments.keySet)

    } catch {
      ...// error
    }).toBuffer
    // 2. if timeout <= 0, validateOnly or no topics can proceed return immediately
    // 3. else pass the assignments and errors to the delayed operation and set the keys
    ...
}
```

createTopics()方法的作用主要为以下三点：

* 确定主题副本数与副本因子，若用户未指定参数，则使用默认值，分别是`num.partitions`和`default.replication.factor`的值；
* 确定Topic的分区副本分配方案，若用户未指定，则通过计算得出，方法为AdminUtils#assignReplicasToBrokers()；
* 将Topic数据写入Zookeeper。

### Zookeeper写入

Topic的数据准备好后，即可将Topic写入Zookeeper，源码如下：

```
def createTopicWithAssignment(topic: String,
                              config: Properties,
                              partitionReplicaAssignment: Map[Int, Seq[Int]],
                              validate: Boolean = true,
                              usesTopicId: Boolean = false): Unit = {
  if (validate)
    validateTopicCreate(topic, partitionReplicaAssignment, config)
  info(s"Creating topic $topic with configuration $config and initial partition assignment $partitionReplicaAssignment")

  // write out the config if there is any, this isn't transactional with the partition assignments
  zkClient.setOrCreateEntityConfigs(ConfigType.Topic, topic, config)

  // create the partition assignment
  writeTopicPartitionAssignment(topic, partitionReplicaAssignment.map { case (k, v) => k -> ReplicaAssignment(v) }, isUpdate = false, usesTopicId)
}
```

主要写入了两部分内容：

* 将topic单独的配置写入到zk中，节点路径为：`/config/topics/${topicName}`，内容为命令行传入的`--config`配置；
* 将Topic的分区信息写入zk中，节点路径为：`/brokers/topics/${topicName}`，内容为Topic的分区副本分配方案；


## TopicChange

Topic数据写入ZK后，会触发KafkaController为`/brokers/topics`节点注册的**TopicChangeHandler**，完成Topic创建的剩余工作，TopicChangeHandler会将**TopicChange**事件对象放入ControllerEventManager
的事件队列中，等待处理，负责处理TopicChange事件的方法为processTopicChange()方法，源码如下：

```
private def processTopicChange(): Unit = {
  if (!isActive) return
  //获取/brokers/topics节点下的所有主题
  val topics = zkClient.getAllTopicsInCluster(true)
  //新增加的topic
  val newTopics = topics -- controllerContext.allTopics
  //被删除的topic
  val deletedTopics = controllerContext.allTopics.diff(topics)
  //更新缓存
  controllerContext.setAllTopics(topics)
  //为新建主题注册分区变动的Handler 节点  /brokers/topics/${topicName}
  registerPartitionModificationsHandlers(newTopics.toSeq)
  //获取主题分区分配方案
  val addedPartitionReplicaAssignment = zkClient.getReplicaAssignmentAndTopicIdForTopics(newTopics)
  deletedTopics.foreach(controllerContext.removeTopic)
  processTopicIds(addedPartitionReplicaAssignment)

  addedPartitionReplicaAssignment.foreach { case TopicIdReplicaAssignment(_, _, newAssignments) =>
    newAssignments.foreach { case (topicAndPartition, newReplicaAssignment) =>
      //更新缓存
      controllerContext.updatePartitionFullReplicaAssignment(topicAndPartition, newReplicaAssignment)
    }
  }
  info(s"New topics: [$newTopics], deleted topics: [$deletedTopics], new partition replica assignment " +
    s"[$addedPartitionReplicaAssignment]")
  if (addedPartitionReplicaAssignment.nonEmpty) {
    val partitionAssignments = addedPartitionReplicaAssignment
      .map { case TopicIdReplicaAssignment(_, _, partitionsReplicas) => partitionsReplicas.keySet }
      .reduce((s1, s2) => s1.union(s2))
    //创建分区副本
    onNewPartitionCreation(partitionAssignments)
  }
}
```

processTopicChange()方法会读取ZK对应节点下的数据，并更新本地缓存，同时为新建Topic注册`PartitionModificationsHandler`,监听Topic的分区变化，最后调用onNewPartitionCreation()方法开始进行分区与副本的相关操作。

```
//It does the following -
// 1. Move the newly created partitions to the NewPartition state
// 2. Move the newly created partitions from NewPartition->OnlinePartition state
private def onNewPartitionCreation(newPartitions: Set[TopicPartition]): Unit = {
  info(s"New partition creation callback for ${newPartitions.mkString(",")}")
  partitionStateMachine.handleStateChanges(newPartitions.toSeq, NewPartition)
  replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions).toSeq, NewReplica)
  partitionStateMachine.handleStateChanges(newPartitions.toSeq, OnlinePartition, Some(OfflinePartitionLeaderElectionStrategy(false)) )
  replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions).toSeq, OnlineReplica)
}
```

onNewPartitionCreation()方法总共分为4步：

* 1、将待创建的分区状态置为NewPartition；
* 2、将待创建的副本状态置为NewReplica;
* 3、将分区状态从NewPartition转换为OnlinePartition；
* 4、将副本状态从NewReplica转换为OnlineReplica状态。

下面分别分析状态转换时的具体操作。

### NewPartition

新建分区状态由**NonExistentPartition**转换为**NewPartition**的源码如下：

```
case NewPartition =>
  validPartitions.foreach { partition =>
    stateChangeLog.info(s"Changed partition $partition state from ${partitionState(partition)} to $targetState with  assigned replicas ${controllerContext.partitionReplicaAssignment(partition).mkString(",")}")
    controllerContext.putPartitionState(partition, NewPartition)
  }
  Map.empty
```

源码很简单，遍历所有状态为NonExistentPartition的分区，更新Partition对应的controller缓存的状态为NewPartition。

### NewReplica

新建Replica状态由**NonExistentReplica**转换为**NewReplica**的源码如下：

```
case NewReplica =>
    validReplicas.foreach { replica =>
      val partition = replica.topicPartition
      val currentState = controllerContext.replicaState(replica)

      controllerContext.partitionLeadershipInfo(partition) match {
        case Some(leaderIsrAndControllerEpoch) =>
          ...//省略 初次创建，leaderIsrAndControllerEpoch为空
        case None =>
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, partition, currentState, NewReplica)
          //更新缓存状态  
          controllerContext.putReplicaState(replica, NewReplica)
      }
    }

```

### OnlinePartition

执行分区状态由**NewPartition**转换为**OnlinePartition**的源码如下：

```
case OnlinePartition =>
  //过滤状态为NewPartition的分区
  val uninitializedPartitions = validPartitions.filter(partition => partitionState(partition) == NewPartition)
  val partitionsToElectLeader = validPartitions.filter(partition => partitionState(partition) == OfflinePartition || partitionState(partition) == OnlinePartition)
  if (uninitializedPartitions.nonEmpty) {
    //为新建的 partition 初始化 leader 和 isr
    val successfulInitializations = initializeLeaderAndIsrForPartitions(uninitializedPartitions)
    successfulInitializations.foreach {
      //更新缓存状态 
      partition => controllerContext.putPartitionState(partition, OnlinePartition)
    }
  }
  if (partitionsToElectLeader.nonEmpty) {
    ...// other code
  }
```

这一步的主要作用是初始化Partition的leader及ISR集合，核心方法为initializeLeaderAndIsrForPartitions()，源码如下：

```
private def initializeLeaderAndIsrForPartitions(partitions: Seq[TopicPartition]): Seq[TopicPartition] = {
  val successfulInitializations = mutable.Buffer.empty[TopicPartition]
  //partition - replica map
  val replicasPerPartition = partitions.map(partition => partition -> controllerContext.partitionReplicaAssignment(partition))
  val liveReplicasPerPartition = replicasPerPartition.map { case (partition, replicas) =>
      //brokerOnline && !replicasOnOfflineDirs.getOrElse(brokerId, Set.empty).contains(topicPartition)
      val liveReplicasForPartition = replicas.filter(replica => controllerContext.isReplicaOnline(replica, partition))
      partition -> liveReplicasForPartition
  }

  val (partitionsWithoutLiveReplicas, partitionsWithLiveReplicas) = liveReplicasPerPartition.partition { case (_, liveReplicas) => liveReplicas.isEmpty }
  //分区没有有效Replica
  partitionsWithoutLiveReplicas.foreach { case (partition, _) =>
    val failMsg = s"..."
    logFailedStateChange(partition, NewPartition, OnlinePartition, new StateChangeFailedException(failMsg))
  }
  //分区初始化
  val leaderIsrAndControllerEpochs = partitionsWithLiveReplicas.map { case (partition, liveReplicas) =>
    //leader副本为副本列表的第一个，ISR集合为所有有效副本
    val leaderAndIsr = LeaderAndIsr(liveReplicas.head, liveReplicas.toList)
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
    partition -> leaderIsrAndControllerEpoch
  }.toMap
  val createResponses = try {
    //分区节点写入ZK
    zkClient.createTopicPartitionStatesRaw(leaderIsrAndControllerEpochs, controllerContext.epochZkVersion)
  } catch {
    ...// error
  }
  createResponses.foreach { createResponse =>
    val code = createResponse.resultCode
    val partition = createResponse.ctx.get.asInstanceOf[TopicPartition]
    val leaderIsrAndControllerEpoch = leaderIsrAndControllerEpochs(partition)
    if (code == Code.OK) {
      //写入ZK成功，更新本地缓存
      controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
      //1、向所有replica节点发送LeaderAndIsrRequest
      //2、向所有在线broker发送UpdateMetadataRequest
      controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(leaderIsrAndControllerEpoch.leaderAndIsr.isr,
        partition, leaderIsrAndControllerEpoch, controllerContext.partitionFullReplicaAssignment(partition), isNew = true)
      successfulInitializations += partition
    } else {
      logFailedStateChange(partition, NewPartition, OnlinePartition, code)
    }
  }
  successfulInitializations
}
```

新建主题分区的状态由**NewPartition**转换为**OnlinePartition**时需要完成4部分工作：

* 1、完成leader副本及ISR副本初始化，**liveReplicas列表中的第一个副本将作为该分区的leader副本，所有的liveReplicas作为ISR集合**；
```
val leaderAndIsr = LeaderAndIsr(liveReplicas.head, liveReplicas.toList)
```
* 2、调用KafkaZkClient#createTopicPartitionStatesRaw()方法将leaderAndIsr信息写入ZK，包含3部分数据：
  * 向ZK写入**`/brokers/topics/${topicName}/partitions/`**PERSISTENT节点；
  * 向ZK写入**`/brokers/topics/${topicName}/partitions/${分区id}`**PERSISTENT节点；
  * 向ZK写入**`/brokers/topics/${topicName}/partitions/${分区id}/state`**PERSISTENT节点；
* 3、向Replica所属Broker节点发送LeaderAndIsrRequest；
* 4、向所有在线Broker节点发送UpdateMetadataRequest；
* 5、更新分区状态为OnlinePartition；


### OnlineReplica

最后一步，更新缓存中Replica的状态，将新建Replica状态由**NewReplica**转换为**OnlineReplica**。源码如下：

```
case OnlineReplica =>
  validReplicas.foreach { replica =>
    val partition = replica.topicPartition
    val currentState = controllerContext.replicaState(replica)

    currentState match {
      case NewReplica =>
        val assignment = controllerContext.partitionFullReplicaAssignment(partition)
        if (!assignment.replicas.contains(replicaId)) {
          error(s"Adding replica ($replicaId) that is not part of the assignment $assignment")
          val newAssignment = assignment.copy(replicas = assignment.replicas :+ replicaId)
          controllerContext.updatePartitionFullReplicaAssignment(partition, newAssignment)
        }
      ...//
  //状态更新    
  controllerContext.putReplicaState(replica, OnlineReplica)

```

## LeaderAndIsrRequest

Controller处理完TopicChange事件后，会向新建Replica所在Broker发送LeaderAndIsrRequest，**完成Replica对应的本地日志创建及日志相关操作**,方法的调用链为：

* KafkaApis#handleLeaderAndIsrRequest()
* ReplicaManager#becomeLeaderOrFollower()
* ReplicaManager#makeLeaders() or  ReplicaManager#makeFollowers()
* Partition#makeLeader() or  Partition#makeFollower()
* LogManager#getOrCreateLog()

这里不再对此部分进行源码分析(后续会进行分析)，至此消息Topic创建完成。

# ModifyTopic

Topic创建完成后，Kafka还提供了脚本工具对Topic进行修改，共分两种：

* kafka-configs.sh，用于修改Topic相关的配置项(config)。
  * 添加配置
```
bin/kafka-configs.sh --bootstrap-server broker_host:port --entity-type topics --entity-name my_topic_name --alter --add-config x=y
```
  * 删除配置
```
bin/kafka-configs.sh --bootstrap-server broker_host:port --entity-type topics --entity-name my_topic_name --alter --delete-config x
```

* kafka-topics.sh，用于对Topic进行扩容，即增加Topic的Partition数量。
  * 指定扩容后的Partition数量
```
bin/kafka-topics.sh --bootstrap-server broker_host:port --alter --topic my_topic_name 
    --partitions 4
```
  * 指定扩容后的数量和具体的副本分配方案
```
bin/kafka-topics.sh --bootstrap-server broker_host:port --alter  --topic my-topic-name  
   --replica-assignment 0:1:2,0:1:2,0:1:2,2:1:0  --partitions 4
```

本篇内容主要分析Topic如何进行分区扩容。

## 分区扩容

**Kafka只支持增加Topic的分区数，不支持减少分区数，即只存在分区扩容，不存在缩容。**

### CreatePartitionsRequest

使用kafka-topics.sh脚本进行分区扩容，同样也是执行TopicCommand#main()方法，`--alter`会使程序调用TopicCommand.TopicService#alterTopic()方法，源码如下：

```
def alterTopic(opts: TopicCommandOptions): Unit = {
  val topic = new CommandTopicPartition(opts)
  val topics = getTopics(opts.topic, opts.excludeInternalTopics)
  ensureTopicExists(topics, opts.topic, !opts.ifExists)

  if (topics.nonEmpty) {
    val topicsInfo = adminClient.describeTopics(topics.asJavaCollection).topicNameValues()
    val newPartitions = topics.map { topicName =>
      if (topic.hasReplicaAssignment) {
        val startPartitionId = topicsInfo.get(topicName).get().partitions().size()
        val newAssignment = {
          val replicaMap = topic.replicaAssignment.get.drop(startPartitionId)
          new util.ArrayList(replicaMap.map(p => p._2.asJava).asJavaCollection).asInstanceOf[util.List[util.List[Integer]]]
        }
        topicName -> NewPartitions.increaseTo(topic.partitions.get, newAssignment)
      } else {
        topicName -> NewPartitions.increaseTo(topic.partitions.get)
      }
    }.toMap
    //执行Topic扩容
    adminClient.createPartitions(newPartitions.asJava, new CreatePartitionsOptions().retryOnQuotaViolation(false)).all().get()
  }
}
```

分区扩容调用的是KafkaAdminClient#createPartitions()方法实现，源码如下：

```
public CreatePartitionsResult createPartitions(final Map<String, NewPartitions> newPartitions,
                                               final CreatePartitionsOptions options) {
    final Map<String, KafkaFutureImpl<Void>> futures = new HashMap<>(newPartitions.size());
    final CreatePartitionsTopicCollection topics = new CreatePartitionsTopicCollection(newPartitions.size());
    for (Map.Entry<String, NewPartitions> entry : newPartitions.entrySet()) {
        ...//数据封装
    }
    if (!topics.isEmpty()) {
        final long now = time.milliseconds();
        final long deadline = calcDeadlineMs(now, options.timeoutMs());
        //创建CreatePartitionsRequest
        final Call call = getCreatePartitionsCall(options, futures, topics, Collections.emptyMap(), now, deadline);
        //发送请求
        runnable.call(call, now);
    }
    return new CreatePartitionsResult(new HashMap<>(futures));
}
```

同Topic创建一样，通过脚本提交的扩容请求，最终被封装为CreatePartitionsRequest发送给Controller进行处理。

### HandleCreatePartitionsRequest

controller节点处理CreatePartitionsRequest的入口方法为KafkaApis#handleCreatePartitionsRequest()，handleCreatePartitionsRequest()方法中
扩容的核心步骤是调用ZkAdminManager#createPartitions()方法实现，其源码如下：

```
def createPartitions(...): Unit = {
  val allBrokers = adminZkClient.getBrokerMetadatas()
  val allBrokerIds = allBrokers.map(_.id)

  // 1. map over topics creating assignment and calling AdminUtils
  val metadata = newPartitions.map { newPartition =>
    val topic = newPartition.name
    try {
      val existingAssignment = zkClient.getFullReplicaAssignmentForTopics(immutable.Set(topic)).map {
        case (topicPartition, assignment) =>
          if (assignment.isBeingReassigned) {
            //正在重新分配
            throw new ReassignmentInProgressException(s"A partition reassignment is in progress for the topic '$topic'.")
          }
          topicPartition.partition -> assignment
      }
      if (existingAssignment.isEmpty)
        throw new UnknownTopicOrPartitionException(s"The topic '$topic' does not exist.")

      val oldNumPartitions = existingAssignment.size
      val newNumPartitions = newPartition.count
      val numPartitionsIncrement = newNumPartitions - oldNumPartitions
      if (numPartitionsIncrement < 0) {
        //不允许缩容
        throw new InvalidPartitionsException(s"Topic currently has $oldNumPartitions partitions, which is higher than the requested $newNumPartitions.")
      } else if (numPartitionsIncrement == 0) {
        //分区数一致，无效操作
        throw new InvalidPartitionsException(s"Topic already has $oldNumPartitions partitions.")
      }
      
      val newPartitionsAssignment = Option(newPartition.assignments).map {...}
      //验证新增分区的分配方案或自动计算分区方案
      val assignmentForNewPartitions = adminZkClient.createNewPartitionsAssignment( topic, existingAssignment, allBrokers, newPartition.count, newPartitionsAssignment)
      ...
      //写入ZK
      val updatedReplicaAssignment = adminZkClient.createPartitionsWithAssignment(topic, existingAssignment, assignmentForNewPartitions)
      CreatePartitionsMetadata(topic, updatedReplicaAssignment.keySet)
      
    } catch {
      ...// error
    }
  }
  // 2. if timeout <= 0, validateOnly or no topics can proceed return immediately
  // 3. else pass the assignments and errors to the delayed operation and set the keys
  ...
}

```

关于分区创建逻辑主要分为3步：

* 1、参数验证，如扩容的分区数必须大于要修改Topic的原始分区数，Topic存在，指定的分区方案brokerId是否正确等。
* 2、验证用户指定的分配方案或自动计算分区副本分配方案；
* 3、将扩容后的分区方案写入ZK；

#### ZK写入

实现将扩容后的分区分配方案写入ZK的方法为AdminZkClient#createPartitionsWithAssignment()，源码如下：

```
def createPartitionsWithAssignment(...): Map[Int, ReplicaAssignment] = {
  //合并新增分区的分配方案和之前的分区分配方案
  val combinedAssignment = existingAssignment ++ newPartitionAssignment
  //写入ZK
  writeTopicPartitionAssignment(topic, combinedAssignment, isUpdate = true)
  combinedAssignment
}
```

方法比较简单，将新增分区的分配方案和之前的分区分配方案合并，并将新的分配方案更新到ZK节点`/brokers/topics/${topicName}`中。

### PartitionModifications

新的分区分配方案写入ZK后，会触发创建主题时为ZK节点`/brokers/topics/${topicName}`注册的PartitionModificationsHandler#handleDataChange()方法，该方法会将
事件**PartitionModifications**放入Controller的事件队列中，等待Controller处理。

```
class PartitionModificationsHandler(eventManager: ControllerEventManager, topic: String) extends ZNodeChangeHandler {
  override val path: String = TopicZNode.path(topic)
  //监听节点变化，放入PartitionModifications事件
  override def handleDataChange(): Unit = eventManager.put(PartitionModifications(topic))
}
```

负责处理PartitionModifications事件的方法为KafkaController#processPartitionModifications()，源码如下：

```
private def processPartitionModifications(topic: String): Unit = {
  if (!isActive) return
  val partitionReplicaAssignment = zkClient.getFullReplicaAssignmentForTopics(immutable.Set(topic))
  val partitionsToBeAdded = partitionReplicaAssignment.filter { case (topicPartition, _) =>
    controllerContext.partitionReplicaAssignment(topicPartition).isEmpty
  }

  if (topicDeletionManager.isTopicQueuedUpForDeletion(topic)) {
    //Topic待删除
    if (partitionsToBeAdded.nonEmpty) {
      //回滚zk节点 /brokers/topics/${topicName} 中存储的分区分配方案
      restorePartitionReplicaAssignment(topic, partitionReplicaAssignment)
    } else {
      // This can happen if existing partition replica assignment are restored to prevent increasing partition count during topic deletion
      info("Ignoring partition change during topic deletion as no new partitions are added")
    }
  } else if (partitionsToBeAdded.nonEmpty) {
    
    partitionsToBeAdded.forKeyValue { (topicPartition, assignedReplicas) =>
      //缓存更新
      controllerContext.updatePartitionFullReplicaAssignment(topicPartition, assignedReplicas)
    }
    //分区创建
    onNewPartitionCreation(partitionsToBeAdded.keySet)
  }
}
```

Topic扩容前，需先判断**Topic是否为待删除状态**，若是，则回滚zk节点`/brokers/topics/${topicName}`中存储的分区分配方案，若否则调用onNewPartitionCreation()方法完成
新增分区创建，并更新ControllerContext。

onNewPartitionCreation()方法的实现在创建Topic已分析过，这里不再复述。至此，Topic的扩容分析已结束。


# RemoveTopic

Kafka Topic的删除同样是使用`kafka-topics.sh`脚本完成，命令如下：

```
bin/kafka-topics.sh --bootstrap-server broker_host:port --delete --topic my_topic_name
```

同时，Broker端的配置项`delete.topic.enable`的值(1.0.0版本后默认为true)必须为true，否则将无法删除Topic。


## SendDeleteTopicsRequest

使用`kafka-topics.sh`脚本基于Topic Name进行主题删除操作的方法调用链为：TopicCommand#main()->TopicCommand.TopicService#deleteTopic()->KafkaAdminClient#deleteTopics()->
KafkaAdminClient#handleDeleteTopicsUsingNames()，handleDeleteTopicsUsingNames()方法实现如下：

```
    private Map<String, KafkaFuture<Void>> handleDeleteTopicsUsingNames(final Collection<String> topicNames,
                                                                        final DeleteTopicsOptions options) {
        final Map<String, KafkaFutureImpl<Void>> topicFutures = new HashMap<>(topicNames.size());
        final List<String> validTopicNames = new ArrayList<>(topicNames.size());
        for (String topicName : topicNames) {
            ...//topic name非空校验 topicName == null || topicName.isEmpty();
        }
        if (!validTopicNames.isEmpty()) {
            final long now = time.milliseconds();
            final long deadline = calcDeadlineMs(now, options.timeoutMs());
            //创建DeleteTopicsRequest发送任务
            final Call call = getDeleteTopicsCall(options, topicFutures, validTopicNames,Collections.emptyMap(), now, deadline);
            //提交发送任务，发送请求
            runnable.call(call, now);
        }
        return new HashMap<>(topicFutures);
    }
```

可以看到，使用`kafka-topics.sh`脚本执行的任务最终均是通过向Controller发送不同的请求，让Controller完成任务处理。主题删除对应的请求类型为DeleteTopicsRequest。

## HandleDeleteTopicsRequest

Controller节点处理DeleteTopicsRequest的入口方法为KafkaApis#handleDeleteTopicsRequest()，handleDeleteTopicsRequest()方法主要作用是参数验证，如TopicName或TopicId以及是否
允许删除Topic(delete.topic.enable)等，然后调用了ZkAdminManager#deleteTopics()方法，源码如下：

```
def deleteTopics(...): Unit = {
  // 1. map over topics calling the asynchronous delete
  val metadata = topics.map { topic =>
      try {
        controllerMutationQuota.record(metadataCache.numPartitions(topic).getOrElse(0).toDouble)
        //添加删除主题节点
        adminZkClient.deleteTopic(topic)
        DeleteTopicMetadata(topic, Errors.NONE)
      } catch {
        ...//error
      }
  }

  // 2. if timeout <= 0 or no topics can proceed return immediately
  // 3. else pass the topics and errors to the delayed operation and set the keys
  ...
```

方法的核心步骤是调用AdminZkClient#deleteTopic()方法，对zk节点进行操作。

### ZK节点

AdminZkClient#deleteTopic()方法源码如下：

```
def deleteTopic(topic: String): Unit = {
  //ZK中有 '/brokers/topics/${topicName}'节点
  if (zkClient.topicExists(topic)) {
    try {
      //创建主题删除节点
      zkClient.createDeleteTopicPath(topic)
    } catch {
      ...//  
    }
  } else {
    throw new UnknownTopicOrPartitionException(s"Topic `$topic` to delete does not exist")
  }
}
```

方法主要分两步：
* 1、确认ZK中存在要删除的主题node:`/brokers/topics/${topicName}`;
* 2、创建Topic删除节点：**`/admin/delete_topics/${topicName}`**;

至此，DeleteTopicsRequest请求处理完成，将请求转换为了Topic删除的ZK节点。而节点的变动将会触发Controller为该节点注册的处理器。

## TopicDeletion

Controller选举成功后，会为`/admin/delete_topics`节点注册TopicDeletionHandler，监听该节点的变化，实现如下：

```
class TopicDeletionHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = DeleteTopicsZNode.path
  //监听子节点变化，创建TopicDeletion事件
  override def handleChildChange(): Unit = eventManager.put(TopicDeletion)
}
```

监听到节点变化后，会向ControllerEventManager的事件队列中添加TopicDeletion事件，等待线程处理，处理方法为KafkaController#processTopicDeletion()：

```
  private def processTopicDeletion(): Unit = {
    //当前broker为controller
    if (!isActive) return
    var topicsToBeDeleted = zkClient.getTopicDeletions.toSet
    debug(s"Delete topics listener fired for topics ${topicsToBeDeleted.mkString(",")} to be deleted")
    val nonExistentTopics = topicsToBeDeleted -- controllerContext.allTopics
    if (nonExistentTopics.nonEmpty) {
      //对应的主题不存在
      warn(s"Ignoring request to delete non-existing topics ${nonExistentTopics.mkString(",")}")
      //删除 `/admin/delete_topics/${topicName}` 节点
      zkClient.deleteTopicDeletions(nonExistentTopics.toSeq, controllerContext.epochZkVersion)
    }
    topicsToBeDeleted --= nonExistentTopics
    if (config.deleteTopicEnable) {
      //`delete.topic.enable` 为 true
      if (topicsToBeDeleted.nonEmpty) {
        // mark topic ineligible for deletion if other state changes are in progress
        topicsToBeDeleted.foreach { topic =>
          val partitionReassignmentInProgress = controllerContext.partitionsBeingReassigned.map(_.topic).contains(topic)
          if (partitionReassignmentInProgress) {
            //主题正在进行分区重新分配，标记无法进行删除的主题
            topicDeletionManager.markTopicIneligibleForDeletion(Set(topic),reason = "topic reassignment in progress")
          }
        }
        //将主题添加到待删除队列
        topicDeletionManager.enqueueTopicsForDeletion(topicsToBeDeleted)
      }
    } else {
      // If delete topic is disabled remove entries under zookeeper path : /admin/delete_topics
      info(s"Removing $topicsToBeDeleted since delete topic is disabled")
      zkClient.deleteTopicDeletions(topicsToBeDeleted.toSeq, controllerContext.epochZkVersion)
    }
  }
```
processTopicDeletion()方法分为以下几步：

* 确认当前Broker为集群Controller;
* 确认主题元数据存在，若不存在，删除之前创建的`/admin/delete_topics/${topicName}` 节点；
* 确认可进行主题删除，即`delete.topic.enable`配置true，若不为true，将`/admin/delete_topics`节点下的数据全部删除；
* 若Topic正在进行分区重分配，调用TopicDeletionManager#markTopicIneligibleForDeletion()方法，否则调用TopicDeletionManager#enqueueTopicsForDeletion()方法。

可知，负责处理Topic删除的是TopicDeletionManager对象。

## TopicDeletionManager

被选举为Controller角色的Broker会调用TopicDeletionManager#init()方法，完成TopicDeletionManager初始化，TopicDeletionManager对象负责处理Topic的
删除。

ControllerContext对象中维护了两个Set集合，分别用来存放可被删除的Topic和无法删除的Topic，如下：

```
val topicsToBeDeleted = mutable.Set.empty[String]
val topicsIneligibleForDeletion = mutable.Set.empty[String]
```

无法进行删除的Topic，如Topic分区正在重分配或托管Topic副本的Broker宕机等，会调用TopicDeletionManager#markTopicIneligibleForDeletion()方法会将Topic添加到topicsIneligibleForDeletion列表中：
```
  def markTopicIneligibleForDeletion(topics: Set[String], reason: => String): Unit = {
    if (isDeleteTopicEnabled) {
      val newTopicsToHaltDeletion = controllerContext.topicsToBeDeleted & topics
      controllerContext.topicsIneligibleForDeletion ++= newTopicsToHaltDeletion
      if (newTopicsToHaltDeletion.nonEmpty)
        info(s"Halted deletion of topics ${newTopicsToHaltDeletion.mkString(",")} due to $reason")
    }
  }
```

可以删除的Topic则会调用TopicDeletionManager#enqueueTopicsForDeletion()方法被添加到topicsToBeDeleted列表中，并调用删除方法：

```
def enqueueTopicsForDeletion(topics: Set[String]): Unit = {
  if (isDeleteTopicEnabled) {
    //添加到topicsToBeDeleted列表
    controllerContext.queueTopicDeletion(topics)
    //执行删除
    resumeDeletions()
  }
}
```

resumeDeletions()方法实现如下：

```
private def resumeDeletions(): Unit = {
  //待删除的Topic列表
  val topicsQueuedForDeletion = Set.empty[String] ++ controllerContext.topicsToBeDeleted
  val topicsEligibleForRetry = mutable.Set.empty[String]
  val topicsEligibleForDeletion = mutable.Set.empty[String]

  topicsQueuedForDeletion.foreach { topic =>
    if (controllerContext.areAllReplicasInState(topic, ReplicaDeletionSuccessful)) {
      //所有主题副本删除完成，即Topic已删除完成，从controller的缓存和ZK中清除这个topic
      completeDeleteTopic(topic)
    } else if (!controllerContext.isAnyReplicaInState(topic, ReplicaDeletionStarted)) {
      if (controllerContext.isAnyReplicaInState(topic, ReplicaDeletionIneligible)) {
        //任一副本删除失败，状态ReplicaDeletionIneligible，添加到重试列表
        topicsEligibleForRetry += topic
      }
    }
    if (isTopicEligibleForDeletion(topic)) {
      //Topic可删除
      topicsEligibleForDeletion += topic
    }
  }
  if (topicsEligibleForRetry.nonEmpty) {
    //对删除失败的副本重试
    retryDeletionForIneligibleReplicas(topicsEligibleForRetry)
  }
  if (topicsEligibleForDeletion.nonEmpty) {
    //执行Topic删除
    onTopicDeletion(topicsEligibleForDeletion)
  }
}
```
resumeDeletions()方法可分为以下几步：

* 1、从ControllerContext.topicsToBeDeleted中获取待删除的Topic并遍历；
* 2、若Topic的所有副本状态都等于ReplicaDeletionSuccessful，即主题已完成删除，执行completeDeleteTopic()方法；
* 3、若Topic的任一副本状态为ReplicaDeletionIneligible，即存在副本删除失败，记录并调用retryDeletionForIneligibleReplicas()方法进行重试；
* 4、判断Topic是否可进行删除，若可以，调用onTopicDeletion()方法删除：

这里共有3个核心方法：**执行Topic删除的调用onTopicDeletion()和删除完成后completeDeleteTopic()。**

### onTopicDeletion

onTopicDeletion源码如下：

```
  private def onTopicDeletion(topics: Set[String]): Unit = {
    val unseenTopicsForDeletion = topics.diff(controllerContext.topicsWithDeletionStarted)
    if (unseenTopicsForDeletion.nonEmpty) {
      val unseenPartitionsForDeletion = unseenTopicsForDeletion.flatMap(controllerContext.partitionsForTopic)
      //先将分区状态更新为OfflinePartition
      partitionStateMachine.handleStateChanges(unseenPartitionsForDeletion.toSeq, OfflinePartition)
      //再次更新分区状态为NonExistentPartition
      partitionStateMachine.handleStateChanges(unseenPartitionsForDeletion.toSeq, NonExistentPartition)
      // 将Topic添加至topicsWithDeletionStarted集合，记录开始执行删除的Topic
      controllerContext.beginTopicDeletion(unseenTopicsForDeletion)
    }
    // send update metadata so that brokers stop serving data for topics to be deleted
    //通知所有在线Broker,主题正在删除
    //主题的分区leader被设置为 val LeaderDuringDelete: Int = -2，即分区副本将停止服务
    client.sendMetadataUpdate(topics.flatMap(controllerContext.partitionsForTopic))
    //执行分区删除
    onPartitionDeletion(topics)
  }
```
onTopicDeletion()方法的内容主要分为3部分：

* 1、更新ControllerContext中待删除主题的分区状态，当前状态->OfflinePartition->NonExistentPartition；
* 2、向所有Broker发送UpdateMetadataRequest，通知Topic正在执行删除操作，各分区的的Leader将被设置为LeaderDuringDelete(-2)，分区副本停止服务；
* 3、调用onPartitionDeletion()方法，删除Topic的所有分区。


onPartitionDeletion()方法实现如下：

```
private def onPartitionDeletion(topicsToBeDeleted: Set[String]): Unit = {
  val allDeadReplicas = mutable.ListBuffer.empty[PartitionAndReplica]
  val allReplicasForDeletionRetry = mutable.ListBuffer.empty[PartitionAndReplica]
  val allTopicsIneligibleForDeletion = mutable.Set.empty[String]
  //遍历待删除的主题
  topicsToBeDeleted.foreach { topic =>
    val (aliveReplicas, deadReplicas) = controllerContext.replicasForTopic(topic).partition { r =>
      controllerContext.isReplicaOnline(r.replica, r.topicPartition)
    }
    //获取主题已成功删除的副本
    val successfullyDeletedReplicas = controllerContext.replicasInState(topic, ReplicaDeletionSuccessful)
    //未被删除的副本
    val replicasForDeletionRetry = aliveReplicas.diff(successfullyDeletedReplicas)
    //已死亡副本 broker下线 或 副本下线
    allDeadReplicas ++= deadReplicas
    allReplicasForDeletionRetry ++= replicasForDeletionRetry
    if (deadReplicas.nonEmpty) {
      allTopicsIneligibleForDeletion += topic
    }
  }
  // move dead replicas directly to failed state
  replicaStateMachine.handleStateChanges(allDeadReplicas, ReplicaDeletionIneligible)
  replicaStateMachine.handleStateChanges(allReplicasForDeletionRetry, OfflineReplica)
  replicaStateMachine.handleStateChanges(allReplicasForDeletionRetry, ReplicaDeletionStarted)

  if (allTopicsIneligibleForDeletion.nonEmpty) {
    //存在副本死亡，无法进行Topic删除 将Topic添加到topicsIneligibleForDeletion
    markTopicIneligibleForDeletion(allTopicsIneligibleForDeletion, reason = "offline replicas")
  }
}
```

方法内容可分为3部分：

* 1、获取待删除Topic中的DeadReplica(如Replica所在Broker下线)，若有则将DeadReplica状态更新为`ReplicaDeletionIneligible`，并将其Topic添加入
  ControllerContext中的topicsIneligibleForDeletion集合，即Topic无法删除(无法删除DeadReplica);

* 2、将待删除Replica的状态状态更新为OfflineReplica，并向Replica节点发送StopReplicaRequest(deletePartition = false)，停止分区副本同步
```
case OfflineReplica =>
  validReplicas.foreach { replica =>
    //发送StopReplicaRequest
    controllerBrokerRequestBatch.addStopReplicaRequestForBrokers(Seq(replicaId), replica.topicPartition, deletePartition = false)
  }
  val (replicasWithLeadershipInfo, replicasWithoutLeadershipInfo) = validReplicas.partition { replica =>
    controllerContext.partitionLeadershipInfo(replica.topicPartition).isDefined
  }
  //将副本从ISR移除
  val updatedLeaderIsrAndControllerEpochs = removeReplicasFromIsr(replicaId, replicasWithLeadershipInfo.map(_.topicPartition))
  updatedLeaderIsrAndControllerEpochs.forKeyValue { (partition, leaderIsrAndControllerEpoch) =>
    ...// 省略
    val replica = PartitionAndReplica(partition, replicaId)
    controllerContext.putReplicaState(replica, OfflineReplica)
  }
  ...// 省略
```

* 3、将待删除Replica的状态状态更新为ReplicaDeletionStarted，并**再次发送StopReplicaRequest(deletePartition = true)，删除分区元数据**
```
case ReplicaDeletionStarted =>
  validReplicas.foreach { replica =>=
    //更新缓存状态
    controllerContext.putReplicaState(replica, ReplicaDeletionStarted)
    //向Broker发送StopReplicaRequest，完成Replica删除
    controllerBrokerRequestBatch.addStopReplicaRequestForBrokers(Seq(replicaId), replica.topicPartition, deletePartition = true)
  }
```

可见**删除副本时需发送两次StopReplicaRequest**。


#### TopicDeletionStopReplicaResponseReceived事件

发送删除副本的StopReplicaRequest时，会注册一个回调函数，如下：

```
  private def sendStopReplicaRequests(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    ... //other code
    //响应回调
    def responseCallback(brokerId: Int, isPartitionDeleted: TopicPartition => Boolean)
                        (response: AbstractResponse): Unit = {
      ...//
      if (partitionErrorsForDeletingTopics.nonEmpty) //Errors.NONE 
        sendEvent(TopicDeletionStopReplicaResponseReceived(brokerId, stopReplicaResponse.error,partitionErrorsForDeletingTopics))
    }
    ...//other code
```

sendEvent()方法会将TopicDeletionStopReplicaResponseReceived事件放入ControllerEventManager的事件队列，KafkaController处理TopicDeletionStopReplicaResponseReceived事件
调用的方法为processTopicDeletionStopReplicaResponseReceived()，实现如下：

```
private def processTopicDeletionStopReplicaResponseReceived(...): Unit = {
  if (!isActive) return
  val partitionsInError = if (requestError != Errors.NONE)
    partitionErrors.keySet
  else
    partitionErrors.filter { case (_, error) => error != Errors.NONE }.keySet
  //删除失败的副本
  val replicasInError = partitionsInError.map(PartitionAndReplica(_, replicaId))
  // move all the failed replicas to ReplicaDeletionIneligible
  //将副本状态更新为ReplicaDeletionIneligible
  topicDeletionManager.failReplicaDeletion(replicasInError)
  if (replicasInError.size != partitionErrors.size) {
    // some replicas could have been successfully deleted
    val deletedReplicas = partitionErrors.keySet.diff(partitionsInError)
    //将副本状态更新为 ReplicaDeletionSuccessful
    topicDeletionManager.completeReplicaDeletion(deletedReplicas.map(PartitionAndReplica(_, replicaId)))
  }
}
```

processTopicDeletionStopReplicaResponseReceived()负责处理StopReplicaResponse，若副本删除失败，则将副本状态修改为ReplicaDeletionIneligible，否则副本删除
完成，状态修改为ReplicaDeletionSuccessful。


### completeDeleteTopic

当一个Topic的所有副本状态均已转换位ReplicaDeletionSuccessful后，主题删除完成，下一轮的resumeDeletions()方法将会调用completeDeleteTopic()，完成主题删除的
最后一步，源码如下：

```
  private def completeDeleteTopic(topic: String): Unit = {
    //取消Topic zknode监听器
    client.mutePartitionModifications(topic)
    
    val replicasForDeletedTopic = controllerContext.replicasInState(topic, ReplicaDeletionSuccessful)
    //replica状态 ReplicaDeletionSuccessful =>  NonExistentReplica
    replicaStateMachine.handleStateChanges(replicasForDeletedTopic.toSeq, NonExistentReplica)
    //ZK节点删除
    client.deleteTopic(topic, controllerContext.epochZkVersion)
    //缓存元数据删除
    controllerContext.removeTopic(topic)
  }
```

方法主要分为4部分内容：

* 1、取消被删除Topic的ZK节点监听处理器，ZNode路径为`/brokers/topics/${TopicName}`；
* 2、将状态为ReplicaDeletionSuccessful的副本状态修改为NonExistentReplica；
* 3、删除zk中的数据包括：**`/brokers/topics/${TopicName}`、`/config/topics/${TopicName}` 、`/admin/delete_topics/${TopicName}`**；
* 4、清理缓存中Topic元数据。

至此，Topic删除完成。
