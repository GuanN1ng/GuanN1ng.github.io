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

### ReplicaAssignment

若用户未指定主题的分区副本分配方案，则由AdminUtils#assignReplicasToBrokers()完成分配方案的计算，分配算法的目标有三个：

* 主题所有的分区副本均匀的分布在各broker节点上；
* 每个分区的所有副本(leader和follower)尽量分配到不同的broker节点；
* 如果所有broker都有机架信息，尽可能将每个分区的副本分配到不同的机架(consumer消费时的preferredReadReplica);

源码如下：

```
def assignReplicasToBrokers(...): Map[Int, Seq[Int]] = {
  ...//param check
  if (brokerMetadatas.forall(_.rack.isEmpty))
    //所有broker节点均无机架信息
    assignReplicasToBrokersRackUnaware(nPartitions, replicationFactor, brokerMetadatas.map(_.id), fixedStartIndex,startPartitionId)
  else {
    if (brokerMetadatas.exists(_.rack.isEmpty))
      throw new AdminOperationException("Not all brokers have rack information for replica rack aware assignment.")
    //所有broker节点均有机架信息  
    assignReplicasToBrokersRackAware(nPartitions, replicationFactor, brokerMetadatas, fixedStartIndex,startPartitionId)
  }
}
```

分区副本分配方案的计算分为两类：无机架信息(rack.isEmpty)和有机架信息。

#### 无机架信息

当所有broker节点均无机架信息时，分配方案由assignReplicasToBrokersRackUnaware()实现，源码如下：

```
private def assignReplicasToBrokersRackUnaware(...): Map[Int, Seq[Int]] = {
  //分配方案
  val ret = mutable.Map[Int, Seq[Int]]()
  //所有的broker节点
  val brokerArray = brokerList.toArray
  //fixedStartIndex = -1，startIndex为随机取一个broker
  val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
  //startPartitionId=-1 ,currentPartitionId=0
  var currentPartitionId = math.max(0, startPartitionId)
  //随机取一个分配步长
  var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
  //nPartitions分区数
  for (_ <- 0 until nPartitions) {
    if (currentPartitionId > 0 && (currentPartitionId % brokerArray.length == 0)) {
      //防止 nPartitions 过大时,其中某些partition的分配完全一样，currentPartitionId为brokerArray.length整数倍时+1
      nextReplicaShift += 1
    }
    //第一个分区副本的节点
    val firstReplicaIndex = (currentPartitionId + startIndex) % brokerArray.length
    val replicaBuffer = mutable.ArrayBuffer(brokerArray(firstReplicaIndex))
    //该分区剩余副本分配
    for (j <- 0 until replicationFactor - 1)
      replicaBuffer += brokerArray(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerArray.length))
    ret.put(currentPartitionId, replicaBuffer)
    currentPartitionId += 1
  }
  ret
}

//分区副本节点计算
private def replicaIndex(firstReplicaIndex: Int, secondReplicaShift: Int, replicaIndex: Int, nBrokers: Int): Int = {
  val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
  (firstReplicaIndex + shift) % nBrokers
}
```

分配算法主要分为两步：

* 从所有在线broker列表中随机选取一个起始位置，循环分配每个Partition的第一个Replica；
* 以该Partition的第一个Replica所在broker为起点，按照步长依次分配该Partition的剩余Replica。

例：假设一个Kafka集群有5个在线broker节点，其中一个Topic的Partition数为10，每个Partition有3个Replica，且最初随机选择的startIndex和nextReplicaShift节点均为0，计算过程如下：

* P0的第一个副本在`(0+0)%5=0`，第二个副本在`(0+(1+(0+0)%4)))%5=1`，第三副本在`(0+(1+(0+1)%4)))%5=2`，完成后currentPartitionId+1=1，nextReplicaShift=0；
* P1的第一个副本在`(1+0)%5=1`，第二个副本在`(1+(1+(0+0)%4)))%5=2`，第三副本在`(1+(1+(0+1)%4)))%5=3`，完成后currentPartitionId+1=2，nextReplicaShift=0；
* ...
* P4的第一个副本在`(4+0)%5=4`，第二个副本在`(4+(1+(0+0)%4)))%5=0`，第三副本在`(4+(1+(0+1)%4)))%5=1`，完成后currentPartitionId+1=5，nextReplicaShift=0；
* 此时currentPartitionId=5满足条件，nextReplicaShift+1=1，P5的第一个副本在`(5+0)%5=0`，第二个副本在`(0+(1+(1+0)%4)))%5=2`，第三副本在`(0+(1+(1+1)%4)))%5=3`，完成后currentPartitionId+1=6，nextReplicaShift=1；

最终分配结果如下：


| broker-0 | broker-1  | broker-2 | broker-3  | broker-4  |
|----------|-------|----------|----------|----------|
|p0      |p1      |p2      |p3      |p4     | 
|p5      |p6      |p7      |p8      |p9      |
|p4      |p0      |p1      |p2      |p3      |
|p8      |p9      |p5      |p6      |p7      |
|p3      |p4      |p0      |p1      |p2      |
|p7      |p8      |p9      |p5      |p6      |



#### 有机架信息

当所有节点均有机架信息(rack)时，分配方案由assignReplicasToBrokersRackAware()方法实现，该方法保证副本尽量在各机架及各节点间分配均匀，如果分区副本数等于或大于机架数，则将确保每个机架至少获得一个副本，否则，每个机架最多只能获得一个副本。
实现如下：

```
  private def assignReplicasToBrokersRackAware(...): Map[Int, Seq[Int]] = {
    /**
     * 机架节点map，例
     * rack1: 0, 1, 2
     * rack2: 3, 4, 5
     * rack3: 6, 7, 8
     */
    val brokerRackMap = brokerMetadatas.collect { case BrokerMetadata(id, Some(rack)) =>
      id -> rack
    }.toMap
    val numRacks = brokerRackMap.values.toSet.size
    /**
     * 机架交替的broker列表,例
     * 0, 3, 6, 1, 4, 7, 2, 5, 8
     */
    val arrangedBrokerList = getRackAlternatedBrokerList(brokerRackMap)
    val numBrokers = arrangedBrokerList.size
    //结果集
    val ret = mutable.Map[Int, Seq[Int]]()
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(arrangedBrokerList.size)
    var currentPartitionId = math.max(0, startPartitionId)
    var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(arrangedBrokerList.size)
    for (_ <- 0 until nPartitions) {
      if (currentPartitionId > 0 && (currentPartitionId % arrangedBrokerList.size == 0))
        nextReplicaShift += 1
      //循环分配每个分区的第一个副本  
      val firstReplicaIndex = (currentPartitionId + startIndex) % arrangedBrokerList.size
      val leader = arrangedBrokerList(firstReplicaIndex)
      val replicaBuffer = mutable.ArrayBuffer(leader)
      val racksWithReplicas = mutable.Set(brokerRackMap(leader))
      val brokersWithReplicas = mutable.Set(leader)
      var k = 0
      for (_ <- 0 until replicationFactor - 1) {
        var done = false
        while (!done) {
          val broker = arrangedBrokerList(replicaIndex(firstReplicaIndex, nextReplicaShift * numRacks, k, arrangedBrokerList.size))
          val rack = brokerRackMap(broker)
          // Skip this broker if
          // 1. there is already a broker in the same rack that has assigned a replica AND there is one or more racks
          //    that do not have any replica, or
          // 2. the broker has already assigned a replica AND there is one or more brokers that do not have replica assigned
          if ((!racksWithReplicas.contains(rack) || racksWithReplicas.size == numRacks)
              && (!brokersWithReplicas.contains(broker) || brokersWithReplicas.size == numBrokers)) {
            replicaBuffer += broker
            racksWithReplicas += rack
            brokersWithReplicas += broker
            done = true
          }
          k += 1
        }
      }
      ret.put(currentPartitionId, replicaBuffer)
      currentPartitionId += 1
    }
    ret
  }
```

算法实现与无机架信息的基本一致，只是需要将broker列表进一步处理，方便进行主要分为3步：

* 1、构架机架信息交替变化的broker节点列表，见源码注释；
* 2、从机架信息交替的broker列表中随机选取一个起始位置，循环分配每个Partition的第一个Replica；
* 3、分配每一个分区的其余副本，偏向于分配到没有任何该分区副本的机架上；


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

* kafka-topics.sh，用于对Topic进行扩容，即增加Topic的Partition数量(**不支持减少分区**)。
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



# RemoveTopic
