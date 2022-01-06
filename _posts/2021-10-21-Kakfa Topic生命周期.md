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
> bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 1 --topic my-topic-name
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

handleCreateTopicsRequest()方法的作用是完成调用ZkAdminManager#createTopics()方法前的数据准备，createTopics()方法源码如下：

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

#### ReplicaAssignment

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

##### 无机架信息

当所有broker节点均无机架信息时，分配方案由assignReplicasToBrokersRackUnaware()实现，主要分为两步：



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
      //第一次为0 不会进入，后续副本计算步长自增+1
      //防止 nPartitions 过大时,其中某些partition的分配完全一样
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

##### 有机架信息



#### Zookeeper写入



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

自动创建主题前，会先进行Topic Name的合法性校验，若当前Broker实例的角色为controller，则调用createTopicsInZk()方法完成主题创建，否则向当前集群的controller发送主题创建请求。












# ModifyTopic


# RemoveTopic
