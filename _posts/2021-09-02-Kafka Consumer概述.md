---
title:  Kafka Consumer概述
date:   2021-09-02 10:42:23
categories: Kafka
---

Kafka中的Topic是一个逻辑概念，它还可以细分为多个分区(Partition)，一个分区只属于单个主题，消息存储是基于分区进行存储的，每个分区可被视作一个可追加的日志文件，
Producer会在分区日志的尾部追加数据，而Consumer负责订阅Topic，并从主题所属的分区日志的特定的位置(offset)读取并消费数据。

## 消费者组

Kafka引入consumer group的概念来表示一组消费者实例的集合，每个消费者只属于一个消费者组，消费者组内订阅同一topic的消费者按照一定的分区分配策略进行消费，一个TopicPartition只能被同一消费者组
内的一个消费者消费，消费者组之间不受影响，如下图。

![consumer group](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/consumer%20group.png)

消费者与消费组这种模型可以让整体的消费能力具备横向伸缩性，我们可以增加（或减少）消费者的个数来提高（或降低）整体的消费能力。对于分区数固定的Topic，一味的增加消费者组内的
消费者数量并不会让消费能力一直得到提升，因为一个TopicPartition只能被同一消费者组内的一个消费者消费，当出现消费者数量大于分区的情况时，就会有消费者分配不到TopicPartition而无法
消费任何消息。

![消费者数量多于主题分区数](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/too%20many%20consumer.png)

以上分配逻辑基于Kafka提供的默认分区分配策略**RangeAssignor**进行分析，根据不同Topic的Producer生产速率及TopicPartition数量，应合理的调整消费者组内订阅该Topic的消费者实例数量，来解决消息堆积或资源浪费的问题。

消息中间件的消息投递模式可分为两类：点对点(P2P)模式和发布订阅(Pub/Sub)模式。得益于消费者与消费者组的模型，Kafka同时支持两种消息投递模式：

* 所有的Consumer实例都属于同一个consumer group，则所有的消息都会被均衡的投递给每一个Consumer，即每条消息只会被一个consumer处理，此时为P2P模式；
* 所有的Consumer实例都属于不同的consumer group，则所有的消息都会被广播给每一个消费者，即每条消息会被所有的Consumer处理，此时为发布订阅模式。


## Consumer Client

下面介绍一下Consumer客户端使用时的一些API。

### 订阅主题

消费者进行数据消费时，首先需要完成相关主题的订阅，一个消费者可以订阅一个或多个主题，使用subscribe()方法完成主题订阅，以下为Consumer类内subscribe()方法的重载列表。

```
void subscribe(Collection<String> topics);

void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);

void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

void subscribe(Pattern pattern);
```

subscribe API可分为两类：使用topic集合的方式订阅以及通过正则表达式的方式订阅。但subscribe方法的**多次调用并非增加主题，而是以最后一次调用subscribe方法时提供的主题列表为准**。

ConsumerRebalanceListener参数为消费者再均衡监听器，当分配给消费者的主题分区发生变化时触发回调该Listener，后续分析消费者再均衡时再详解。


### 分配主题分区

消费者组内订阅同一topic的消费者可通过配置**分区分配策略进行主题分区自动分配**，也可以使用**assign()方法完成手动订阅某些主题的特定分区**。但使用**assign方法订阅主题分区的
消费者不具备自动再均衡的功能**，无法实现消费负载均衡及故障自动转移。

#### assign

主题分区信息可通过KafkaConsumer#partitionsFor(topic)方法进行查询获取。然后通过调用KafkaConsumer#assign(Collection)方法实现手动指定主题分区进行消费：

```
List<PartitionInfo> partitionsFor(String topic);

void assign(Collection<TopicPartition> partitions);
```

PartitionInfo类中包含了主题的元数据信息：

```
public class PartitionInfo {
    private final String topic; //主题
    private final int partition; //主题分区编号
    private final Node leader; // 当前leader副本所在节点
    private final Node[] replicas; //分区副本的AR集合
    private final Node[] inSyncReplicas; // 分区副本的ISR集合
    private final Node[] offlineReplicas; //分区副本的OSR集合
    
    //...
}
```

assign方法内参数为Collection<TopicPartition>，其中TopicPartition为指定的主题分区，该类只有两个属性：topic和partition，分别代表主题及对应的分区编号：

```
public final class TopicPartition implements Serializable {
    private final int partition;
    private final String topic;

    public TopicPartition(String topic, int partition) {
        this.partition = partition;
        this.topic = topic;
    }
}

```

#### PartitionAssignor

采用subscribe方法订阅主题的消费者会根据配置的分区分配策略完成消费分区分配，Kafka为用户提供了RangeAssignor、RoundRobinAssignor、StickyAssignor等实现。
用户也可实现AbstractPartitionAssignor接口创建自定义的分区分配策略只需实现AbstractPartitionAssignor中的assign方法即可:
                                             
 ```
 /**
  * org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor
  * @param partitionsPerTopic  <主题-分区编号>集合
  * @param subscriptions  <消费者id-订阅信息>集合
  * @return
  */
 public abstract Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                          Map<String, Subscription> subscriptions);
 ```
 
 Subscription是ConsumerPartitionAssignor的内部类，用来表示消费者的订阅信息：
 
 ```
 final class Subscription {
     private final List<String> topics;  //消费者订阅的主题
     // 用户自定义信息，可自行补充，用于计算分配，单需要实现顶层接口ConsumerPartitionAssignor
     private final ByteBuffer userData;  
     private final List<TopicPartition> ownedPartitions; // 当前消费者已被分配的分区
     private Optional<String> groupInstanceId; //组id
 }
 ```

类的继承结构如下：

<img src="https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/ConsumerPartitionAssignor.png" width="800" height="350"/>


##### RangeAssignor

RangeAssignor是Kafka的默认分区分配策略，原理是使用主题分数区除以消费者数获取跨度，所有消费者按照字典序排列，然后按照跨度进行平均分配，若存在余数，字典序靠前的消费者
会被多分配一个分区，RangeAssignor#assign()方法实现如下：

```
 /**
  * @param partitionsPerTopic  <主题-分区编号>集合
  * @param subscriptions  <消费者id-订阅topic信息>集合
  */
public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
    
    //topic-list<consumer>  按照订阅的topic将consumer分组
    Map<String, List<MemberInfo>> consumersPerTopic = consumersPerTopic(subscriptions);

    Map<String, List<TopicPartition>> assignment = new HashMap<>();
    //初始化<消费者，获得的分区>容器
    for (String memberId : subscriptions.keySet())
        assignment.put(memberId, new ArrayList<>());

    for (Map.Entry<String, List<MemberInfo>> topicEntry : consumersPerTopic.entrySet()) {
        String topic = topicEntry.getKey();
        //订阅该主题的所有消费者
        List<MemberInfo> consumersForTopic = topicEntry.getValue();
        //主题分区数
        Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
        if (numPartitionsForTopic == null)
            continue;
        //按消费者id排序
        Collections.sort(consumersForTopic);
        //跨度 =  主题分区数 / 订阅主题的消费者数
        int numPartitionsPerConsumer = numPartitionsForTopic / consumersForTopic.size();
        //余数
        int consumersWithExtraPartition = numPartitionsForTopic % consumersForTopic.size();

        List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);
        for (int i = 0, n = consumersForTopic.size(); i < n; i++) {
            //遍历消费者，依次分配，余数也依次分配
            int start = numPartitionsPerConsumer * i + Math.min(i, consumersWithExtraPartition);
            int length = numPartitionsPerConsumer + (i + 1 > consumersWithExtraPartition ? 0 : 1);
            assignment.get(consumersForTopic.get(i).memberId).addAll(partitions.subList(start, start + length));
        }
    }
    return assignment;
}
```

![RangeAssignor](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/RangeAssignor.png)

可以看出，当策略为RangeAssignor时，由于主题分区数多数情况下并非消费者数的整数倍，随着消费者订阅的Topic增加，**容易出现部分消费者过载**。


##### RoundRobinAssignor

RoundRobinAssignor分配策略的原理是将消费组内所有消费者及消费者订阅的所有主题的分区按照字典序排序，然后通过轮询方式逐个将分区分配给每个消费者。RoundRobinAssignor#assign()实现如下：

```
 /**
  * @param partitionsPerTopic  <主题-分区编号>集合
  * @param subscriptions  <消费者id-订阅topic信息>集合
  */
public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                Map<String, Subscription> subscriptions) {
    Map<String, List<TopicPartition>> assignment = new HashMap<>();
    List<MemberInfo> memberInfoList = new ArrayList<>();
    for (Map.Entry<String, Subscription> memberSubscription : subscriptions.entrySet()) {
        //初始化<消费者id，获得的分区>容器
        assignment.put(memberSubscription.getKey(), new ArrayList<>());
        //所有的消费者信息
        memberInfoList.add(new MemberInfo(memberSubscription.getKey(),
                                          memberSubscription.getValue().groupInstanceId()));
    }
    /**
     * 所有消费者集合的无限循环迭代器 通过重复获取集合的迭代器实现
     * private T advance() {
     *     if (!iterator.hasNext()) {
     *         iterator = iterable.iterator();
     *     }
     *     return iterator.next();
     * }
     */
    CircularIterator<MemberInfo> assigner = new CircularIterator<>(Utils.sorted(memberInfoList));
    //遍历所有分区 
    for (TopicPartition partition : allPartitionsSorted(partitionsPerTopic, subscriptions)) {
        final String topic = partition.topic();
        //查找订阅该分区的消费者
        while (!subscriptions.get(assigner.peek().memberId).topics().contains(topic))
            assigner.next();
        assignment.get(assigner.next().memberId).add(partition);
    }
    return assignment;
}


private List<TopicPartition> allPartitionsSorted(Map<String, Integer> partitionsPerTopic,
                                                 Map<String, Subscription> subscriptions) {
    SortedSet<String> topics = new TreeSet<>();
    for (Subscription subscription : subscriptions.values())
        topics.addAll(subscription.topics());
    //按照主题排序所有的主题分区
    List<TopicPartition> allPartitions = new ArrayList<>();
    for (String topic : topics) {
        Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
        if (numPartitionsForTopic != null)
            allPartitions.addAll(AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic));
    }
    return allPartitions;
}
```

![RoundRobinAssignor](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/RoundRobinAssignor.png)

如上图，轮询分配的策略**在同一个消费者组内的所有消费者都订阅相同Topic时，分配是均匀的，消费者费配到的分区数的差值不超过一。当同一个消费者组内的消费都订阅不同Topic时，则可能导致分配不均匀**，上图所示的第二个例子中，
完全可以将Topic-B_1的主题分区分给consumer-1处理。减轻consumer-2的压力。

##### StickyAssignor

StickyAssignor的设计有两个目标：

(1) 分区的分配要尽可能均匀：

![StickyAssignor](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/StickyAssignor.png)

(2) 分区的分配尽可能的与上次分配的结果保持相同：

![StickyAssignor](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/StickyAssignor-2.png)


当以上两者发生冲突时，第一个目标优于第二个目标。StickyAssignor分配策略比另外两者分配策略而言显得更加优异，既能最大程度的保证分配均匀，也能够减少不必要的分区移动。
StickyAssignor继承自AbstractStickyAssignor，assign方法实现如下：

```
public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                Map<String, Subscription> subscriptions) {
    Map<String, List<TopicPartition>> consumerToOwnedPartitions = new HashMap<>();
    Set<TopicPartition> partitionsWithMultiplePreviousOwners = new HashSet<>();
    //    
    if (allSubscriptionsEqual(partitionsPerTopic.keySet(), subscriptions, consumerToOwnedPartitions, partitionsWithMultiplePreviousOwners)) {
        //所有消费者订阅相同的topic
        partitionsTransferringOwnership = new HashMap<>();
        return constrainedAssign(partitionsPerTopic, consumerToOwnedPartitions, partitionsWithMultiplePreviousOwners);
    } else {
        partitionsTransferringOwnership = null;
        return generalAssign(partitionsPerTopic, subscriptions);
    }
}
```
StickyAssignor中根据所有消费者是否订阅相同的topic分别做了实现：constrainedAssign()和generalAssign()。

###### constrainedAssign

当组内所有消费者订阅相同Topic时，分配方案由constrainedAssign()产生，源码如下：

```
private Map<String, List<TopicPartition>> constrainedAssign(Map<String, Integer> partitionsPerTopic,
                                                            //<消费者,上次分配但仍然有效的分区(没有为空集合)>
                                                            Map<String, List<TopicPartition>> consumerToOwnedPartitions,
                                                            Set<TopicPartition> partitionsWithMultiplePreviousOwners) {
    //所有主题分区
    SortedSet<TopicPartition> unassignedPartitions = getTopicPartitions(partitionsPerTopic);

    Set<TopicPartition> allRevokedPartitions = new HashSet<>();

    // the consumers not yet at capacity
    List<String> unfilledMembers = new LinkedList<>();
    // the members with exactly maxQuota partitions assigned
    Queue<String> maxCapacityMembers = new LinkedList<>();
    // the members with exactly minQuota partitions assigned
    Queue<String> minCapacityMembers = new LinkedList<>();

    int numberOfConsumers = consumerToOwnedPartitions.size();
    //平均分配 主题分区数 / 消费者数  向上取整 最大值 向下取整 最小值  保证各消费者获得分区差值不超过1
    int minQuota = (int) Math.floor(((double) unassignedPartitions.size()) / numberOfConsumers);
    int maxQuota = (int) Math.ceil(((double) unassignedPartitions.size()) / numberOfConsumers);

    //初始化结果集 list.size == minQuota
    Map<String, List<TopicPartition>> assignment = new HashMap<>(
        consumerToOwnedPartitions.keySet().stream().collect(Collectors.toMap(c -> c, c -> new ArrayList<>(minQuota))));

    for (Map.Entry<String, List<TopicPartition>> consumerEntry : consumerToOwnedPartitions.entrySet()) {
        String consumer = consumerEntry.getKey();
        //上一次分配的有效分区(订阅主题未改变)
        List<TopicPartition> ownedPartitions = consumerEntry.getValue();
        //初始化结果集的空list
        List<TopicPartition> consumerAssignment = assignment.get(consumer);

        for (TopicPartition doublyClaimedPartition : partitionsWithMultiplePreviousOwners) {
            if (ownedPartitions.contains(doublyClaimedPartition)) {
                log.error("Found partition {} still claimed as owned by consumer {}, despite being claimed by multiple "
                        + "consumers already in the same generation. Removing it from the ownedPartitions",
                    doublyClaimedPartition, consumer);
                //移除被多消费者声明消费的分区
                ownedPartitions.remove(doublyClaimedPartition);
            }
        }

        int i = 0;
        //分配之前持有的旧分区 分区的分配尽可能的与上次分配的结果保持相同
        for (TopicPartition tp : ownedPartitions) {
            if (i < maxQuota) {
                //未到上限，继续持有之前的分区
                consumerAssignment.add(tp);
                unassignedPartitions.remove(tp);
            } else {
                //超出后，将剩余分区添加到未分配分区集合中，分配给其他消费者
                allRevokedPartitions.add(tp);
            }
            ++i;
        }

        if (ownedPartitions.size() < minQuota) {
            //之前持有的分区数量 低于平均下限
            unfilledMembers.add(consumer);
        } else {
            // It's possible for a consumer to be at both min and max capacity if minQuota == maxQuota
            if (consumerAssignment.size() == minQuota)
                minCapacityMembers.add(consumer);
            if (consumerAssignment.size() == maxQuota)
                maxCapacityMembers.add(consumer);
        }
    }
    //将持有分区数低于下限的消费者排序
    Collections.sort(unfilledMembers);
    Iterator<TopicPartition> unassignedPartitionsIter = unassignedPartitions.iterator();

    // Fill remaining members up to minQuota
    while (!unfilledMembers.isEmpty() && !unassignedPartitions.isEmpty()) {
        Iterator<String> unfilledConsumerIter = unfilledMembers.iterator();

        while (unfilledConsumerIter.hasNext()) {
            String consumer = unfilledConsumerIter.next();
            List<TopicPartition> consumerAssignment = assignment.get(consumer);

            if (unassignedPartitionsIter.hasNext()) {
                TopicPartition tp = unassignedPartitionsIter.next();
                consumerAssignment.add(tp);
                unassignedPartitionsIter.remove();
                // We already assigned all possible ownedPartitions, so we know this must be newly to this consumer
                if (allRevokedPartitions.contains(tp) || partitionsWithMultiplePreviousOwners.contains(tp))
                    partitionsTransferringOwnership.put(tp, consumer);
            } else {
                break;
            }

            if (consumerAssignment.size() == minQuota) {
                //获取的分区数到达下限
                minCapacityMembers.add(consumer);
                unfilledConsumerIter.remove();
            }
        }
    }

    // 所有分区已全部分配，但仍有分区数低于minQuota的消费者  尝试从分区数为maxQuota的消费者偷取分区
    for (String consumer : unfilledMembers) {
        List<TopicPartition> consumerAssignment = assignment.get(consumer);
        //还需分配的分区数
        int remainingCapacity = minQuota - consumerAssignment.size();
        while (remainingCapacity > 0) {
            //获取maxQuota消费者
            String overloadedConsumer = maxCapacityMembers.poll();
            if (overloadedConsumer == null) {
                throw new IllegalStateException("Some consumers are under capacity but all partitions have been assigned");
            }
            //偷取一个分区
            TopicPartition swappedPartition = assignment.get(overloadedConsumer).remove(0);
            consumerAssignment.add(swappedPartition);
            --remainingCapacity;
            // This partition is by definition transferring ownership, the swapped partition must have come from
            // the max capacity member's owned partitions since it can only reach max capacity with owned partitions
            partitionsTransferringOwnership.put(swappedPartition, consumer);
        }
        minCapacityMembers.add(consumer);
    }

    // 所有消费者的分区数均以大于等于minQuota,但仍有未分配的分区，将剩余分区分配给minQuota的消费者
    for (TopicPartition unassignedPartition : unassignedPartitions) {
        String underCapacityConsumer = minCapacityMembers.poll();
        if (underCapacityConsumer == null) {
            throw new IllegalStateException("Some partitions are unassigned but all consumers are at maximum capacity");
        }
        assignment.get(underCapacityConsumer).add(unassignedPartition);

        if (allRevokedPartitions.contains(unassignedPartition) || partitionsWithMultiplePreviousOwners.contains(unassignedPartition))
            partitionsTransferringOwnership.put(unassignedPartition, underCapacityConsumer);
    }

    log.info("Final assignment of partitions to consumers: \n{}", assignment);

    return assignment;
}

```

因为组内所有消费者均订阅相同的Topic，通过对分区数除以消费者数的结果进行向上及向下取整，即得到每个消费者可分配分区数的上限maxQuota及minQuota，保证任意消费间分配的分区数差值
不会超过1，具体分配可概括为以下3步：

* 1、先为消费者分配上次分配结果中仍有效的分区，**保证尽可能的与上次分配的结果保持相同**，若超过maxQuota，将剩余分区标记，后续分配给其他消费者；
* 2、经过步骤1后，所有主题分区已全部分配，但仍有获得分区数低于minQuota的消费者，遍历获得分区数为maxQuota的消费者列表，并偷取1个分区，直至满足minQuota；
* 3、所有消费者的分区数均以大于等于minQuota,但仍有未分配的分区，将剩余分区分配给minQuota的消费者。

###### generalAssign

当组内存在订阅不一致的消费者时，分配方案由generalAssign()方法产生，源码如下：

```
private Map<String, List<TopicPartition>> generalAssign(Map<String, Integer> partitionsPerTopic,
                                                        Map<String, Subscription> subscriptions) {
    //消费者上一次被分配分区情况
    Map<String, List<TopicPartition>> currentAssignment = new HashMap<>();
    //上一次主题分区对应的消费者
    Map<TopicPartition, ConsumerGenerationPair> prevAssignment = new HashMap<>();
    partitionMovements = new PartitionMovements();
    //填充上述两个集合
    prepopulateCurrentAssignments(subscriptions, currentAssignment, prevAssignment);

    //因为订阅的主题是不同的 记录当前分区可以分配给哪些消费者
    // a mapping of all topic partitions to all consumers that can be assigned to them
    final Map<TopicPartition, List<String>> partition2AllPotentialConsumers = new HashMap<>();
    //记录消费者可以被分配分区列表
    // a mapping of all consumers to all potential topic partitions that can be assigned to them
    final Map<String, List<TopicPartition>> consumer2AllPotentialPartitions = new HashMap<>();

    // initialize partition2AllPotentialConsumers and consumer2AllPotentialPartitions in the following two for loops
    for (Entry<String, Integer> entry: partitionsPerTopic.entrySet()) {
        for (int i = 0; i < entry.getValue(); ++i)
            partition2AllPotentialConsumers.put(new TopicPartition(entry.getKey(), i), new ArrayList<>());
    }

    for (Entry<String, Subscription> entry: subscriptions.entrySet()) {
        String consumerId = entry.getKey();
        consumer2AllPotentialPartitions.put(consumerId, new ArrayList<>());
        entry.getValue().topics().stream().filter(topic -> partitionsPerTopic.get(topic) != null).forEach(topic -> {
            for (int i = 0; i < partitionsPerTopic.get(topic); ++i) {
                TopicPartition topicPartition = new TopicPartition(topic, i);
                consumer2AllPotentialPartitions.get(consumerId).add(topicPartition);
                partition2AllPotentialConsumers.get(topicPartition).add(consumerId);
            }
        });

        // 新增消费者
        if (!currentAssignment.containsKey(consumerId))
            currentAssignment.put(consumerId, new ArrayList<>());
    }

    // 维护上一次分配中，<主题分区-消费者>的关系
    Map<TopicPartition, String> currentPartitionConsumer = new HashMap<>();
    for (Map.Entry<String, List<TopicPartition>> entry: currentAssignment.entrySet())
        for (TopicPartition topicPartition: entry.getValue())
            currentPartitionConsumer.put(topicPartition, entry.getKey());
    //对所有分区进行排序
    List<TopicPartition> sortedPartitions = sortPartitions(partition2AllPotentialConsumers);

    // all partitions that need to be assigned (initially set to all partitions but adjusted in the following loop)
    List<TopicPartition> unassignedPartitions = new ArrayList<>(sortedPartitions);
    boolean revocationRequired = false;
    for (Iterator<Entry<String, List<TopicPartition>>> it = currentAssignment.entrySet().iterator(); it.hasNext();) {
        Map.Entry<String, List<TopicPartition>> entry = it.next();
        if (!subscriptions.containsKey(entry.getKey())) {
            //消费者下线 移除
            // if a consumer that existed before (and had some partition assignments) is now removed, remove it from currentAssignment
            for (TopicPartition topicPartition: entry.getValue())
                currentPartitionConsumer.remove(topicPartition);
            it.remove();
        } else {
            // otherwise (the consumer still exists)
            for (Iterator<TopicPartition> partitionIter = entry.getValue().iterator(); partitionIter.hasNext();) {
                TopicPartition partition = partitionIter.next();
                if (!partition2AllPotentialConsumers.containsKey(partition)) {
                    //主题分区不存在（主题分区数配置降低了）
                    // if this topic partition of this consumer no longer exists remove it from currentAssignment of the consumer
                    partitionIter.remove();
                    currentPartitionConsumer.remove(partition);
                } else if (!subscriptions.get(entry.getKey()).topics().contains(partition.topic())) {
                    //消费者不再订阅该主题
                    partitionIter.remove();
                    revocationRequired = true;
                } else
                    unassignedPartitions.remove(partition);
            }
        }
    }

    //按照消费者获取的分区数升序排序
    TreeSet<String> sortedCurrentSubscriptions = new TreeSet<>(new SubscriptionComparator(currentAssignment));
    sortedCurrentSubscriptions.addAll(currentAssignment.keySet());
    //进行平衡操作
    balance(currentAssignment, prevAssignment, sortedPartitions, unassignedPartitions, sortedCurrentSubscriptions,
        consumer2AllPotentialPartitions, partition2AllPotentialConsumers, currentPartitionConsumer, revocationRequired);
    return currentAssignment;
}
```

generalAssign()方法主要是校验上一次分配的结果，将仍有效的分区分配继续保留给消费者，并删除已无效的消费者(消费者下线)和主题分区(主题分区数配置降低)，然后调用
balance()方法完成分区分配及平衡操作。


```
private void balance(Map<String, List<TopicPartition>> currentAssignment,
                     Map<TopicPartition, ConsumerGenerationPair> prevAssignment,
                     List<TopicPartition> sortedPartitions,
                     List<TopicPartition> unassignedPartitions,
                     TreeSet<String> sortedCurrentSubscriptions,
                     Map<String, List<TopicPartition>> consumer2AllPotentialPartitions,
                     Map<TopicPartition, List<String>> partition2AllPotentialConsumers,
                     Map<TopicPartition, String> currentPartitionConsumer,
                     boolean revocationRequired) {
    //initializing == true时,当前所有消费者均未分配到主题分区
    boolean initializing = currentAssignment.get(sortedCurrentSubscriptions.last()).isEmpty();
    boolean reassignmentPerformed = false;

    // 遍历未分配的分区
    for (TopicPartition partition: unassignedPartitions) {
        // skip if there is no potential consumer for the partition
        if (partition2AllPotentialConsumers.get(partition).isEmpty())
            continue;
        //将该分区分配给sortedCurrentSubscriptions(按订阅分区数升序排列的消费者)中第一个订阅该主题的消费者
        assignPartition(partition, sortedCurrentSubscriptions, currentAssignment,
            consumer2AllPotentialPartitions, currentPartitionConsumer);
    }

    //分配结果已固定的分区，即只有一个消费者订阅该主题
    Set<TopicPartition> fixedPartitions = new HashSet<>();
    for (TopicPartition partition: partition2AllPotentialConsumers.keySet())
        if (!canParticipateInReassignment(partition, partition2AllPotentialConsumers))
            fixedPartitions.add(partition);
    sortedPartitions.removeAll(fixedPartitions);
    unassignedPartitions.removeAll(fixedPartitions);


    Map<String, List<TopicPartition>> fixedAssignments = new HashMap<>();
    for (String consumer: consumer2AllPotentialPartitions.keySet())
        if (!canParticipateInReassignment(consumer, currentAssignment,consumer2AllPotentialPartitions, partition2AllPotentialConsumers)) {
            //不可继续获取分区的消费者 该消费者当前分配的数量 = 能够最大分配数量(理论上consumer2AllPotentialPartitions)
            //且 分配的分区不能分配给其他消费者 （只有当前消费者订阅了该主题）
            sortedCurrentSubscriptions.remove(consumer);
            fixedAssignments.put(consumer, currentAssignment.remove(consumer));
        }

    // create a deep copy of the current assignment so we can revert to it if we do not get a more balanced assignment later
    Map<String, List<TopicPartition>> preBalanceAssignment = deepCopy(currentAssignment);
    Map<TopicPartition, String> preBalancePartitionConsumers = new HashMap<>(currentPartitionConsumer);

    // if we don't already need to revoke something due to subscription changes, first try to balance by only moving newly added partitions
    if (!revocationRequired) {
        //不存在消费者取消订阅Topic的情况，使用unassignedPartitions执行一次重分配
        performReassignments(unassignedPartitions, currentAssignment, prevAssignment, sortedCurrentSubscriptions,
            consumer2AllPotentialPartitions, partition2AllPotentialConsumers, currentPartitionConsumer);
    }
    //执行所有分区重分配
    reassignmentPerformed = performReassignments(sortedPartitions, currentAssignment, prevAssignment, sortedCurrentSubscriptions,
               consumer2AllPotentialPartitions, partition2AllPotentialConsumers, currentPartitionConsumer);
    
    if (!initializing && reassignmentPerformed && getBalanceScore(currentAssignment) >= getBalanceScore(preBalanceAssignment)) {
        // 回退到进行重分配之前的方案
        deepCopy(preBalanceAssignment, currentAssignment);
        currentPartitionConsumer.clear();
        currentPartitionConsumer.putAll(preBalancePartitionConsumers);
    }

    // 将之前的fixedAssignments重新添加到最终的分配方案中
    for (Entry<String, List<TopicPartition>> entry: fixedAssignments.entrySet()) {
        String consumer = entry.getKey();
        currentAssignment.put(consumer, entry.getValue());
        sortedCurrentSubscriptions.add(consumer);
    }

    fixedAssignments.clear();
}
```

balance()方法可总结为以下几点：

1、首先将未分配的主题分区分配给sortedCurrentSubscriptions(按订阅分区数升序排列的消费者)中第一个订阅该主题的消费者，完成所有分区的初次分配；
2、消费者只订阅了一个主题且该主题也只被这一个消费者订阅的情况，此时**分配方案已固定**，此消费者及消息的所有分区均不会参与重分配；
3、调用performReassignments()方法进行重新分配；
4、重分配后的BalanceScore(所有消费者分配的分区数差异的总和)高于重分配前，则回退；


performReassignments()的方法实现如下：

```
private boolean performReassignments(List<TopicPartition> reassignablePartitions,
                                     Map<String, List<TopicPartition>> currentAssignment,
                                     Map<TopicPartition, ConsumerGenerationPair> prevAssignment,
                                     TreeSet<String> sortedCurrentSubscriptions,
                                     Map<String, List<TopicPartition>> consumer2AllPotentialPartitions,
                                     Map<TopicPartition, List<String>> partition2AllPotentialConsumers,
                                     Map<TopicPartition, String> currentPartitionConsumer) {
    boolean reassignmentPerformed = false;
    boolean modified;

    // repeat reassignment until no partition can be moved to improve the balance
    do {
        modified = false;
        // reassign all reassignable partitions (starting from the partition with least potential consumers and if needed)
        Iterator<TopicPartition> partitionIterator = reassignablePartitions.iterator();

        // 可重新分配的分区全部已处理 或 已到达balance(消费者未持有所有可获取的主题分区，且存在一个消费者获取了任一分区但其所持有的分区数多余当前消费者，则不平衡)
        while (partitionIterator.hasNext() && !isBalanced(currentAssignment, sortedCurrentSubscriptions, consumer2AllPotentialPartitions)) {
            TopicPartition partition = partitionIterator.next();

            // the partition must have at least two consumers
            if (partition2AllPotentialConsumers.get(partition).size() <= 1)
                log.error("Expected more than one potential consumer for partition '{}'", partition);

            // the partition must have a current consumer
            String consumer = currentPartitionConsumer.get(partition);
            if (consumer == null)
                log.error("Expected partition '{}' to be assigned to a consumer", partition);

            //当前分区所属的消费者持有的分区数 大于 当前分区上一次分配中被分配到的消费者
            if (prevAssignment.containsKey(partition) &&
                currentAssignment.get(consumer).size() > currentAssignment.get(prevAssignment.get(partition).consumer).size() + 1) {
                //将该分区重新分配给之前的consumer
                reassignPartition(partition, currentAssignment, sortedCurrentSubscriptions, currentPartitionConsumer, prevAssignment.get(partition).consumer);
                reassignmentPerformed = true;
                modified = true;
                continue;
            }

            for (String otherConsumer: partition2AllPotentialConsumers.get(partition)) {
                //当前分区所属的消费者持有的分区数 大于 该分区可分配的otherConsumer ，重分配
                if (currentAssignment.get(consumer).size() > currentAssignment.get(otherConsumer).size() + 1) {
                    reassignPartition(partition, currentAssignment, sortedCurrentSubscriptions, currentPartitionConsumer, consumer2AllPotentialPartitions);
                    reassignmentPerformed = true;
                    modified = true;
                    break;
                }
            }
        }
    } while (modified);

    return reassignmentPerformed;
}
```

重分配决定主题分区归属时，有两层判断：

* 1、分区当前所属的消费者持有的分区数大于分区上一次分配中所属的消费者持有的分区数，将分区转移；
* 2、分区当前所属的消费者持有的分区数大于其它可获取该分区的消费者持有的分区数，将分区转移。

### 消息获取

消息的消费一般有两种模式：push和poll模式。push模式是服务端主动将消息推送给消费者，poll模式是消费者主动向服务端发起请求来拉取消息。Kafka中的消费时基于poll模式的。
通过不断轮询调用KafkaConsumer#poll(java.time.Duration)方法，来获取消费者所分配的主题分区上的一组消息。

```
public ConsumerRecords<K, V> poll(final Duration timeout) {
    return poll(time.timer(timeout), true);
}
```


### 反序列化

KafkaProducer发送消息时会将消息序列化，对应的，KafkaConsumer也需要将消息反序列化，反序列化器是org.apache.kafka.common.serialization.Deserializer的实现类，
Kafka提供以下的实现类供用户使用：
                                                      
* StringDeserializer
* ByteDeserializer
* DoubleDeserializer
* ListDeserializer
* ...

用户也可以实现Deserializer接口创建自定义的反序列化器。


### 消费者拦截器

消费者拦截器主要在拉取到消息或在提交消费位移时进行一些定制化的操作。接口为org.apache.kafka.clients.consumer.ConsumerInterceptor：

```
public interface ConsumerInterceptor<K, V> extends Configurable, AutoCloseable {
    /**
     * This is called just before the records are returned by KafkaConsumer#poll(java.time.Duration)}
     * @param records records to be consumed by the client or records returned by the previous interceptors in the list.
     */
    ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records);
    /**
     * This is called when offsets get committed.
     * @param offsets A map of offsets by partition with associated metadata
     */
    void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets);

    void close();
}
```
KafkaConsumer会在poll()方法返回之前调用拦截器的onConsume()方法来对消息进行相应的定制化操作，比如修改返回的消息内容、按照某种规则过滤消息，如根TTL功能。据消息时间戳判断消息是否有效，
可能会减少poll()方法返回的消息的个数。 如果onConsume()方法中抛出异常， 那么会被捕获并记录到日志中， 但是异常不会再向上传递。

**KafkaConsumer会在提交完消费位移之后调用拦截器的onCommit()方法， 可以使用这个方法来记录跟踪所提交的位移信息**。

### 位移提交

Kafka中的每个主题分区内的消息都有唯一的offset，为了防止消费进度丢失，消费者通过向服务器提交offset来表示当前消费到分区中消息的位置。poll()方法返回的是未被消息过的消息，消费者需要每次将消费进度(消费的消息对应的offset)保存到Kafka
的内部主题`_consumer_offsets`中，即消费位移提交。若位移提交出现问题，会导致重复消费或消息丢失的现象。**提交的位置为当前已消费的消息offset+1，即下一次需消费的起始位置**。

```
consumer.commitSync(Collections.singletonMap(partition,new OffsetAndMetadata(record.offset()+1)));
```

消费者的位移提交可分为自动提交和手动提交两种方式：

#### 自动提交

自动提交是KafkaConsumer默认的消费位移提交方式，通过客户端参数`enable.auto.commit`配置，默认为true,自动提交是指Kafka按照一定的时间周期进行消费位移的提交，通过参数
`auto.interval.ms`配置，默认5s，即默认情况下，consumer会每隔5s将拉取到的每个分区中的最大消息位移提交到主题`_consumer_offsets`中。

自动提交是延时提交，当consumer实例突然崩溃时，可能会导致已消费的消息位移尚未提交，consumer group发生rebalance，导致消息被**重复消费**。当消费者处理业务逻辑为先缓存消息，
再进行消费时，可能发生消息位移已提交，但消息还在缓存队列内，未被消费，导致**消息丢失**情况的出现。

综上：自动提交下，无需开发人员额外编码，代码简洁，但可能导致重复消费及消息丢失的问题。即使通过缩短提交周期也无法避免，且会使位移提交更加频繁。

#### 手动提交

手动提交的方式下，开发人员可以控制何时进行消费位移提交，通过`enable.auto.commit=false`开启手动提交方式，手动提交可分为同步提交和异步提交：

* 同步提交

同步提交会阻塞消费者线程直至发送成功或发生不可重试异常，抛出异常（详见org.apache.kafka.clients.consumer.internals.ConsumerCoordinator#commitOffsetsSync），
通过调用KafkaConsumer#commitSync()方式实现，KafkaConsumer提供了以下4个重载方法，控制手动提交的粒度（如单消息、单分区、多分区或全部）及阻塞时间。

```
void commitSync();

void commitSync(Duration timeout);

void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);

void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout);
```

* 异步提交 

异步提交对比同步提交的不同点在于执行提交时消费者线程不会阻塞，发生任何异常均不会重试。异步提交可设置OffsetCommitCallback回调函数，位移提交完成后，会调用其onComplete()方法，可通过
判断参数Exception是否为null，来判断是否发送成功。

```
void commitAsync();

void commitAsync(OffsetCommitCallback callback);

void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);
```

当异步提交的异常时，可进行手动重试，但应注意"ABA"的问题，防止重复请求时将其它位移提交请求覆盖。可通过维护递增的请求id，如已有更大的请求id存在，应放弃重试。考虑到后续也会再次发起同步请求，也可以
不进行重试，但会增加重复消费的概率。在消费者退出或再均衡前也可采用同步提交的方式确保正确提交。

### 消费控制

#### 指定消费位移

* auto.offset.reset

消费者拉取消息时，可能在服务端无法查找到对应的消费位移，如消费者的组为新建立的consumer group、消费者组内一个新的consumer订阅了新的topic或服务端_consumer_offsets主题内
关于这个消费组的位移信息因过期被移除。此时KafkaConsumer会采用auto.offset.reset配置来决定从何处开始进行消费.

    auto.offset.reset共有3个可选值：
     * latest  默认值，表示从分区末尾开始消费
     * earliest  从分区起始处开始消费
     * none  当出现找不到消费位移时，抛出NoOffsetForPartitionException

* seek

seek方法提供了从特定偏移量进行读取消息的能力，即可向前跳过若干消息，也可向后回溯若干消息，为消息的消费提供了很大的灵活性。得益于seek()方法，**可以无需依赖Broker端的_consumer_offsets内部主题存储消
费位移，可以先将消息位移存储到任意的存储介质中，如DB，文件系统等。下次消费时，读取之前存储的位移，并通过seek()方法指向该位置继续消费**。

```
void seek(TopicPartition partition, long offset);  //指定偏移量

void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata);

void seekToBeginning(Collection<TopicPartition> partitions);  //分区起始位置

void seekToEnd(Collection<TopicPartition> partitions); //分区末尾
```

KafkaConsumer提供了一些获取分区特定偏移量的方法：

```
//方法会返回时间戳大于等于待查询时间的第一条消息对应的位置和时间戳， 对应于OffsetAndTimestamp中的offset和timestamp字段
Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch);
Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout);

//功能与seekToBeginning相同
Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions);
Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout);

//功能与直接使用seekToEnd相同 
Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions);
Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout);

```

调用seek()方法前，必须先完成分区分配。调用流程为`poll()/assign()->seek()->poll()`。

#### pause & resume

KafkaConsumer提供了pause()用于暂停poll()方法返回某些分区的消息而先消费其他分区，且不会导致group rebalance。后续通过调用pause()方法恢复对这些分区的继续消费。

```
void pause(Collection<TopicPartition> partitions);

void resume(Collection<TopicPartition> partitions);
```

被暂停的分区可通过paused()方法获取：

```
Set<TopicPartition> paused();
```


#### wakeup & close

无论使用`while-true`或`while-isRunning.get()`循环调用poll()方法获取消息，都可以通过调用wakeup()方法退出poll逻辑。wakeup()方法是线程安全的，调用后poll方法会抛出WakeupExceptions完成循环跳出。
退出poll循环后，需要调用close()方法，释放消费者占用的系统资源，若采用自动提交的方式，此时，也会完成消息位移的提交。

```
void wakeup();

void close(); //默认30s

void close(Duration timeout); //自定义等待时间
```

    



