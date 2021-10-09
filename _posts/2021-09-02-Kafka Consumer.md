---
title:  Kafka Consumer
date:   2021-09-02 10:42:23
categories: Kafka
---

Kafka中的Topic是一个逻辑概念，它还可以细分为多个分区(Partition)，一个分区只属于单个主题，消息存储是基于分区进行存储的，每个分区可被视作一个可追加的日志文件，
Producer会在分区日志的尾部追加数据，而Consumer负责订阅Topic，并从主题所属的分区日志的特定的位置(offset)读取并消费数据。

### 消费者组

Kafka引入consumer group的概念来表示一组消费者实例的集合，每个消费者只属于一个消费者组，消费者组内订阅同一topic的消费者按照一定的分区分配策略进行消费，一个TopicPartition只能被同一消费者组
内的一个消费者消费，消费者组之间不受影响。

![consumer group](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/consumer%20group.png)

消费者与消费组这种模型可以让整体的消费能力具备横向伸缩性，我们可以增加（或减少）消费者的个数来提高（或降低）整体的消费能力。对于分区数固定的Topic，一味的增加消费者组内的
消费者数量并不会让消费能力一直得到提升，因为一个TopicPartition只能被同一消费者组内的一个消费者消费，当出现消费者数量大于分区的情况时，就会有消费者分配不到TopicPartition而无法
消费任何消息。

![消费者数量多于主题分区数](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/too%20many%20consumer.png)

以上分配逻辑基于Kafka提供的默认分区分配策略**RangeAssignor**进行分析，根据不同Topic的Producer生产速率及TopicPartition数量，应合理的调整消费者组内订阅该Topic的消费者实例数量，来解决消息堆积或资源浪费的问题。

消息中间件的消息投递模式可分为两类：点对点(P2P)模式和发布订阅(Pub/Sub)模式。得益于消费者与消费者组的模型，Kafka同时支持两种消息投递模式：

* 所有的Consumer实例都属于同一个consumer group，则所有的消息都会被均衡的投递给每一个Consumer，即每条消息只会被一个consumer处理，此时为P2P模式；
* 所有的Consumer实例都属于不同的consumer group，则所有的消息都会被广播给每一个消费者，即每条消息会被所有的Consumer处理，此时为发布订阅模式。


### Consumer Client



#### 订阅主题

消费者进行数据消费时，首先需要完成相关主题的订阅，一个消费者可以订阅一个或多个主题，使用subscribe()方法完成主题订阅，以下为Consumer类内subscribe()方法的重载列表。

```
void subscribe(Collection<String> topics);

void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);

void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

void subscribe(Pattern pattern);
```

subscribe API可分为两类：使用topic集合的方式订阅以及通过正则表达式的方式订阅。但subscribe方法的**多次调用并非增加主题，而是以最后一次调用subscribe方法时提供的主题列表为准**。

ConsumerRebalanceListener参数为消费者再均衡监听器，当分配给消费者的主题分区发生变化时触发回调该Listener，后续分析消费者再均衡时再详解。


#### 分配主题分区

消费者组内订阅同一topic的消费者即可通过配置**分区分配策略进行主题分区自动分配**，也可以使用**assign API完成手动订阅某些主题的特定分区**。但使用**assign方法订阅主题分区的
消费者不具备自动再均衡的功能**，无法实现消费负载均衡及故障自动转移。

##### assign

通过调用KafkaConsumer#assign(Collection)方法实现手动指定主题分区进行消费：

```
void assign(Collection<TopicPartition> partitions);
```

该方法内参数为Collection<TopicPartition>，其中TopicPartition为指定的主题分区，该类只有两个属性：topic和partition，分别代表主题及对应的分区编号：

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





##### ConsumerPartitionAssignor


#### 消息获取












订阅主题，手动指定消费分区或按照分区分配策略分配分区

public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener)


 public void assign(Collection<TopicPartition> partitions) 
 
 
```
public interface ConsumerRebalanceListener {

    void onPartitionsRevoked(Collection<TopicPartition> partitions);

    void onPartitionsAssigned(Collection<TopicPartition> partitions);

    default void onPartitionsLost(Collection<TopicPartition> partitions) {
        onPartitionsRevoked(partitions);
    }
}

```


消费消息  poll()方法


分区针对消费组的消费位移存在内部主题 _consumer_offset主题捏

offset提交

手动提交enable.auto.commit=false

同步提交

commitSync
指定主题分区 offset提交
commitSync(final Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets)

异步提交
commitAsync(OffsetCommitCallback callback)



自动提交  enable.auto.commit=true 

auto.commit.interval.ms = 5 (默认5秒提交一次)

* 重复消费
* 消息丢失   消息消费异步执行，已提交的任务尚未消费，offset已提交

控制消费  

pause(Collection<TopicPartition> partitions)  //暂停消费
resume(Collection<TopicPartition> partitions) //恢复消费


指定offset消费



当一个新的消费者组或消费者组内的消费者订阅了新的主题，或者消费者组的offset信息过期被删除后，都无法在 _consumer_offset主题内找到对应的offset

自动

auto.offset.reset  latest  //   earliest     none 抛出异常


手动 seek()方法


调用poll或assign分配分区，    2  指定偏移量   3 继续消费
poll()/assign()->seek()->poll()


消费者负载均衡及再均衡  高可用性及伸缩性

分区分配策略  实现ConsumerPartitionAssignor接口

kafka提供的实现   
rangAssignor    多组topic，分配不均匀，  负载不均衡，导致部分消费者过载
roundRobinAssignor  将topic分区字典排序，轮询方式逐个分配，组内消费者订阅不同的消息，也会导致分配不均匀

******** StickyAssignor 1 分区分配尽量均匀 2 再分配时，分配结果与上次分配尽量相同，同一个分区尽量分给之前负责的消费者

  
reBalance 原理


角色

消费者协调器  consumerCoordinator

组协调器 GroupCoordinator 


时机 ： 1、新的消费者加入  2、 消费者宕机或退出   3 GroupCoordinator节点变更   4 topic 分区数发生变化

步骤:

1 消费者找到对应的GroupCoordinator 并建立连接   根据groupId hash找到_consumer_offset主题内的分区leader副本

2 发送JoinGroupRequest 
   
   组协调器需要做两件事  1 选举消费者组的Leader，新组，第一个请求的消费者即为Leader,若之前的Leader下线，选举map中的第一个
                       2  选举分区策略  多数消费者支持的策略   
   
3 kafka不参与具体的分配细节，将分区分配的交还给消费者组leader执行，Leader将分区方案返回给组协调器，协调器同步给消费者内的消费者。


4 heartBeat  保持心跳  heartbeat.interval.ms 默认3s   session.timeout.ms

    


消费者拦截器


ConsumerInterceptor   onConsumer   onCommit


