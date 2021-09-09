---
layout: post
title:  "Kafka Consumer"
date:   2021-09-06 10:42:23
categories: Kafka
---

概念

消费者   消费者组   可扩展性

消费者组是一个逻辑概念，将该组内的消费者归为一类，每个消费者只隶属于一个消费者组，消费者组内订阅同一topic的消费者按照一定的分区分配策略进行消费
，一个分区只能被同一消费者组内的一个消费者消费，消费者组之间不受影响


KafkaConsumer


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


