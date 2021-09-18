---
layout: post
title:  "Kafka Producer"
date:   2021-08-28 17:14:02
categories: Kafka
---

生产者负责按照消息中的主题分区将消息发送到对应的Kafka节点，在发送前消息会经过拦截器、序列化器、分区器等一系列的处理，最终通过网络IO发送到Kafka节点中去。KafkaProducer的整体架构如下：

![Kafka 发送流程](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/producer.png)

kafkaProducer由两个线程协调运行，分别为主线程及Sender线程，主线程负责创建消息，并完成序列化、分区选择等处理，并写入RecordAccumulator中。Sender线程负责从
RecordAccumulator中获取消息,并将按TopicPartition归类的消息按照分区Leader节点再次分组，发送到Kafka。发送前，还会将请求保存到InFightRequests中，等待获取发送响应
做进一步处理。


### send方法

**KafkaProducer是线程安全的**，可以在多个线程中共享单个KafkaProducer实例，也可以使用池化思想来进行管理，供其他线程使用。在构建完KafkaProducer及ProducerRecord后，
即可调用KafkaProducer#send方法将消息发送出去，send方法有两种实现，如下：

```
public Future<RecordMetadata> send(ProducerRecord<K, V> record)
public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback)
```

通过这两种方法可实现**消息发送的三种模式：发后即忘、同步、异步**：

* 发后即忘

发后即忘模式不关心消息是否正确到达kafka节点，大多数情况下没什么问题，但发生不可重试异常或重试次数为0时会造成消息丢失。这种模式性能最高，但可靠性也最差。
```
producer.send(record)
```

* 同步

send方法**本身是异步的**，方法返回值为Future对象，**同步模式通过调用Future的get方法来阻塞当前线程，等待Kafka的响应**，直到发送成功或捕获发送异常。可见。同步发送模式下，虽然可靠性高，但性能最差。

```
try {
   //producer.send(record).get();
    Future<RecordMetadata> future= producer.send(record);   
    RecordMetadata meta = future.get(timeout, unit); //超时机制
} catch (RuntimeException exception) {
    //do something
}
```

* 异步

只调用send(record)方法获取Future对象也可实现异步，但这需要引入复杂的业务处理，Kafka重载了send方法，添加了Callback类型参数。Callback接口只有一个方法onCompletion(),
方法的两个参数只会有一个有值，当**发送成功时，Exception为null,失败时RecordMetadata为null**。

```
new Callback(){
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(exception != null){
            //发送失败
        }else {
            //发送成功
        }
    }
}
```

#### 发送异常

消息发送异常可分为两类：**可重试异常、不可重试异常**。当发生不可重试异常时，send方法会直接抛出异常；可重试异常时，如果**retries参数不为0，kafkaProducer在规定的重试次数内会自动重试**，
不会抛出异常，超出次数还未成功时，则会抛出异常，由外层逻辑处理。

可重试异常：TimeoutException、InvalidMetadataException、UnknownTopicOrPartitionException

不可重试异常：InvalidTopicException、RecordTooLargeException、UnknownServerException

### 生产者拦截器

使用生产者连接器可以完成2部分工作，**发送前消息处理以及发送结果回调前处理**，实例为ProducerInterceptor接口的实现类，接口内共有3个方法：
```
public interface ProducerInterceptor<K, V> extends Configurable {
    ProducerRecord<K, V> onSend(ProducerRecord<K, V> record);
    void onAcknowledgement(RecordMetadata metadata, Exception exception);
    void close();
}
```

* onSend

调用KafkaProducer#send()方法时，ProducerInterceptor的onSend()方法会在消息序列化及分区选择前被调用，可以完成消息过滤及修改消息等工作

* onAcknowledgement

消息发送收到应答即成功或失败时被调用，早于用户设定的Callback执行。


### 序列化

生产者需将消息序列化后才可以通过网络IO发送到Kafka节点，同样消费者也需要将消息反序列化后再进行业务处理，且生产者和消费者需要使用对应的序列化器和反序列化器。序列化器是
org.apache.kafka.common.serialization.Serializer的实现类，反序列化器是org.apache.kafka.common.serialization.Deserializer的实现类，Kafka提供了几组默认的实现类
供用户使用：

* StringSerializer、StringDeserializer
* ByteSerializer、ByteDeserializer
* DoubleSerializer、DoubleDeserializer
* ListSerializer、ListDeserializer
* ...

用户也可以实现以上两个接口创建自定义的序列化器和反序列化器；

### 分区器

若ProducerRecord中**partition已有指定值，则直接使用，不再经过分区器**，如果ProducerRecord中partition没有指定值，那需要经过分区器决定消息发往哪个TopicPartition。分区器是org.apache.kafka.clients.producer.Partitioner接口的实现类，接口有三个方法：

```
public interface Partitioner extends Configurable, Closeable {
    //为给定的消息计算分区
    int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster);
    void close();
    default void onNewBatch(String topic, Cluster cluster, int prevPartition) {}
}
```

Kafka提供了DefaultPartitioner、RoundRobinPartitioner、UniformStickyPartitioner三种分区器的实现，默认使用DefaultPartitioner作为生产者端的分区器：

```
public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster,
                         int numPartitions) {
    if (keyBytes == null) {
        return stickyPartitionCache.partition(topic, cluster);
    }
    // hash the keyBytes to choose a partition
    return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
}
```
用户也可自行实现Partitioner接口，创建自定义的分区器。