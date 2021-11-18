---
layout: post
title:  "Kafka Producer 概述"
date:   2021-08-03 17:14:02
categories: Kafka
---

KafkaProducer的职责是将消息发送到Broker端，用户使用时，只需调用KafkaProducer#send方法即可完成，本文将通过KafkaProducer#send方法源码来分析Kakfa消息发送的流程。

### KafkaProducer#send

send方法有两种实现，如下：

```
//异步的发送一条消息
public Future<RecordMetadata> send(ProducerRecord<K, V> record) { return send(record, null);}
//异步的发送消息，发送确认后唤起回调函数Callback
public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    //拦截器
    ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
    return doSend(interceptedRecord, callback);
}
```

Kafka中消息被封装为ProducerRecord对象，属性如下：

```
public class ProducerRecord<K, V> {

    private final String topic; //消息主题
    private final Integer partition; //消息分区
    private final Headers headers; //消息头部信息
    private final K key; //消息 key
    private final V value;  //消息值
    private final Long timestamp; //时间戳
    ...
}
```

通过这两种方法可实现**消息发送的三种模式：发后即忘、同步、异步**：

* 发后即忘

发后即忘模式不关心消息是否正确到达kafka节点，大多数情况下没什么问题，但发生不可重试异常或剩余重试次数为0时会造成消息丢失。这种模式性能最高，但可靠性也最差。
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

这两个方法只是简单API，消息ProducerRecord经过**拦截器**处理后，是调用**doSend()**方法实现发送。

### KafkaProducer.doSend

doSend方法源码如下：

```
private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
    TopicPartition tp = null;
    try {
        throwIfProducerClosed();
        long nowMs = time.milliseconds();
        ClusterAndWaitTime clusterAndWaitTime;
        try {
            //阻塞等待主题元数据有效
            clusterAndWaitTime = waitOnMetadata(record.topic(), record.partition(), nowMs, maxBlockTimeMs);
        } catch (KafkaException e) {
            if (metadata.isClosed()) throw new KafkaException("Producer closed while send in progress", e);
            throw e;
        }
        nowMs += clusterAndWaitTime.waitedOnMetadataMs;
        long remainingWaitMs = Math.max(0, maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs);
        Cluster cluster = clusterAndWaitTime.cluster;
        byte[] serializedKey;
        try {
            // key序列化
            serializedKey = keySerializer.serialize(record.topic(), record.headers(), record.key());
        } catch (ClassCastException cce) {
            throw new SerializationException(...);
        }
        byte[] serializedValue;
        try {
            //value序列化 
            serializedValue = valueSerializer.serialize(record.topic(), record.headers(), record.value());
        } catch (ClassCastException cce) {
            throw new SerializationException(...);
        }
        //计算消息分区
        int partition = partition(record, serializedKey, serializedValue, cluster);
        tp = new TopicPartition(record.topic(), partition);
        setReadOnly(record.headers());
        Header[] headers = record.headers().toArray();
        //估算消息字节数 
        int serializedSize = AbstractRecords.estimateSizeInBytesUpperBound(apiVersions.maxUsableProduceMagic(),compressionType, serializedKey, serializedValue, headers);
        //消息大小 大于 max.request.size配置项 或 大于 buffer.memory配置 ，抛出异常
        ensureValidRecordSize(serializedSize);
        
        long timestamp = record.timestamp() == null ? nowMs : record.timestamp();
        Callback interceptCallback = new InterceptorCallback<>(callback, this.interceptors, tp);

        if (transactionManager != null && transactionManager.isTransactional()) {
            transactionManager.failIfNotReadyForSend();
        }
        //向RecordAccumulator append数据
        RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey,serializedValue, headers, interceptCallback, remainingWaitMs, true, nowMs);
        if (result.abortForNewBatch) {
            //需新建消息批次时，重新计算消息分区并尝试再次追加
            int prevPartition = partition;
            partitioner.onNewBatch(record.topic(), cluster, prevPartition);
            partition = partition(record, serializedKey, serializedValue, cluster);
            tp = new TopicPartition(record.topic(), partition); 
            interceptCallback = new InterceptorCallback<>(callback, this.interceptors, tp);
            result = accumulator.append(tp, timestamp, serializedKey,serializedValue, headers, interceptCallback, remainingWaitMs, false, nowMs);
        }

        if (transactionManager != null && transactionManager.isTransactional())
            transactionManager.maybeAddPartitionToTransaction(tp);
        //batch已满，唤醒sender线程发送数据
        if (result.batchIsFull || result.newBatchCreated) {
            this.sender.wakeup();
        }
        return result.future;
    } catch (...// 异常处理) {
        this.errors.record();
        this.interceptors.onSendError(record, tp, e);
        ...、、
    } 
}
```

主要可分为以下几步：

* 1、waitOnMetadata方法，确认主题元数据有效，若无则阻塞等待，超时抛出异常；
* 2、使用序列化器完成消息的序列化，key和value；
* 3、根据配置的分区器完成消息分区计算；
* 4、校验消息大小，不超过`max.request.size`和`buffer.memory`配置的大小；
* 5、将消息写入RecordAccumulator中；
* 6、写入的RecordBatch大小已满(如到达batch.size)，唤醒sender线程发送数据。

可以看出，一条消息是经过**生产者拦截器、序列化器、分区器，然后写入RecordAccumulator中，最后唤醒Sender执行发送任务实现的消息发送**。下面先简单介绍下拦截器、序列化器、分区器的实现及功能，
Accumulator及Sender后续单独进行分析。


### 生产者拦截器

使用生产者拦截器可以完成2部分工作，**发送前消息处理以及发送结果回调前处理**，实例为ProducerInterceptor接口的实现类，接口内共有3个方法：
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
org.apache.kafka.common.serialization.Serializer的实现类，反序列化器是org.apache.kafka.common.serialization.Deserializer的实现类，Kafka提供了以下几组实现类
供用户使用：

* StringSerializer、StringDeserializer
* ByteSerializer、ByteDeserializer
* DoubleSerializer、DoubleDeserializer
* ListSerializer、ListDeserializer
* ...

用户也可以实现以上两个接口创建自定义的序列化器和反序列化器。

### 分区器

若ProducerRecord中**partition已有指定值，则直接使用，不再经过分区器**，如果ProducerRecord中partition没有指定值，则需要使用分区器计算消息发往哪个TopicPartition。

```
private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
    Integer partition = record.partition();
    return partition != null ? partition :
            partitioner.partition(record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
}

```

分区器是org.apache.kafka.clients.producer.Partitioner接口的实现类，接口有三个方法：

```
public interface Partitioner extends Configurable, Closeable {
    //为给定的消息计算分区
    int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster);
    void close();
    //重新计算新RecordBatch写入的分区
    default void onNewBatch(String topic, Cluster cluster, int prevPartition) {}
}
```

Kafka提供了DefaultPartitioner、RoundRobinPartitioner、UniformStickyPartitioner三种分区器的实现，默认使用DefaultPartitioner作为生产者端的分区器，其方法实现如下：

```
public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster,
                         int numPartitions) {
    if (keyBytes == null) {
        // key为null 随机计算Utils.toPositive(ThreadLocalRandom.current().nextInt()) 并放入缓存，后续一直使用该分区
        return stickyPartitionCache.partition(topic, cluster);
    }
    //使用key的Hash值与主题分区数取模
    return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
}

//更新stickyPartitionCache 重新计算无key消息分区
public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
    stickyPartitionCache.nextPartition(topic, cluster, prevPartition);
}
```
用户也可自行实现Partitioner接口，创建自定义的分区器。