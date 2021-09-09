---
layout: post
title:  "Kafka Producer"
date:   2021-09-05 17:14:02
categories: Kafka
---

线程安全 ： accumulator synchronized实现


发送方法返回Future<RecordMetadata>对象，可设置Callback对象，

发送方式

发后即忘

同步  future.get() 阻塞

异步  Callback



KafkaProducer.dosend()方法；

主线程及sender线程，主线程写入recordAccumulator ,sender线程负责发送，通过消息累加器RecordAccumulator解耦


主线程：

1 构建ProducerRecord

2 ProducerInterceptors.onsend:  生产者发送拦截器
 ```
ProducerRecord<K, V> onSend(ProducerRecord<K, V> record);
void onAcknowledgement(RecordMetadata metadata, Exception exception);
void close();
```
 
 三个方法  onSend  onAcknowledgement
    序列化及分配分区前，调用onSend()
    发送成功被应答前或失败时，调用onAcknowledgement()  该方法由IO线程执行，应尽量简单   早于callback执行
    
3 org.apache.kafka.common.serialization.Serializer  序列化       


4 org.apache.kafka.clients.producer.Partitioner接口   确定主题分区  

若已指定分区，则使用指定分区

DefaultPartitioner   key ！= null   ?  hash(key) % partition  :  轮询


5 写入RecordAccumulator  缓存消息，批量发送，减少网络传输的资源消耗，默认大小32MB，服务生产消息过快，缓存占满后，会导致send阻塞，阻塞超时后抛出异常。
 
 
 append方法
org.apache.kafka.clients.producer.internals.RecordAccumulator#append

获取指定主题分区的队列  TopicPartition

每个主题分区维护一个双端队列 

private final ConcurrentMap<TopicPartition, Deque<org.apache.kafka.clients.producer.internals.ProducerBatch>> batches;

```
Deque<org.apache.kafka.clients.producer.internals.ProducerBatch> dq = getOrCreateDeque(tp);
synchronized (dq) {   //加锁 分区对应的双端队列  保证线程安全
    if (closed)
        throw new KafkaException("Producer closed while send in progress");
    RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq, nowMs);
    if (appendResult != null)
        return appendResult;
}

```

this.batches = new CopyOnWriteMap<>();  kafka自定义数据结构  CopyOnWriteMap 读写分离


这个数据结构需要支持的是读多写少的场景。读多是因为每条消息过来都会根据 key 读取 value 的信息，假如有 1000 万条消息，那么就会读取 batches 对象 1000 万次。
写少是因为，比如我们生产者发送数据需要往一个主题里面去发送数据，假设这个主题有 50 个分区，那么这个 batches 里面就需要写 50 个 key-value 数据就可以了



在新建 ProducerBatch 评估这条消息的大小是否超过 batch size 参数
小，如果不超过，那么就以 batch size 参数的大 来创建 ProducerBatch ，这样在使用完这
段内存区域之后，可以通过 BufferPool 的管理来进行 用；如果超过，那么就以评估的大小来
ProducerBatch 这段内存区域不会被复用。




sender线程 1、 唤醒发送  2、更新元数据

NetworkClient   java NIO

将按照分区分组的消息改为按照节点分组，提高发送效率

/* the selector used to perform network i/o */
    private final Selectable selector;


 /* the set of requests currently being sent or awaiting a response */
    private final InFlightRequests inFlightRequests = Map<NodeId, Deque<NetworkClient.InFlightRequest>>;

是缓存 了已经发出去但还
没有收到响应的请求


。这个配置参数为 max. 工 n flight.requests
per. connection ，默认值为5 ，即每个连接最多只能缓存5 个未响应的请求，超过该数值
之后就不能再向这个连接发送更多的请求了，除非有缓存的请求收到了响应（ Response ）



事务实现  多条消息写入同时成功或同时失败

消息传输保证

不启用事务

at least once 至少一次  生产者写入消息，只要成功写入，多副本机制一定可以确保消息不会丢失，因重试机制，可能回造成消息重复写入，
at most once 消息消费时，先提交offset,则最多一次，后提交offset 则至少一次


等幂和事务

exactly once



等幂   
PID  producer id，producer初始化时分配

sequence Number   序列号  每个生产者对每个TopicPartition都有一个对应的序列化，每条消息递增

broker 为每个<PID,partition> 维护一个seq   保证单个生产者会话中单分区的幂等。若大于seq+1,有消息丢失，若小于 ，则属于重复写入



事务：


2PC 两阶段提交

保证多分区写入的原子性


transactionId  配置事务id 

producer初始化时获取两个参数
PID
producer epoch  确保只有一个producer参与该事务id下的事务

transactionCoordinator 事务协调器

_transaction_state  内部主题，默认分区50个，根据transactionId



生产者事务消息流程

1 找到transactionCoordinator，根据transactionId找到对应的transaction_state主题分区leader副本所在节点
2 保存当前pid及transactionId的对应关系持久化到_transaction_state主题分区文件中，保证及时宕机也不会丢失
3 调用beginTransaction()开启事务，只是本地标记。 transactionCoordinator只有在收到第一条消息后才会任务事务开启。

4 业务网流

4.1  addPartitionToTxnRequest  当需要对一个分区写入消息时，需要将 transactionId-partition的对应关系持久到_transaction_state，若是第一个分区，开启事务计时
以便后续对分区进行设置commit或abort

4.2 发送消息

4.3sendOffsetsToTransaction(),将事务消息的offset信息保存到  _transaction_state

5 提交或终止

producer调用commitTransaction 或  abortTransaction方法，transactionCoordinator开始2pc
在第一阶段，Coordinator将其内部状态更新为“prepare_commit”并在事务日志中更新此状态。一旦完成了这个事务，无论发生什么事，都能保证事务完成。
Coordinator然后开始阶段2，在那里它将事务提交标记写入作为事务一部分的Topic分区。
这些事务标记不会暴露给应用程序，但是在read_committed模式下被Consumer使用来过滤掉被中止事务的消息，并且不返回属于开放事务的消息（即那些在日志中但没有事务标记与他们相关联）。
一旦标记被写入，事务协调器将事务标记为“完成”，并且Producer可以开始下一个事务。






消费端

read_uncommitted

read_committed