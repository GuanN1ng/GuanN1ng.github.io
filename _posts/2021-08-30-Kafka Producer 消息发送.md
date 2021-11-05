---
title:  Kafka Producer 消息发送
date:   2021-08-30 21:33:42
categories: Kafka
---

RecordAccumulator主要用来缓存消息，真正实现消息发送是**Sender线程及NetworkClient网络客户端**。Sender线程负责从AccumulatorRecord中批量的拉取消息并封装为网络请求的ClientRequest对象，
而NetworkClient封装了Java NIO，负责将消息通过网络IO发送至Broker端。


### Sender初始化

Sender在创建Kafka Producer时完成初始化并启动：

```
//KafkaProducer#KafkaProducer 
this.sender = newSender(logContext, kafkaClient, this.metadata);
String ioThreadName = NETWORK_THREAD_PREFIX + " | " + clientId;
this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
this.ioThread.start();
```

Sender线程为**KafkaThread类的实现，会被设置为守护线程**，KafkaThread继承自Java Thread：

```
public KafkaThread(final String name, Runnable runnable, boolean daemon) {
    super(runnable, name);
    configureThread(name, daemon);
}

private void configureThread(final String name, boolean daemon) {
    setDaemon(daemon);
    setUncaughtExceptionHandler((t, e) -> log.error("Uncaught exception in thread '{}':", name, e));
}
```

### Wakeup

执行KafkaProducer#send()方法时，也会完成对Sender#wakeup()方法的代用，代码如下，最终会调用java.nio.channels.Selector的wakeup方法，**唤醒被阻塞在select方法中的
线程**，尽快将请求发送出去。

```
#org.apache.kafka.clients.producer.internals.Sender#wakeup
public void wakeup() {
    this.client.wakeup();
}

#org.apache.kafka.clients.NetworkClient#wakeup
public void wakeup() {
    this.selector.wakeup();
}

#org.apache.kafka.common.network.Selector#wakeup
public void wakeup() {
    this.nioSelector.wakeup();
}

```

### Run方法

run方法中通过调用while循环调用runOnce方法实现消息发送(runOnce方法中还包含了事务相关处理，后续更新)：

```
void runOnce() {
        //事务处理
        if (transactionManager != null) {...}

        long currentTimeMs = time.milliseconds();
        long pollTimeout = sendProducerData(currentTimeMs);
        client.poll(pollTimeout, currentTimeMs);
    }
```

#### sendProducerData

对于KafkaProducer的应用逻辑来说，需要关注消息是发向哪个主题分区，但对于网络连接来说，客户端需要关注的是与哪个Broker建立连接，不关心消息属于哪个分区，Sender线程
需要将<TopicPartition,Deque<ProducerBatch>>的数据结构转变为<NodeId,List<ProducerBatch>>的形式，NodeId即为Kafka Broker的id,最终封装成为ClientRequest对象。

* 1、通过RecordAccumulator#ready方法获取已就绪的分区节点。遍历`ConcurrentMap<TopicPartition, Deque<ProducerBatch>> batches`获取大小达到batch.size大小或时间达到linger.ms的分区及其
对应的分区leader节点(会跳过加锁的分区，保证有序)。

```
Cluster cluster = metadata.fetch(); 
ReadyCheckResult result = accumulator.ready(cluster, now); //获取待发送节点数据
if (!result.unknownLeaderTopics.isEmpty()) {
    //有未知leader分区的Topic,再次请求获取元数据
    for (String topic : result.unknownLeaderTopics)
        this.metadata.add(topic, now);
    log.debug("Requesting metadata update due to unknown leader topics from the batched records: {}",
        result.unknownLeaderTopics);
    this.metadata.requestUpdate();
}
```

* 2、判断与Broker节点的IO连接是否可用，不可用，移除节点。

```
Iterator<Node> iter = result.readyNodes.iterator();
long notReadyTimeout = Long.MAX_VALUE;
while (iter.hasNext()) {
    Node node = iter.next();
    if (!this.client.ready(node, now)) {  //判断连接是否可用
        iter.remove();
        notReadyTimeout = Math.min(notReadyTimeout, this.client.pollDelayMs(node, now));
    }
}
```

* 3、从RecordAccumulator中拉取消息，按照BrokerId分类，并判断是否保证发送顺序，**max.in.flight.requests.per.connection参数为1，表示需要保证分区发送顺序，则调用accumulator.mutePartition(),对当前分区加锁**。

```
Map<Integer, List<ProducerBatch>> batches = accumulator.drain(cluster, result.readyNodes, this.maxRequestSize, now);
addToInflightBatches(batches);
if (guaranteeMessageOrder) {
    // 确保分区消息有序   maxInflightRequests == 1
    for (List<org.apache.kafka.clients.producer.internals.ProducerBatch> batchList : batches.values()) {
        for (org.apache.kafka.clients.producer.internals.ProducerBatch batch : batchList)
            this.accumulator.mutePartition(batch.topicPartition); //对当前分区加锁
    }
}
```

* 4、处理超时消息，移除超时未发送消息，delivery.timeout.ms参数，默认时间是120s

```
accumulator.resetNextBatchExpiryTime();
List<ProducerBatch> expiredInflightBatches = getExpiredInflightBatches(now);
List<ProducerBatch> expiredBatches = this.accumulator.expiredBatches(now);
expiredBatches.addAll(expiredInflightBatches);
if (!expiredBatches.isEmpty())
    log.trace("Expired {} batches in accumulator", expiredBatches.size());
for (org.apache.kafka.clients.producer.internals.ProducerBatch expiredBatch : expiredBatches) {
    String errorMessage = "Expiring " + expiredBatch.recordCount + " record(s) for " + expiredBatch.topicPartition
        + ":" + (now - expiredBatch.createdMs) + " ms has passed since batch creation";
    failBatch(expiredBatch, new TimeoutException(errorMessage), false);
    if (transactionManager != null && expiredBatch.inRetry()) {
        transactionManager.markSequenceUnresolved(expiredBatch);
    }
}
sensors.updateProduceRequestMetrics(batches);
```

* 5、注册结果回调处理函数，并完成ClientRequest的构造，调用NetworkClient#send()方法发送消息。

```
RequestCompletionHandler callback = response -> handleProduceResponse(response, recordsByPartition, time.milliseconds());
String nodeId = Integer.toString(destination);
ClientRequest clientRequest = client.newClientRequest(nodeId, requestBuilder, now, acks != 0,
        requestTimeoutMs, callback);
client.send(clientRequest, now);
```

### NetworkClient

NetworkClient对Kafka对网络层的封装实现，底层采用Java NIO类库实现，添加了一些额外的功能，具体关系如下：

![kafka NIO](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/networkclient.png)


* NetworkSend 数据发送Buffer
* NetworkReceive 接收数据Buffer，通过MemoryPool进行池化管理，超过memoryPool时，暂停读取channel。
* TransportLayer  对SocketChannel的封装
* Kafka Selector持有Java NIO中Selector类型的成员变量，以及所有的KafkaChannel

```
private final java.nio.channels.Selector nioSelector;
private final Map<String, KafkaChannel> channels;
```

NetworkClient的消息发送通过两个核心方法完成：

```
public void send(ClientRequest request, long now)
public List<ClientResponse> poll(long timeout, long now)
```

#### NetworkClient#send

send方法完成NetworkSend对象的构建并调用Selector.send()发送该对象。

```
inFlightRequests.add(inFlightRequest);
selector.send(new NetworkSend(clientRequest.destination(), send));
```

* 将请求保存到InFlightRequests中，InFlightRequests中保存着准备发送或等待响应的请求(**leastLoadedNode为InFlightRequests.size最小的节点**)。
* 调用Selector#send，将数据写入对应KafkaChannel的NetworkSend(Buffer)中，并注册TransportLayer(SocketChannel)对应的写事件。

#### NetworkClient#poll

poll中，调用Selector#poll方法，并完成Selector中所有Channel的事件。

```
//更新元数据信息
long metadataTimeout = metadataUpdater.maybeUpdate(now);
//调用 Selector.poll()进行socket相关的IO操作
try {
    this.selector.poll(Utils.min(timeout, metadataTimeout, defaultRequestTimeoutMs));
} catch (IOException e) {
    log.error("Unexpected error during I/O", e);
}

// 处理完成后的操作
long updatedNow = this.time.milliseconds();
List<ClientResponse> responses = new ArrayList<>();
handleCompletedSends(responses, updatedNow);
handleCompletedReceives(responses, updatedNow);
handleDisconnections(responses, updatedNow);
handleConnections();
handleInitiateApiVersionRequests(updatedNow);
handleTimedOutConnections(responses, updatedNow);
handleTimedOutRequests(responses, updatedNow);
completeResponses(responses);
```

completeResponses方法内调用Sender线程设置的回调函数RequestCompletionHandler->Sender#completeBatch()，完成消息重试、ProducerBatch清理及事务处理。

```
if (error == Errors.MESSAGE_TOO_LARGE && batch.recordCount > 1 && !batch.isDone() &&
        (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 || batch.isCompressed())) {
    //消息太大，分割再次尝试发送，不占用重试次数
    if (transactionManager != null)
        transactionManager.removeInFlightBatch(batch);
    this.accumulator.splitAndReenqueue(batch);
    maybeRemoveAndDeallocateBatch(batch);
    this.sensors.recordBatchSplit();
} else if (error != Errors.NONE) {
    //可重试的异常
    if (canRetry(batch, response, now)) {
        reenqueueBatch(batch, now); //重试
    } else if (error == Errors.DUPLICATE_SEQUENCE_NUMBER) {
        //重试机制导致发送出去重复的消息  SEQUENCE_NUMBER 幂等时的序列号
        completeBatch(batch, response);
    } else {
        //
        failBatch(batch, response, batch.attempts() < this.retries);
    }
    if (error.exception() instanceof InvalidMetadataException) {
        //更新元数据
        metadata.requestUpdate();
    }
} else {
    completeBatch(batch, response);
}

// Unmute the completed partition.
if (guaranteeMessageOrder)
    this.accumulator.unmutePartition(batch.topicPartition);

```


#### Selector#poll

poll方法封装了JAVA NIO的业务操作。

```
public void poll(long timeout) throws IOException {
    if (timeout < 0)
        throw new IllegalArgumentException("timeout should be >= 0");

    boolean madeReadProgressLastCall = madeReadProgressLastPoll;
    //清除上次poll的缓存
    clear();

    boolean dataInBuffers = !keysWithBufferedRead.isEmpty();
    //连接事件不为空或Channel有数据在缓冲区中但却无法读取(比如因为内存不足),timeout为0 ，select立即返回
    if (!immediatelyConnectedKeys.isEmpty() || (madeReadProgressLastCall && dataInBuffers))
        timeout = 0;
    
    //若之前内存池内存耗尽, 而现在又可用了, 将一些因为内存压力而暂时取消读事件的 Channel 重新注册读事件
    if (!memoryPool.isOutOfMemory() && outOfMemory) {
        for (KafkaChannel channel : channels.values()) {
            if (channel.isInMutableState() && !explicitlyMutedChannels.contains(channel)) {
                channel.maybeUnmute();
            }
        }
        outOfMemory = false;
    }

   
    long startSelect = time.nanoseconds();
    int numReadyKeys = select(timeout);
    long endSelect = time.nanoseconds();
    this.sensors.selectTime.record(endSelect - startSelect, time.milliseconds());

    if (numReadyKeys > 0 || !immediatelyConnectedKeys.isEmpty() || dataInBuffers) {
        //java nio获取就绪事件
        Set<SelectionKey> readyKeys = this.nioSelector.selectedKeys();

        if (dataInBuffers) {
            keysWithBufferedRead.removeAll(readyKeys); //so no channel gets polled twice
            Set<SelectionKey> toPoll = keysWithBufferedRead;
            keysWithBufferedRead = new HashSet<>(); //poll() calls will repopulate if needed
            //处理有数据缓存的channel
            pollSelectionKeys(toPoll, false, endSelect);
        }

        // 处理底层有数据的channel
        pollSelectionKeys(readyKeys, false, endSelect);
        // Clear all selected keys so that they are included in the ready count for the next select
        readyKeys.clear();
        //处理待连接的channel
        pollSelectionKeys(immediatelyConnectedKeys, true, endSelect);
        immediatelyConnectedKeys.clear();
    } else {
        madeReadProgressLastPoll = true; //no work is also "progress"
    }

    long endIo = time.nanoseconds();
    this.sensors.ioTime.record(endIo - endSelect, time.milliseconds());

    // Close channels that were delayed and are now ready to be closed
    completeDelayedChannelClose(endIo);

    // 在关闭过期连接后, 将完成接收的 Channels 加入 completedReceives.
    maybeCloseOldestConnection(endSelect);
}

```
pollSelectionKey方法内对相应的SelectionKey事件进行处理。同JAVA NIO。

```
void pollSelectionKeys(Set<SelectionKey> selectionKeys, boolean isImmediatelyConnected, long currentTimeNanos) {
    for (SelectionKey key : determineHandlingOrder(selectionKeys)) {
        KafkaChannel channel = channel(key);
        long channelStartTimeNanos = recordTimePerConnection ? time.nanoseconds() : 0;
        boolean sendFailed = false;
        String nodeId = channel.id();

        if (idleExpiryManager != null)
            idleExpiryManager.update(nodeId, currentTimeNanos);

        try {
           //处理已经完成握手的连接
            if (isImmediatelyConnected || key.isConnectable()) {
                if (channel.finishConnect()) {
                    this.connected.add(nodeId);
                    this.sensors.connectionCreated.record();
                    SocketChannel socketChannel = (SocketChannel) key.channel();  
                } else {
                    continue;
                }
            }
            //....省略部分代码
            
            //读事件处理
            if (channel.ready() && (key.isReadable() || channel.hasBytesBuffered()) && !hasCompletedReceive(channel)
                    && !explicitlyMutedChannels.contains(channel)) {
                attemptRead(channel);
            }

            if (channel.hasBytesBuffered() && !explicitlyMutedChannels.contains(channel)) {
                keysWithBufferedRead.add(key);
            } 
            ...            
            //写事件处理
            long nowNanos = channelStartTimeNanos != 0 ? channelStartTimeNanos : currentTimeNanos;
            try {
                attemptWrite(key, channel, nowNanos);
            } catch (Exception e) {
                sendFailed = true;
                throw e;
            }

            /* cancel any defunct sockets */
            if (!key.isValid())
                close(channel, CloseMode.GRACEFUL);

        } 
        ...
        } finally {
            maybeRecordTimePerConnection(channel, channelStartTimeNanos);
        }
    }
}

```





#### 发送异常

消息发送异常可分为两类：**可重试异常、不可重试异常**。当发生不可重试异常时，send方法会直接抛出异常；可重试异常时，如果**retries参数不为0，kafkaProducer在规定的重试次数内会自动重试**，
不会抛出异常，超出次数还未成功时，则会抛出异常，由外层逻辑处理。

可重试异常：TimeoutException、InvalidMetadataException、UnknownTopicOrPartitionException

不可重试异常：InvalidTopicException、RecordTooLargeException、UnknownServerException