---
title:  Kafka Producer 消息发送
date:   2021-08-30 21:33:42
categories: Kafka
---

RecordAccumulator主要作用是缓存消息，KafkaProducer#doSend方法中将消息追加入RecordAccumulator后，会调用`sender.wakeup()`将Sender线程唤醒，负责实现消息发送是**Sender线程及NetworkClient网络客户端**。
Sender线程负责从AccumulatorRecord中批量的拉取消息并封装为网络请求的ClientRequest对象，而NetworkClient封装了Java NIO，负责将消息通过网络IO发送至Broker端。


## Sender线程

Sender线程通过**KafkaThread类实现**，在创建Kafka Producer时完成初始化并启动：

```
//KafkaProducer#KafkaProducer 
this.sender = newSender(logContext, kafkaClient, this.metadata);
String ioThreadName = NETWORK_THREAD_PREFIX + " | " + clientId;
this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
this.ioThread.start();
```

KafkaThread创建时会被设置为**守护线程**，KafkaThread继承自Java Thread：

```
public class KafkaThread extends Thread {
    public KafkaThread(final String name, Runnable runnable, boolean daemon) {
        super(runnable, name);
        configureThread(name, daemon);
    }
    
    private void configureThread(final String name, boolean daemon) {
        setDaemon(daemon);
        setUncaughtExceptionHandler((t, e) -> log.error("Uncaught exception in thread '{}':", name, e));
    }
}
```

### Wakeup

Sender#wakeup()方法的实现如下：

```
#Sender#wakeup
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

调用顺序为Sender->NetworkClient->Selector(Kafka封装的)->Selector(Java NIO)，最终调用的是java.nio.channels.Selector的wakeup方法，可知KafkaProducer#doSend方法中，当存在ProducerBatch写满后
调用wake方法，目的是**唤醒可能被阻塞在java nio select方法中的线程**，尽快将消息数据发送出去。


### Run

Sender#run方法中主要是两部分内容：① 通过while循环调用runOnce方法实现消息发送；② 非立即关闭时，Sender线程退出前，会将Accumulator、事务管理器、等待确认的请求队列中的数据处理完成。

```
public void run() {
    while (running) {
        try {
            //消息发送逻辑
            runOnce();
        } catch (Exception e) {
            log.error("Uncaught error in kafka producer I/O thread: ", e);
        }
    }
    //Producer非立即关闭，此时停止接收数据，但会等待Accumulator、事务管理器、等待确认的请求队列中的数据处理完成
    while (!forceClose && ((this.accumulator.hasUndrained() || this.client.inFlightRequestCount() > 0) || hasPendingTransactionalRequests())) {
        try {
            runOnce();
        } catch (Exception e) {
            log.error("Uncaught error in kafka producer I/O thread: ", e);
        }
    }
    
    while (!forceClose && transactionManager != null && transactionManager.hasOngoingTransaction()) {
        //终止事务
        if (!transactionManager.isCompleting()) {
            transactionManager.beginAbort();
        }
        try {
            runOnce();
        } catch (Exception e) {
            log.error("Uncaught error in kafka producer I/O thread: ", e);
        }
    }
    //立即关闭 关闭事务，关闭并清空Accumulator
    if (forceClose) {
        if (transactionManager != null) {
            transactionManager.close();
        }
        this.accumulator.abortIncompleteBatches();
    }
    //关闭NetworkClient
    try {
        this.client.close();
    } catch (Exception e) {
        log.error("Failed to close network client", e);
    }
}

```

#### runOnce

runOnce方法的作用是运行一次发送任务：① 调用sendProducerData方法发送消息数据，② 调用NetworkClient#poll，进行相应的IO操作。

```
void runOnce() {
    if (transactionManager != null) {
        //幂等及事务相关处理下篇内容分析    
    }
    long currentTimeMs = time.milliseconds();
    //发送消息数据
    long pollTimeout = sendProducerData(currentTimeMs);
    //Socket IO操作及响应处理
    client.poll(pollTimeout, currentTimeMs);
}

```

#### sendProducerData

sendProducerData方法源码如下，可简单概括为通过`Accumulator#drain`方法拉取数据，并调用`sendProduceRequests`方法完成消息发送，还有一些如对元数据无效、Broker节点的网络连接不可用、ProducerBatch超时未发送等条件的判断过滤及
保证消息有序的分区加锁的业务。

Accumulator中缓存消息时采用的是`Map<TopicPartition, Deque<ProducerBatch>>`的数据结构，通过主题分区进行分类，而调用drain获取的是`Map<Integer, List<ProducerBatch>>`，key代表的是节点id，
这里进行了数据形式转变，因为对于KafkaProducer的应用逻辑来说，需要关注消息是发向哪个主题分区，但对于网络连接来说，客户端需要关注的是将数据发向哪个Broker，并建立连接，不关心消息属于哪个分区，
通过按照broker分区，一次请求就把所有在这台broker上的分区leader的消息发送完，可以提升消息发送的效率。


```
private long sendProducerData(long now) {
    //获取当前Kafka集群信息，如节点、主题、分区等
    Cluster cluster = metadata.fetch();
    //获取可发送消息数据对应的分区leader副本所在Broker节点集合， 如ProducerBatch isFull或到达配置时间linger.ms 
    RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(cluster, now);
    //有分区节点未知的主题，更新元数据
    if (!result.unknownLeaderTopics.isEmpty()) {
        for (String topic : result.unknownLeaderTopics)
            this.metadata.add(topic, now);
        this.metadata.requestUpdate();
    }
    
    Iterator<Node> iter = result.readyNodes.iterator();
    long notReadyTimeout = Long.MAX_VALUE;
    while (iter.hasNext()) {
        Node node = iter.next();
        //移除网络IO异常的节点
        if (!this.client.ready(node, now)) {
            iter.remove();
            notReadyTimeout = Math.min(notReadyTimeout, this.client.pollDelayMs(node, now));
        }
    }
    //拉取数据
    Map<Integer, List<ProducerBatch>> batches = this.accumulator.drain(cluster, result.readyNodes, this.maxRequestSize, now);
    addToInflightBatches(batches);
    if (guaranteeMessageOrder) {
        //需确保消息有序
        for (List<ProducerBatch> batchList : batches.values()) {
            for (ProducerBatch batch : batchList)
                //对TopicPartition加排他锁，保证一个Tp只有一个RecordBatch在发送, 防止网络IO不稳定，实现有序性
                this.accumulator.mutePartition(batch.topicPartition);
        }
    }
    
    accumulator.resetNextBatchExpiryTime();
    //处理过期ProducerBatch 超时未发送    delivery.timeout.ms > linger.ms +request.timeout.ms
    List<ProducerBatch> expiredInflightBatches = getExpiredInflightBatches(now);
    List<ProducerBatch> expiredBatches = this.accumulator.expiredBatches(now);
    expiredBatches.addAll(expiredInflightBatches);
    if (!expiredBatches.isEmpty())
        log.trace("Expired {} batches in accumulator", expiredBatches.size());
    for (ProducerBatch expiredBatch : expiredBatches) {
        failBatch(expiredBatch, new TimeoutException(errorMessage), false);
        if (transactionManager != null && expiredBatch.inRetry()) {
            transactionManager.markSequenceUnresolved(expiredBatch);
        }
    }
    ...
    //发送数据
    sendProduceRequests(batches, now);
    return pollTimeout;
}
```

#### sendProduceRequests

Accumulator中拉取的消息将通过sendProduceRequests方法实现发送，源码如下：

```
private void sendProduceRequests(Map<Integer, List<ProducerBatch>> collated, long now) {
    for (Map.Entry<Integer, List<ProducerBatch>> entry : collated.entrySet())
        //遍历，按照Broker节点发送
        sendProduceRequest(now, entry.getKey(), acks, requestTimeoutMs, entry.getValue());
}

private void sendProduceRequest(long now, int destination, short acks, int timeout, List<ProducerBatch> batches) {
    if (batches.isEmpty())
        return;

    final Map<TopicPartition, ProducerBatch> recordsByPartition = new HashMap<>(batches.size());
    //数据处理
    byte minUsedMagic = apiVersions.maxUsableProduceMagic();
    for (ProducerBatch batch : batches) {
        if (batch.magic() < minUsedMagic)
            minUsedMagic = batch.magic();
    }
    //ProduceRequest
    ProduceRequestData.TopicProduceDataCollection tpd = new ProduceRequestData.TopicProduceDataCollection();
    for (ProducerBatch batch : batches) {
        TopicPartition tp = batch.topicPartition;
        MemoryRecords records = batch.records();
        if (!records.hasMatchingMagic(minUsedMagic))
            records = batch.records().downConvert(minUsedMagic, 0, time).records();
        ProduceRequestData.TopicProduceData tpData = tpd.find(tp.topic());
        if (tpData == null) {
            tpData = new ProduceRequestData.TopicProduceData().setName(tp.topic());
            tpd.add(tpData);
        }
        tpData.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(tp.partition()).setRecords(records));
        recordsByPartition.put(tp, batch);
    }

    String transactionalId = null;
    if (transactionManager != null && transactionManager.isTransactional()) {
        //事务id 后续分析
        transactionalId = transactionManager.transactionalId();
    }
    ProduceRequest.Builder requestBuilder = ProduceRequest.forMagic(minUsedMagic, new ProduceRequestData().setAcks(acks).setTimeoutMs(timeout).setTransactionalId(transactionalId).setTopicData(tpd));
    //响应处理器
    RequestCompletionHandler callback = response -> handleProduceResponse(response, recordsByPartition, time.milliseconds());
    String nodeId = Integer.toString(destination);
    //请求构建
    ClientRequest clientRequest = client.newClientRequest(nodeId, requestBuilder, now, acks != 0,requestTimeoutMs, callback);
    //发送
    client.send(clientRequest, now);
}
```

sendProduceRequest方法的内容主要是两部分：

* 1、完成消息发送请求ClientRequest的创建，及设置响应处理的Handler；
* 2、调用NetworkClient将消息通过Socket IO发送至Broker端。

至此。Sender部分的内容已结束。下面开始介绍消息发送中网络IO部分的内容。

## NetworkClient

NetworkClient是Kafka对网络IO操作的封装，Producer端与Broker间的所有请求都是通过NetworkClient进行发送,底层采用Java NIO类库实现，具体组件关系如下：

![kafka NIO](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/networkclient.png)


* Selector持有Java NIO中Selector类型的成员变量，以及所有的KafkaChannel。
* NetworkSend 数据发送Buffer；
* NetworkReceive 接收数据Buffer，通过MemoryPool进行池化管理，超过memoryPool时，暂停读取channel；
* TransportLayer  对SocketChannel的封装。

NetworkClient中主要通过两个核心方法完成消息的发送：

* send方法，完成数据准备，注册写事件

```
public void send(ClientRequest request, long now)
```

* poll方法，遍历就绪事件，进行Socket读写及响应处理

```
public List<ClientResponse> poll(long timeout, long now)
```

### Send

#### NetworkClient#send()

NetworkClient#send()方法源码如下：

```
public void send(ClientRequest request, long now) {
    doSend(request, false, now);
}

private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now) {
    //未关闭
    ensureActive();
    String nodeId = clientRequest.destination();
    if (!isInternalRequest) {
        //当前节点连接可用 且 未完成的请求数小于 max.in.flight.requests.per.connection （有序性相关）
        if (!canSendRequest(nodeId, now))
            throw new IllegalStateException("Attempt to send a request to node " + nodeId + " which is not ready.");
    }
    AbstractRequest.Builder<?> builder = clientRequest.requestBuilder();
    try {
        NodeApiVersions versionInfo = apiVersions.get(nodeId);
        short version;
        if (versionInfo == null) {
            version = builder.latestAllowedVersion();
            if (discoverBrokerVersions && log.isTraceEnabled())
                log.trace("No version information found when sending {} with correlation id {} to node {}. " +"Assuming version {}.", clientRequest.apiKey(), clientRequest.correlationId(), nodeId, version);
        } else {
            version = versionInfo.latestUsableVersion(clientRequest.apiKey(), builder.oldestAllowedVersion(),builder.latestAllowedVersion());
        }
        // 构建ProduceRequest
        doSend(clientRequest, isInternalRequest, now, builder.build(version));
    } catch (UnsupportedVersionException unsupportedVersionException) {
        //构建异常响应
        ClientResponse clientResponse = new ClientResponse(clientRequest.makeHeader(builder.latestAllowedVersion()),clientRequest.callback(), clientRequest.destination(), now, now,false, unsupportedVersionException, null, null);
        if (!isInternalRequest)
            //记录消息发送失败 后续处理
            abortedSends.add(clientResponse);
        else if (clientRequest.apiKey() == ApiKeys.METADATA)
            //元数据更新失败 调用Handler
            metadataUpdater.handleFailedRequest(now, Optional.of(unsupportedVersionException));
    }
}

private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now, AbstractRequest request) {
    String destination = clientRequest.destination();
    RequestHeader header = clientRequest.makeHeader(request.version());
    Send send = request.toSend(header);
    InFlightRequest inFlightRequest = new InFlightRequest(clientRequest,header,isInternalRequest,request,send,now);
    //缓存未收到响应的请求 用于后续 失败重试 及 max.in.flight.requests.per.connection判断
    this.inFlightRequests.add(inFlightRequest);
    //调用selector.send发送
    selector.send(new NetworkSend(clientRequest.destination(), send));
}
```

业务流程可概括为：

* 1、确认消息可发送，需满足条件如下：
    * ensureActive()，NetworkClient未关闭；
    * connectionStates.isReady(node, now)，节点连接状态正常；
    * selector.isChannelReady(node)，SocketChannel正常；
    * inFlightRequests.canSendMore()， 未完成的请求数小于 max.in.flight.requests.per.connection。
* 2、检查版本信息，构建ProduceRequest，版本信息检查异常的请求构建失败响应进行处理；
* 3、记录待发送的请求，添加入inFlightRequests中，InFlightRequests中保存着准备发送或等待响应的请求(**leastLoadedNode为InFlightRequests.size最小的节点**)；
* 4、创建NetworkSend，调用`selector.send`发送。

#### Selector#send

Selector#send方法比较简单，获取目标Broker的KafkaChannel，调用KafkaChannel.setSend方法。

```
public void send(NetworkSend send) {
    String connectionId = send.destinationId();
    KafkaChannel channel = openOrClosingChannelOrFail(connectionId);
    if (closingChannels.containsKey(connectionId)) {
        this.failedSends.add(connectionId);
    } else {
        try {
            channel.setSend(send);
        } catch (Exception e) {
            channel.state(ChannelState.FAILED_SEND);
            this.failedSends.add(connectionId);
            close(channel, CloseMode.DISCARD_NO_NOTIFY);
            if (!(e instanceof CancelledKeyException)) {
                throw e;
            }
        }
    }
}
```

#### KafkaChannel#setSend

KafkaChannel#setSend方法主要是将要发送的NetworkSend对象的引用赋值给KafkaChannel中的send，并注册`SelectionKey.OP_WRITE`写事件，等待KafkaChannel可写状态。

```
public void setSend(NetworkSend send) {
    if (this.send != null)
        throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress, connection id is " + id);
    this.send = send;
    //注册写事件
    this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
}
```


### Poll

#### NetworkClient#poll

NetworkClient#poll()方法的主要作用可分为三部分：

* 判断是否需要更新MetaData，若果需要，发送MetadataRequest进行更新；
* 调用Selector#poll进行IO操作；
* 处理IO操作完成后的业务。

```
public List<ClientResponse> poll(long timeout, long now) {
    ensureActive();
    if (!abortedSends.isEmpty()) {
        //处理发送失败的请求
        List<ClientResponse> responses = new ArrayList<>();
        handleAbortedSends(responses);
        completeResponses(responses);
        return responses;
    }
    //判断是否需要更新MetaData，若果需要，发送MetadataRequest进行更新
    long metadataTimeout = metadataUpdater.maybeUpdate(now);
    try {
        //处理IO事件，如连接建立，读&写
        this.selector.poll(Utils.min(timeout, metadataTimeout, defaultRequestTimeoutMs));
    } catch (IOException e) {
        log.error("Unexpected error during I/O", e);
    }
    
    //IO操作结束后的业务处理
    long updatedNow = this.time.milliseconds();
    List<ClientResponse> responses = new ArrayList<>();
    //处理已完成发送的NetworkSend，构建响应
    handleCompletedSends(responses, updatedNow);
    //处理从Broker端接收到NetWorkReceive，构建响应
    handleCompletedReceives(responses, updatedNow);
    //处理失效连接，构建响应
    handleDisconnections(responses, updatedNow);
    //处理新建立的连接，构建响应
    handleConnections();
    handleInitiateApiVersionRequests(updatedNow);
    handleTimedOutConnections(responses, updatedNow);
    //处理超时请求，构建响应
    handleTimedOutRequests(responses, updatedNow);
    
    //对所有响应进行处理
    completeResponses(responses);
    return responses;
}

```

#### Selector#poll

Selector#poll会遍历所有的IO就绪事件，并进行处理，

```
public void poll(long timeout) throws IOException {
    if (timeout < 0) throw new IllegalArgumentException("timeout should be >= 0");
    boolean madeReadProgressLastCall = madeReadProgressLastPoll;
    //清除上次poll的缓存
    clear();
    
    boolean dataInBuffers = !keysWithBufferedRead.isEmpty();
    //连接事件不为空或Channel有数据在缓冲区中但却无法读取(比如因为内存不足),timeout为0 ，select立即返回
    if (!immediatelyConnectedKeys.isEmpty() || (madeReadProgressLastCall && dataInBuffers))
        timeout = 0;
    
    //若之前内存池内存耗尽, 而现在又可用了, 将一些因为内存压力而暂时取消读事件的Channel解锁，重新注册读事件
    if (!memoryPool.isOutOfMemory() && outOfMemory) {
        for (KafkaChannel channel : channels.values()) {
            if (channel.isInMutableState() && !explicitlyMutedChannels.contains(channel)) {
                channel.maybeUnmute();
            }
        }
        outOfMemory = false;
    }

    long startSelect = time.nanoseconds();
    //获取就绪IO事件数
    int numReadyKeys = select(timeout);
    long endSelect = time.nanoseconds();
    this.sensors.selectTime.record(endSelect - startSelect, time.milliseconds());

    if (numReadyKeys > 0 || !immediatelyConnectedKeys.isEmpty() || dataInBuffers) {
        //java nio获取就绪事件
        Set<SelectionKey> readyKeys = this.nioSelector.selectedKeys();

        if (dataInBuffers) {
            //处理未读取完的channel（如因内存不足），最多对一个key处理2次，这里清空
            keysWithBufferedRead.removeAll(readyKeys); //so no channel gets polled twice
            Set<SelectionKey> toPoll = keysWithBufferedRead;
            keysWithBufferedRead = new HashSet<>(); //poll() calls will repopulate if needed
            //处理有数据缓存的channel
            pollSelectionKeys(toPoll, false, endSelect);
        }

        // 处理底层有数据的channel
        pollSelectionKeys(readyKeys, false, endSelect);
        readyKeys.clear();
        //处理待连接的channel
        pollSelectionKeys(immediatelyConnectedKeys, true, endSelect);
        immediatelyConnectedKeys.clear();
    } else {
        madeReadProgressLastPoll = true; //no work is also "progress"
    }

    long endIo = time.nanoseconds();
    this.sensors.ioTime.record(endIo - endSelect, time.milliseconds());
    completeDelayedChannelClose(endIo);
    maybeCloseOldestConnection(endSelect);
}

```
pollSelectionKey方法内对相应的SelectionKey事件进行处理。

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
            if (channel.ready() && channel.state() == ChannelState.NOT_CONNECTED)
                 channel.state(ChannelState.READY);
            Optional<NetworkReceive> responseReceivedDuringReauthentication = channel.pollResponseReceivedDuringReauthentication();
            responseReceivedDuringReauthentication.ifPresent(receive -> {
                long currentTimeMs = time.milliseconds();
                addToCompletedReceives(channel, receive, currentTimeMs);
            });
            if (channel.ready() && (key.isReadable() || channel.hasBytesBuffered()) && !hasCompletedReceive(channel)
                    && !explicitlyMutedChannels.contains(channel)) {
                //读取数据
                attemptRead(channel);
            }
            if (channel.hasBytesBuffered() && !explicitlyMutedChannels.contains(channel)) {
                //记录正在处理读取的事件
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

            if (!key.isValid())
                close(channel, CloseMode.GRACEFUL);
        } catch (Exception e) {
            ...
        }
        ...
    }
}
```

attemptWrite方法中调用KafkaChannel#write方法，将之前设置的NetworkSend发送出去。至此，Producer端的正常消息发送流程已全部分析完毕。

```
//写事件就绪时写入Socket
public long write() throws IOException {
    if (send == null)
        return 0;
    midWrite = true;
    //将消息写入Socket
    return send.writeTo(transportLayer);
}

```

### handleProduceResponse

poll方法中对所有事件的处理都会封装为`org.apache.kafka.clients.ClientResponse`对象，由`Sender#sendProduceRequest()`方法中构建请求时注册的handler进行处理，源码如下：

```
private void handleProduceResponse(ClientResponse response, Map<TopicPartition, ProducerBatch> batches, long now) {
    RequestHeader requestHeader = response.requestHeader();
    int correlationId = requestHeader.correlationId();
    if (response.wasDisconnected()) {
        //消息未发送成功，异常处理
        for (ProducerBatch batch : batches.values())
            completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NETWORK_EXCEPTION, String.format("Disconnected from node %s", response.destination())),correlationId, now);
    } else if (response.versionMismatch() != null) {
        for (ProducerBatch batch : batches.values())
            completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.UNSUPPORTED_VERSION), correlationId, now);
    } else {
        //broker端有相应
        if (response.hasResponse()) {
            ProduceResponse produceResponse = (ProduceResponse) response.responseBody();
            produceResponse.data().responses().forEach(r -> r.partitionResponses().forEach(p -> {
                TopicPartition tp = new TopicPartition(r.name(), p.index());
                ProduceResponse.PartitionResponse partResp = new ProduceResponse.PartitionResponse(
                        Errors.forCode(p.errorCode()),
                        p.baseOffset(),
                        p.logAppendTimeMs(),
                        p.logStartOffset(),
                        p.recordErrors()
                            .stream()
                            .map(e -> new ProduceResponse.RecordError(e.batchIndex(), e.batchIndexErrorMessage()))
                            .collect(Collectors.toList()),
                        p.errorMessage());
                ProducerBatch batch = batches.get(tp);
                completeBatch(batch, partResp, correlationId, now);
            }));
            this.sensors.recordLatency(response.destination(), response.requestLatencyMs());
        } else {
            //ack = 0时，没有相应，所有请求直接视作无异常完成 
            for (ProducerBatch batch : batches.values()) {
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NONE), correlationId, now);
            }
        }
    }
}
```

handleProduceResponse

```
private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response, long correlationId,long now) {
    Errors error = response.error;
    if (error == Errors.MESSAGE_TOO_LARGE && batch.recordCount > 1 && !batch.isDone() && (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 || batch.isCompressed())) {
        
        if (transactionManager != null)
            transactionManager.removeInFlightBatch(batch);
        this.accumulator.splitAndReenqueue(batch);
        maybeRemoveAndDeallocateBatch(batch);
        this.sensors.recordBatchSplit();
    } else if (error != Errors.NONE) {
        if (canRetry(batch, response, now)) {          
            reenqueueBatch(batch, now);
        } else if (error == Errors.DUPLICATE_SEQUENCE_NUMBER) {        
            completeBatch(batch, response);
        } else {
            failBatch(batch, response, batch.attempts() < this.retries);
        }
        if (error.exception() instanceof InvalidMetadataException) {
            metadata.requestUpdate();
        }
    } else {
        completeBatch(batch, response);
    }
    if (guaranteeMessageOrder)
        this.accumulator.unmutePartition(batch.topicPartition);
}


```


#### 异常重试 

KafkaProducer端

#### 发送异常

消息发送异常可分为两类：**可重试异常、不可重试异常**。当发生不可重试异常时，send方法会直接抛出异常；可重试异常时，如果**retries参数不为0，kafkaProducer在规定的重试次数内会自动重试**，
不会抛出异常，超出次数还未成功时，则会抛出异常，由外层逻辑处理。

可重试异常：TimeoutException、InvalidMetadataException、UnknownTopicOrPartitionException

不可重试异常：InvalidTopicException、RecordTooLargeException、UnknownServerException