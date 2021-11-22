---
title:  Kafka Producer幂等与事务
date:   2021-08-31 21:50:52
categories: Kafka
---

一般而言，消息中间件的消息传输保障有3个层级：

* at most once：至多一次，消息可能会丢失，但绝对不会重复传输。
* at least once：至少一次，消息绝不会丢失，但可能重复传输。
* exactly once：正好一次，消息不会丢失，也不会被重复发送。

Kafka多副本机制确保消息一旦提交成功写入日志文件，这条消息就不会丢失(acks=-1)。当生产者无法判断是否提交成功时，如遇到网络问题而导致请求超时，生产者可以通过多次重试来确保消息
成功提交，但重试过程可能造成消息的重复写入，此时KafkaProducer提供的消息传输保障为at least once。同时，Kafka允许Producer灵活的指定级别，如消息采用发后即忘的方式发送且acks=0，或retries=0，
则KafkaProducer的传输保障为at most one。

Kafka为实现exactly once，引入了幂等和事务特性。

## 幂等

Kafka引入**幂等特性来解决异常重试机制导致的消息重复问题**，开启幂等后，当发送同一条消息时，数据在Broker端只会被持久化一次，避免生产者重试导致的消息重复写入。通过将参数**enable.idempotence设置为true即可开启幂等**功能，
此时retries参数默认为Integer.MAX_VALUE，acks默认为all，并确保max.in.flight.requests.per.connection(指定了Producer在收到Broker晌应之前单连接可以发送多少个Batch)不能大于5。

### 实现原理

幂等要求Broker端能够鉴别消息的唯一性，鉴于主题多分区及多Producer的情况，Kafka引入了两个机制实现幂等：

* PID(Producer ID)，用来标识生产者，全局唯一。初始化时被分配，对用户透明，重启后会重新分配到新的PID。
* sequence number，标识消息，每一个PID，每个TopicPartition都有对应的sequence number，从0单调递增。

可以看出，此处的幂等是有条件的：

* 因为Producer每次重启都会申请到新的PID，所以只能保证Producer在单个会话内的幂等；
* 只能保证单个TopicPartition内的幂等。

### 幂等消息发送流程

下面通过幂等消息的发送来了解Kafka如何实现消息幂等。

#### 获取PID InitProducerIdRequest

幂等相关的逻辑处理是从Sender#runOnce方法开始的，当Producer开启了幂等或事务后，在进行普通消息发送流程前，必须等待幂等及事务相关的处理的完成。这里首先关注PID的获取，
通过源码注释可知PID的获取是通过`TransactionManager#bumpIdempotentEpochAndResetIdIfNeeded()`和`Sender#maybeSendAndPollTransactionalRequest`方法实现。

```
void runOnce() {
    //Producer开启幂等或事务后，在初始化时会新建一个TransactionManager，负责维护Producer端的幂等或事务信息。
    if (transactionManager != null) {
        //幂等及事务处理
        try {
            ...//省略部分事务代码
             // Check whether we need a new producerId. If so, we will enqueue an InitProducerId
             // request which will be sent below
            transactionManager.bumpIdempotentEpochAndResetIdIfNeeded();
            if (maybeSendAndPollTransactionalRequest()) {
                //返回，等待下次任务
                return;
            }
        } catch (AuthenticationException e) {
            transactionManager.authenticationFailed(e);
        }
    }
    //普通消息发送流程
    long currentTimeMs = time.milliseconds();
    long pollTimeout = sendProducerData(currentTimeMs);
    client.poll(pollTimeout, currentTimeMs);
}
```

`bumpIdempotentEpochAndResetIdIfNeeded()`方法主要是完成请求及响应处理Handler的构建。

```
synchronized void bumpIdempotentEpochAndResetIdIfNeeded() {
    //只开启幂等，未开启事务
    if (!isTransactional()) {
        //初始化为false
        if (epochBumpRequired) { bumpIdempotentProducerEpoch(); }
        //状态不为初始化且没有PID，新建对象的状态为UNINITIALIZED
        if (currentState != State.INITIALIZING && !hasProducerId()) {
            //状态转换
            transitionTo(State.INITIALIZING);
            //构建请求数据及Handler
            InitProducerIdRequestData requestData = new InitProducerIdRequestData()
                    //Transaction为null,只开启了事务
                    .setTransactionalId(null)
                    .setTransactionTimeoutMs(Integer.MAX_VALUE);
            InitProducerIdHandler handler = new InitProducerIdHandler(new InitProducerIdRequest.Builder(requestData), false);
            //加入队列，等待发送
            enqueueRequest(handler);
        }
    }
}
```

maybeSendAndPollTransactionalRequest将会完成InitProducerIdRequest的发送，源码如下：

```
private boolean maybeSendAndPollTransactionalRequest() {
    //Producer刚初始化。这里先忽略这里的分支流程
    if (transactionManager.hasInFlightRequest()) { ... }
    if (transactionManager.hasAbortableError() || transactionManager.isAborting()) { ... }
    
    //获取上一步构建的Handler
    TransactionManager.TxnRequestHandler nextRequestHandler = transactionManager.nextRequest(accumulator.hasIncomplete());
    if (nextRequestHandler == null)
        return false;
    //构建request
    AbstractRequest.Builder<?> requestBuilder = nextRequestHandler.requestBuilder();
    Node targetNode = null;
    try {
        FindCoordinatorRequest.CoordinatorType coordinatorType = nextRequestHandler.coordinatorType();
       
        targetNode = coordinatorType != null ?
                //若开启事务后，则需要找到对应的TransactionCoordinator
                transactionManager.coordinator(coordinatorType) : 
                //只开启幂等时，coordinatorType == null，此时只需要找到leastLoadedNode即可
                client.leastLoadedNode(time.milliseconds());
        if (targetNode != null) {
            //NetworkClient 连接是否可用
            if (!awaitNodeReady(targetNode, coordinatorType)) {
                log.trace("Target node {} not ready within request timeout, will retry when node is ready.", targetNode);
                maybeFindCoordinatorAndRetry(nextRequestHandler);
                return true;
            }
        //省略
        } else if (coordinatorType != null) {...} else {...}
        //是否是重试，进行retryBackoffMs的休眠，避免无效的频繁请求
        if (nextRequestHandler.isRetry())
            time.sleep(nextRequestHandler.retryBackoffMs());
        //请求发送
        long currentTimeMs = time.milliseconds();
        ClientRequest clientRequest = client.newClientRequest(targetNode.idString(), requestBuilder, currentTimeMs,true, requestTimeoutMs, nextRequestHandler);
        client.send(clientRequest, currentTimeMs);
        transactionManager.setInFlightCorrelationId(clientRequest.correlationId());
        client.poll(retryBackoffMs, time.milliseconds());
        return true;
    } catch (IOException e) {
        maybeFindCoordinatorAndRetry(nextRequestHandler);
        return true;
    }
}
```

InitProducerIdRequest的发送逻辑比较简单，这里需要注意的是**在选择处理请求的目标节点时，仅开启了幂等的Producer只需获取负载最小的Broker节点发送请求即可(leastLoadedNode)**，事务的情况下面再分析。

#### Broker端处理 generateProducerId

InitProducerIdRequest会被**KafkaApis#handleInitProducerIdRequest**方法接受处理，方法内调用**TransactionCoordinator#handleInitProducerId()**，
最终是通过**ProducerIdManage#generateProducerId**方法产生一个PID，这里只关注PID的生成，其他源码未贴出，如下：

```
  def generateProducerId(): Long = {
    this synchronized {
      if (nextProducerId > currentProducerIdBlock.blockEndId) {
        // 当前实例的PID端已耗尽 ，重新获取
        getNewProducerIdBlock()
        nextProducerId = currentProducerIdBlock.blockStartId + 1
      } else {
       //未耗尽，下次获取值+1
        nextProducerId += 1
      }
      //返回本次获取的PID
      nextProducerId - 1
    }
  }
```

由上可知，ProducerIdManager对象主要是Broker实例用来管理PID信息，当第一次获取或本地已获取的PID段耗尽后，调用`getNewProducerIdBlock()`获取新的PID段，否则使用本地PID段自增返回。
Kafka是如何获取新的PID段的，`getNewProducerIdBlock()`方法源码如下：

```
  private def getNewProducerIdBlock(): Unit = {
    var zkWriteComplete = false
    while (!zkWriteComplete) { //直到从Zookeeper获取到PID段
      // refresh current producerId block from zookeeper again  
      val (dataOpt, zkVersion) = zkClient.getDataAndVersion(ProducerIdBlockZNode.path)

      // generate the new producerId block
      currentProducerIdBlock = dataOpt match {
        case Some(data) =>
          //从zookeeper获取最新的PID信息
          val currProducerIdBlock = ProducerIdManager.parseProducerIdBlockData(data)

          if (currProducerIdBlock.blockEndId > Long.MaxValue - ProducerIdManager.PidBlockSize) {
            //当 PID 分配超过限制时,直接报错 Long类型  2^64  不太可能用完
            // we have exhausted all producerIds (wow!), treat it as a fatal error
            fatal(s"Exhausted all producerIds as the next block's end producerId is will has exceeded long type limit (current block end producerId is ${currProducerIdBlock.blockEndId})")
            throw new KafkaException("Have exhausted all producerIds.")
          }
          //val PidBlockSize: Long = 1000L   加1000获取
          ProducerIdBlock(brokerId, currProducerIdBlock.blockEndId + 1L, currProducerIdBlock.blockEndId + ProducerIdManager.PidBlockSize)
        case None =>
          //第一次获取PID段 获取0-999的PID端
          debug(s"There is no producerId block yet (Zk path version $zkVersion), creating the first block")
          ProducerIdBlock(brokerId, 0L, ProducerIdManager.PidBlockSize - 1)
      }

      val newProducerIdBlockData = ProducerIdManager.generateProducerIdBlockJson(currentProducerIdBlock)
      //尝试写入zookeeper
      val (succeeded, version) = zkClient.conditionalUpdatePath(ProducerIdBlockZNode.path, newProducerIdBlockData, zkVersion, Some(checkProducerIdBlockZkData))
      zkWriteComplete = succeeded

      if (zkWriteComplete)
        info(s"Acquired new producerId block $currentProducerIdBlock by writing to Zk with path version $version")
    }
  }

```

方法中的PID段申请是通过Zookeeper实现。Kafka会在ZK中维护`/latest_producer_id_block`节点，Broker首先要获取该节点下的信息，并累加需申请的长度，最后把自己申请的PID段长度写回到这个节点，重复这个过程直至成功写回ZK。
ZK节点的信息格式如下：

```
{"version":1,"broker":35,"block_start":"4000","block_end":"4999"}
```

* version 版本，乐观锁
* broker  BrokerId
* block_start 对应实例申请的PID段起始位置
* block_end 对应实例申请的PID段结束位置

#### 幂等消息发送

Broker端返回的PID信息，由KafkaProducer侧的TransactionManager通过`ProducerIdAndEpoch`属性维护，该对象不仅保存了申请到的producerId，还有一个epoch属性，也是由Broker返回，
主要用于producer有效判断，防止多个producer客户端使用同一个PID进行消息发送，当发送消息的Producer的epoch不等于Broker端存储的元数据中的值，则会返回异常。

幂等消息与普通消息发送流程基本一致，但幂等消息需要为ProducerBatch设置producerId、epoch以及sequenceNumber参数。这一步是在Sender从Accumulator中拉取消息时完成的，`RecordAccumulator#drainBatchesForOneNode()`方法
源码如下：

```
private List<ProducerBatch> drainBatchesForOneNode(Cluster cluster, Node node, int maxSize, long now) {
    int size = 0;
    List<PartitionInfo> parts = cluster.partitionsForNode(node.id());
    List<ProducerBatch> ready = new ArrayList<>();
    /* to make starvation less likely this loop doesn't start at 0 */
    int start = drainIndex = drainIndex % parts.size();
    do {
        PartitionInfo part = parts.get(drainIndex);
        TopicPartition tp = new TopicPartition(part.topic(), part.partition());
        this.drainIndex = (this.drainIndex + 1) % parts.size();
        //分区锁，有序保证
        if (isMuted(tp))
            continue;
        Deque<ProducerBatch> deque = getDeque(tp);
        if (deque == null)
            continue;
        synchronized (deque) {
            ProducerBatch first = deque.peekFirst();
            if (first == null) continue;
            
            boolean backoff = first.attempts() > 0 && first.waitedTimeMs(now) < retryBackoffMs;
            ////该ProducerBatch是重试batch,且未到时间
            if (backoff)  continue;
        
            if (size + first.estimatedSizeInBytes() > maxSize && !ready.isEmpty()) {
                //超出max.request.size
                break;
            } else {
                // 幂等及事务消息发送前检验，如producerIdAndEpoch是否有效、是否有未响应的消息等
                // 普通消息，方法返回false
                if (shouldStopDrainBatchesForPartition(first, tp))
                    break;
                
                boolean isTransactional = transactionManager != null && transactionManager.isTransactional();
                ProducerIdAndEpoch producerIdAndEpoch = transactionManager != null ? transactionManager.producerIdAndEpoch() : null;
                ProducerBatch batch = deque.pollFirst();
                
                if (producerIdAndEpoch != null && !batch.hasSequence()) {
                    //幂等及事务消息处理
                    transactionManager.maybeUpdateProducerIdAndEpoch(batch.topicPartition);
                    //pid 、epoch及sequenceNumber设置
                    batch.setProducerState(producerIdAndEpoch, transactionManager.sequenceNumber(batch.topicPartition), isTransactional);
                    //自增，下一条消息的sequenceNumber
                    transactionManager.incrementSequenceNumber(batch.topicPartition, batch.recordCount);
                    transactionManager.addInFlightBatch(batch);
                }
                batch.close();
                //size 累加
                size += batch.records().sizeInBytes();
                ready.add(batch);
                batch.drained(now);
            }
        }
    } while (start != drainIndex);
    return ready;
}
```

sequenceNumber相关的操作都是通过TransactionManager类实现的：

* TransactionManager#sequenceNumber()，用于获取指定分区的序列号，实现如下：

```
//同步方法 返回值类型为Integer
synchronized Integer sequenceNumber(TopicPartition topicPartition) {
    //每个分区单独维护，初始值从0开始
    return topicPartitionBookkeeper.getOrCreatePartition(topicPartition).nextSequence;
}
```

* TransactionManager#incrementSequenceNumber，实现sequenceNumber的自增，实现如下：

```
//同步方法
synchronized void incrementSequenceNumber(TopicPartition topicPartition, int increment) {
    //获取当前分区的下一个序列号
    Integer currentSequence = sequenceNumber(topicPartition);
    //计算下一条消息的sequenceNumber
    currentSequence = DefaultRecordBatch.incrementSequence(currentSequence, increment);
    //更新nextSequence
    topicPartitionBookkeeper.getPartition(topicPartition).nextSequence = currentSequence;
}
```

sequenceNumber的类型是Integer，当sequenceNumber大于Integer.MAX_VALUE后，DefaultRecordBatch.incrementSequence()方法会重置sequenceNumber，再次从0开始递增。

```
public static int incrementSequence(int sequence, int increment) {
    if (sequence > Integer.MAX_VALUE - increment)
        return increment - (Integer.MAX_VALUE - sequence) - 1;
    return sequence + increment;
}
```

#### Broker处理幂等消息

普通消息的完整处理可见[Kafka HandleProduceRequest](https://guann1ng.github.io/kafka/2021/08/22/Kafka-HandleProduceRequest/)一文，下面将主要分析有关幂等性处理的源码。
`Log#append()`方法中在进行消息追加前会调用`Log#analyzeAndValidateProducerState()`方法校验幂等及事务状态。实现如下：

```
  private def analyzeAndValidateProducerState(appendOffsetMetadata: LogOffsetMetadata,records: MemoryRecords,origin: AppendOrigin):
  (mutable.Map[Long, ProducerAppendInfo], List[CompletedTxn], Option[BatchMetadata]) = {
    val updatedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    var relativePositionInSegment = appendOffsetMetadata.relativePositionInSegment

    records.batches.forEach { batch =>
      //ProducerId不为null，该消息为幂等或事务消息
      if (batch.hasProducerId) {
        if (origin == AppendOrigin.Client) {
          //获取producerId对应的ProducerStateEntry，该对象内会保存之前发送的5批ProducerBatch元信息
          val maybeLastEntry = producerStateManager.lastEntry(batch.producerId)
          //查询是否有重复消息
          maybeLastEntry.flatMap(_.findDuplicateBatch(batch)).foreach { duplicate =>
            return (updatedProducers, completedTxns.toList, Some(duplicate))
          }
        }
        val firstOffsetMetadata = if (batch.isTransactional)
          Some(LogOffsetMetadata(batch.baseOffset, appendOffsetMetadata.segmentBaseOffset, relativePositionInSegment))
        else
          None
        //sequence验证
        val maybeCompletedTxn = updateProducers(producerStateManager, batch, updatedProducers, firstOffsetMetadata, origin)
        maybeCompletedTxn.foreach(completedTxns += _)
      }
      relativePositionInSegment += batch.sizeInBytes
    }
    (updatedProducers, completedTxns.toList, None)
  }
```

这里关于幂等消息的验证主要分为两部分：

* 是否是重复消息Batch：通过`producerStateManager.lastEntry(batch.producerId)`获取该PID已完成消息追加的最新5个BatchMetadata，遍历判断是否存在相同批次；
*  updateProducers()中会调用`checkSequence()`方法验证sequence是否连续。

下面将分别分析具体的源码实现。


##### findDuplicateBatch

ProducerStateManager对象中维护了<PID,ProducerStateEntry>的Map结构，lastEntry()方法的功能是通过PID获取对应的ProducerStateEntry。

```
private val producers = mutable.Map.empty[Long, ProducerStateEntry]
def lastEntry(producerId: Long): Option[ProducerStateEntry] = producers.get(producerId)
```

ProducerStateEntry定义如下：

```
private[log] class ProducerStateEntry(val producerId: Long, //PID
                                      //BatchMetadata队列
                                      val batchMetadata: mutable.Queue[BatchMetadata], 
                                      var producerEpoch: Short,
                                      var coordinatorEpoch: Int,
                                      var lastTimestamp: Long,
                                      var currentTxnFirstOffset: Option[Long]) {


  def addBatch(producerEpoch: Short, lastSeq: Int, lastOffset: Long, offsetDelta: Int, timestamp: Long): Unit = {
    maybeUpdateProducerEpoch(producerEpoch)
    addBatchMetadata(BatchMetadata(lastSeq, lastOffset, offsetDelta, timestamp))
    this.lastTimestamp = timestamp
  }

  private def addBatchMetadata(batch: BatchMetadata): Unit = {
    // ProducerStateEntry.NumBatchesToRetain = 5
    if (batchMetadata.size == ProducerStateEntry.NumBatchesToRetain)
      //移除最早的BatchMetadata
      batchMetadata.dequeue()
    batchMetadata.enqueue(batch)
  }


  def findDuplicateBatch(batch: RecordBatch): Option[BatchMetadata] = {
    if (batch.producerEpoch != producerEpoch)
       None
    else
      batchWithSequenceRange(batch.baseSequence, batch.lastSequence)
  }

  def batchWithSequenceRange(firstSeq: Int, lastSeq: Int): Option[BatchMetadata] = {
    val duplicate = batchMetadata.filter { metadata =>
      //判断Batch是否相同
      firstSeq == metadata.firstSeq && lastSeq == metadata.lastSeq
    }
    duplicate.headOption
  }
}

```

可以看到，BatchMetadata通过队列维护，`addBatchMetadata()`中，当此时的队列长度等于5时，会先进行移除再插入，**Broker端最多只会维护最新的5个消息追加Batch元数据，这也是当开启幂等后，
KafkaProducer会要求max.in.flight.requests.per.connection的配置值不超过5的原因**。

`findDuplicateBatch()`方法则通过遍历`Queue[BatchMetadata]`并对比新的RecordBatch是否为重复数据。

##### checkSequence

checkSequence方法主要是校验消息的sequenceNumber是否连续，若不连续，抛出OutOfOrderSequenceException。实现如下：

```
private def checkSequence(producerEpoch: Short, appendFirstSeq: Int, offset: Long): Unit = {
    if (producerEpoch != updatedEntry.producerEpoch) {
      if (appendFirstSeq != 0) {    
        if (updatedEntry.producerEpoch != RecordBatch.NO_PRODUCER_EPOCH) {
          //epoch不同，且之前存在epoch，此时对应的pid已过期，新epoch的seq不是从0开始，抛出异常
          throw new OutOfOrderSequenceException(...)
        }
      }
    } else {
      val currentLastSeq = if (!updatedEntry.isEmpty)
        updatedEntry.lastSeq
      else if (producerEpoch == currentEntry.producerEpoch)
        currentEntry.lastSeq
      else
        RecordBatch.NO_SEQUENCE
       //相同的epoch seq不连续 抛出异常
      if (!(currentEntry.producerEpoch == RecordBatch.NO_PRODUCER_EPOCH || inSequence(currentLastSeq, appendFirstSeq))) {
        throw new OutOfOrderSequenceException(...)
      }
    }
}

//序列号比较
private def inSequence(lastSeq: Int, nextSeq: Int): Boolean = {
    nextSeq == lastSeq + 1L || (nextSeq == 0 && lastSeq == Int.MaxValue)
}

```

### 幂等消息有序性

普通消息需通过将max.in.flight.requests.per.connection设置为1来保证有序性，即对同一个Broker节点只允许有一个未获得响应的请求，但这会导致性能的下降，Kafka开启幂等后，
max.in.flight.requests.per.connection的值最大是可以等于5的，此时KafkaProducer如何保证消息的有序性呢？

介绍Broker端处理幂等消息时，`checkSequence()`方法会判断序列号是否连续，若不连续则会抛出异常。KafkaProducer收到异常后，会将对应的ProducerBatch再次放入Accumulator中，待下次消息发送进行拉取，实现重试，`reenqueue()`实现如下：

```
public void reenqueue(ProducerBatch batch, long now) {
    batch.reenqueued(now);
    Deque<ProducerBatch> deque = getOrCreateDeque(batch.topicPartition);
    synchronized (deque) {
        if (transactionManager != null)
            //幂等及事务消息
            insertInSequenceOrder(deque, batch);
        else
            deque.addFirst(batch);
    }
}
```

普通的消息直接追加到待发送队列末尾，幂等及事务消息则是通过`insertInSequenceOrder`实现重试，方法源码如下：

```
private void insertInSequenceOrder(Deque<ProducerBatch> deque, ProducerBatch batch) {
    //不存在序列号 抛出异常
    if (batch.baseSequence() == RecordBatch.NO_SEQUENCE)
        throw new IllegalStateException("Trying to re-enqueue a batch which doesn't have a sequence even though idempotency is enabled.");

    if (transactionManager.nextBatchBySequence(batch.topicPartition) == null)
        throw new IllegalStateException("We are re-enqueueing a batch which is not tracked as part of the in flight " +
            "requests. batch.topicPartition: " + batch.topicPartition + "; batch.baseSequence: " + batch.baseSequence());
    //获取头部的ProducerBatch  
    ProducerBatch firstBatchInQueue = deque.peekFirst();
    //通过序列号比较
    if (firstBatchInQueue != null && firstBatchInQueue.hasSequence() && firstBatchInQueue.baseSequence() < batch.baseSequence()) {
        
        List<ProducerBatch> orderedBatches = new ArrayList<>();
        //while找到重试的ProducerBatch对应的位置
        while (deque.peekFirst() != null && deque.peekFirst().hasSequence() && deque.peekFirst().baseSequence() < batch.baseSequence())
            orderedBatches.add(deque.pollFirst());
        deque.addFirst(batch);
        for (int i = orderedBatches.size() - 1; i >= 0; --i) {
            deque.addFirst(orderedBatches.get(i));
        }
    } else {
        deque.addFirst(batch);
    }
}

```

Sender线程再次拉取时，若ProducerBatch已有序列号，则batch为重试batch，等待inFlightRequests中请求发送完成，才允许再次发送这个Topic-Partition的数据。

```
int firstInFlightSequence = transactionManager.firstInFlightSequence(first.topicPartition);
if (firstInFlightSequence != RecordBatch.NO_SEQUENCE && first.hasSequence()
    && first.baseSequence() != firstInFlightSequence)
    // If the queued batch already has an assigned sequence, then it is being retried.
    // In this case, we wait until the next immediate batch is ready and drain that.
    // We only move on when the next in line batch is complete (either successfully or due to
    // a fatal broker error). This effectively reduces our in flight request count to 1.
    return true;
```


幂等时的有序保证实现机制概括为：

* Broker验证batch的sequence number值，不连续时，直接返回异常；
* Producer请求重试时，batch在reenqueue时会根据sequence number值放到合适的位置（有序保证之一）；
* Sender线程发送时，在遍历queue中的batch时，会检查这个batch是否是重试的batch，如果是的话，只有这个batch是最旧的那个需要重试的batch，才允许发送，否则本次发送跳过这个Topic-Partition数据的发送等待下次发送。



## 事务

Kafka通过引入幂等实现了单会话单TopicPartition的Exactly-Once语义，但幂等无法提供跨多个TopicPartition和跨会话场景下的Exactly-Once保证,Kafka引入事务来弥补这个缺陷，
**通过事务可以保证对多个分区写入操作的原子性**。

Kafka引入事务可以保证**Producer跨会话跨分区的消息幂等发送，以及跨会话的事务恢复**，但不能保证已提交的事务中所有消息都能够被消费，原因有以下几点：

* 对于采用日志压缩策略的主题(cleanup.policy=compact)，相同key的消息。后写入的消息会覆盖前面写入的消息；
* 事务消息可能持久化在同一个分区的不同LogSegment中，当老的日志分段被删除，对应的消息会丢失；
* Consumer通过seek()方法指定offset进行消费，从而遗漏事务中的部分消息；
* Consumer可能没有订阅这个事务涉及到的全部Partition。    

### 事务使用

只需配置KafkaProducer的`transactional.id`参数，即可启用事务，同时KafkaProducer会默认的将`enable.idempotence`幂等参数设置为true，若用户手动将`enable.idempotence`参数设为false，
将抛出ConfigException。 

```
boolean idempotenceEnabled() {
    boolean userConfiguredIdempotence = this.originals().containsKey(ENABLE_IDEMPOTENCE_CONFIG);
    //是否配置了transactional.id
    boolean userConfiguredTransactions = this.originals().containsKey(TRANSACTIONAL_ID_CONFIG);
    boolean idempotenceEnabled = userConfiguredIdempotence && this.getBoolean(ENABLE_IDEMPOTENCE_CONFIG);
    //配置transactional.id，且enable.idempotence为false
    if (!idempotenceEnabled && userConfiguredIdempotence && userConfiguredTransactions)
        throw new ConfigException("Cannot set a " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " without also enabling idempotence.");
    return userConfiguredTransactions || idempotenceEnabled;
}
```

Kafka事务API使用示例代码如下：

```
// 创建生产者 消费者
KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProps());
KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProps());
// 初始化事务
producer.initTransactions();
consumer.subscribe(Arrays.asList("topic"));
while(true){
    ConsumerRecords<String, String> records = consumer.poll(500);
    if(!records.isEmpty()){
        try {
            // 开启事务
            producer.beginTransaction();
            Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
            for(ConsumerRecord record : records){
                // doSomething..
                // 记录提交的偏移量
                commits.put(new TopicPartition(record.topic(), record.partition()),new OffsetAndMetadata(record.offset()));
                // 生产消息
                Future<RecordMetadata> metadataFuture = producer.send(new ProducerRecord<>("new-topic",record.value()));
            }
            // 提交偏移量
            producer.sendOffsetsToTransaction(commits,"groupId");
            // 事务提交
            producer.commitTransaction();
        }catch (Exception e){
             e.printStackTrace();
             //终止事务
             producer.abortTransaction();
        }
    }
}
```


### 实现机制

Kafka事务需要确保跨会话多分区的写入保证原子性，实现机制重点如下：

#### 跨会话(transactional.id)

幂等性引入的PID机制会在Producer重启后更新为新的PID，无法确保Producer fail后事务继续正确执行，Kafka Producer引入TransactionId参数，**由用户通过transactional.id配置**。Kafka保证具有相同TransactionId
的新Producer被创建后，旧的Producer将不再工作(通过epoch实现)，且新的Producer实例可以保证任何未完成的事务要么被commit，要么被abort。

#### 2PC(TransactionCoordinator) 

Kafka采用[两阶段提交2PC](https://zh.wikipedia.org/wiki/%E4%BA%8C%E9%98%B6%E6%AE%B5%E6%8F%90%E4%BA%A4)的思想来保证所有分区的一致性，Broker端引入TransactionCoordinator，
作为事务协调者角色来管理事务，指示所有参与事务的分区进行commit或abort。每个Kafka Server实例(Broker)启动时都会实例化一个TransactionCoordinator对象，定义如下：

```
class TransactionCoordinator(brokerId: Int,
                             txnConfig: TransactionConfig,
                             scheduler: Scheduler,
                             createProducerIdGenerator: () => ProducerIdGenerator,  //PID管理
                             txnManager: TransactionStateManager, // 事务状态管理
                             txnMarkerChannelManager: TransactionMarkerChannelManager, //通知参与事务的分区事务终结
                             time: Time,
                             logContext: LogContext)

```

重要的属性有以下三个：

* ProducerIdGenerator，负责PID的维护与生成；
* TransactionStateManager，事务状态管理；
* TransactionMarkerChannelManager，负责通知参与事务的分区leader事务结果。

#### TransactionCoordinator高可用(_transaction_state)

为防止TransactionCoordinator突然宕机，Kafka会将事务数据持久化到一个内部Topic **"_transaction_state"**内，通过消息的多副本机制，即**min.isr + acks**确保事务状态不丢失，
TransactionCoordinator在故障恢复时可从这个topic中读取数据，确保事务事务可恢复。

Producer与TransactionCoordinator的对应关系通过`TransactionId`的hash与`_transaction_state`的分区数取模实现：

```
def partitionFor(transactionalId: String): Int = Utils.abs(transactionalId.hashCode) % transactionTopicPartitionCount
```

其中transactionTopicPartitionCount为主题`_transaction_state`的分区个数 ，可通过broker端参数transaction.state.log.num.partitions来配置，默认值为50。**获得的分区编号对应的分区Leader副本
所在Broker实例中的TransactionCoordinator对象即为该Producer从属的TransactionCoordinator**。


#### 事务状态


| 状态枚举 | 说明  |
|----------|-------|
| Empty   | Transaction has not existed yet <br> transition: received AddPartitionsToTxnRequest => Ongoing <br>             received AddOffsetsToTxnRequest => Ongoing  |
| Ongoing    | Transaction has started and ongoing <br> transition: received EndTxnRequest with commit => PrepareCommit<br>            received EndTxnRequest with abort => PrepareAbort<br>            received AddPartitionsToTxnRequest => Ongoing<br>            received AddOffsetsToTxnRequest => Ongoing  |
| PrepareCommit    | LPOP  |
| PrepareAbort    | LPOP  |
| CompleteCommit    | LPOP  |
| CompleteAbort    | LPOP  |
| Dead    | LPOP  |
| PrepareEpochFence    | LPOP  |

```

/**
 * Transaction has not existed yet
 *
 * transition: received AddPartitionsToTxnRequest => Ongoing
 *             received AddOffsetsToTxnRequest => Ongoing
 */
private[transaction] case object Empty extends TransactionState { val byte: Byte = 0  override def isExpirationAllowed: Boolean = true }

/**
 * Transaction has started and ongoing
 *
 * transition: received EndTxnRequest with commit => PrepareCommit
 *             received EndTxnRequest with abort => PrepareAbort
 *             received AddPartitionsToTxnRequest => Ongoing
 *             received AddOffsetsToTxnRequest => Ongoing
 */
private[transaction] case object Ongoing extends TransactionState { val byte: Byte = 1 }


// Group is preparing to commit
// transition: received acks from all partitions => CompleteCommit
private[transaction] case object PrepareCommit extends TransactionState { val byte: Byte = 2}

// Group is preparing to abort
// transition: received acks from all partitions => CompleteAbort
private[transaction] case object PrepareAbort extends TransactionState { val byte: Byte = 3 }

//Group has completed commit 
//Will soon be removed from the ongoing transaction cache
private[transaction] case object CompleteCommit extends TransactionState { val byte: Byte = 4 override def isExpirationAllowed: Boolean = true }

//Group has completed abort .Will soon be removed from the ongoing transaction cache
private[transaction] case object CompleteAbort extends TransactionState { val byte: Byte = 5  override def isExpirationAllowed: Boolean = true }

//TransactionalId has expired and is about to be removed from the transaction cache
private[transaction] case object Dead extends TransactionState { val byte: Byte = 6 }

 //We are in the middle of bumping the epoch and fencing out older producers.
private[transaction] case object PrepareEpochFence extends TransactionState { val byte: Byte = 7}

```
    
 
    

### 执行流程

流程图如下：

![Kafka Transaction](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/kafka%20transaction.png)

官网详解可见[Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging#KIP98ExactlyOnceDeliveryandTransactionalMessaging-DataFlow)

#### Finding a transaction coordinator -- the FindCoordinatorRequest

TransactionCoordinator负责分配PID和事务管理，因此Producer发送事务消息时的第一步就是找出对应的TransactionCoordinator，Producer会向LeastLoadedNode(inflightRequests.size对应的Broker)发送FindCoordinatorRequest，
Broker收到请求后，**根据transactionalId的哈希值计算主题_transaction_state中的分区编号，再找出分区Leader所在的Broker节点**，该Broker节点即为这个transactionalId对应的TransactionCoordinator节点。


#### Getting a producer Id -- the InitPidRequest

找到TransactionCoordinator后，当前Producer就可以向TransactionCoordinator发送InitPidRequest获取PID（只开启幂等未开启事务的Producer,可以向任意Broker节点发送请求），PID的分配同幂等部分处理，
事务相关的TransactionCoordinator处理流程如下：

* 1.判断transactionId是否有对应的事务状态，如果没有，初始化其事务meta信息 TransactionMetadata（会给其分配一个PID，初始的epoch为-1），如果有事务状态，获取之前的状态；

```
val coordinatorEpochAndMetadata = txnManager.getTransactionState(transactionalId).flatMap {
        case None =>
          val producerId = producerIdManager.generateProducerId()
          val createdMetadata = new TransactionMetadata(...）
          txnManager.putTransactionStateIfNotExists(createdMetadata)
        case Some(epochAndTxnMetadata) => Right(epochAndTxnMetadata)
}
```
* 2.若之前存在事务状态信息，校验其TransactionMetadata的状态信息，进行事务恢复:
    见kafka.coordinator.transaction.TransactionCoordinator#prepareInitProducerIdTransit
```
txnMetadata.state match {
    case PrepareAbort | PrepareCommit =>
      Left(Errors.CONCURRENT_TRANSACTIONS)
    case CompleteAbort | CompleteCommit | Empty =>
      val transitMetadataResult =
        // If the epoch is exhausted and the expected epoch (if provided) matches it, generate a new producer ID
        if (txnMetadata.isProducerEpochExhausted &&
            expectedProducerIdAndEpoch.forall(_.epoch == txnMetadata.producerEpoch)) {
          val newProducerId = producerIdManager.generateProducerId()
          Right(txnMetadata.prepareProducerIdRotation(newProducerId, transactionTimeoutMs, time.milliseconds(),
            expectedProducerIdAndEpoch.isDefined))
        } else {
          txnMetadata.prepareIncrementProducerEpoch(transactionTimeoutMs, expectedProducerIdAndEpoch.map(_.epoch),
            time.milliseconds())
        }
      transitMetadataResult match {
        case Right(transitMetadata) => Right((coordinatorEpoch, transitMetadata))
        case Left(err) => Left(err)
      }
    case Ongoing =>
      // indicate to abort the current ongoing txn first. Note that this epoch is never returned to the
      // user. We will abort the ongoing transaction and return CONCURRENT_TRANSACTIONS to the client.
      // This forces the client to retry, which will ensure that the epoch is bumped a second time. In
      // particular, if fencing the current producer exhausts the available epochs for the current producerId,
      // then when the client retries, we will generate a new producerId.
      Right(coordinatorEpoch, txnMetadata.prepareFenceProducerEpoch())

    case Dead | PrepareEpochFence =>
      val errorMsg = s"Found transactionalId $transactionalId with state ${txnMetadata.state}. " +
        s"This is illegal as we should never have transitioned to this state."
      fatal(errorMsg)
      throw new IllegalStateException(errorMsg)
}
```

* 3.将transactionId与相应的TransactionMetadata持久化到事务日志中，对于新的transactionId，这个持久化的数据主要是保存transactionId与PID关系信息

#### Starting a Transaction – The beginTransaction() API

调用org.apache.kafka.clients.producer.KafkaProducer#beginTransaction即可，Producer端将本地事务状态标记为INITIALIZING状态，表明开启一个事务。

#### The consume-transform-produce loop

这个阶段囊括了整个事务的数据处理过程，如拉取数据，处理业务，写入下游等过程。

具体实现可分为以下几步：

##### AddPartitionsToTxnRequest

当Producer向一个TopicPartition发送数据前，需要先向TransactionCoordinator发送AddPartitionsToTxnRequest请求，TransactionCoordinator会将这个TopicPartition更新
到TransactionId对应的TransactionMetadata中，并将<transactionId, TopicPartition>的对应关系存储在主题_transaction_state中。

如果该分区是对应事务中的第一个分区， 那么此时TransactionCoordinator还会启动对该事务的计时。

##### ProduceRequest

生产者通过ProduceRequest请求发送消息到用户自定义主题中， 这一点和发送普通消息时相同，和普通的消息不同的是， 事务消息内的ProducerBatch中会包含实质的PID、 producerEpoch和sequence number参数。源码详见
org.apache.kafka.clients.producer.internals.RecordAccumulator#drainBatchesForOneNode，Sender方法获取消息批次时完成事务参数设置。

```
// org.apache.kafka.clients.producer.internals.ProducerBatch#setProducerState
public void setProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence, boolean isTransactional) {
    recordsBuilder.setProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence, isTransactional);
}
```

##### AddOffsetsToTxnRequest

调用`sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,ConsumerGroupMetadata groupMetadata)`方法可以在一个事务批次里处理consume-transform-produce，这个方法会向TransactionCoordinator发送
AddOffsetsToTxnRequest请求，将group对应的_consumer_offsets的Partition（与写入涉及的TopicPartition一样）保存到事务对应的meta中，并持久化到主题_transaction_state中。

```
public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
    throwIfInvalidGroupMetadata(groupMetadata);
    throwIfNoTransactionManager();
    throwIfProducerClosed();
    TransactionalRequestResult result = transactionManager.sendOffsetsToTransaction(offsets, groupMetadata);
    sender.wakeup();
    result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
}
```

##### TxnOffsetsCommitRequest

Producer在收到TransactionCoordinator关于AddOffsetsToTxnRequest请求的结果后，后再次发送TxnOffsetsCommitRequest请求给对应的GroupCoordinator，从而将本次事务
中包含的消费位移信息offsets存储到主题_consumer_offsets中。

#### Committing or Aborting a Transaction

上述事务流程处理完成后，Producer需要调用commitTransaction()或者abortTransaction()方法来commit或者abort这个事务操作。

##### EndTxnRequest

无论调用commitTransaction()方法还是abortTransaction()方法，Producer都会向Transaction Coordinator发送EndTxnRequest请求，以此来通知它提交(Commit)事务还是中止(Abort)事务。

TransactionCoordinator在收到EndTxnRequest请求后会执行如下操作:

* 更新事务meta信息，进行事务状态转移，将PREPARE_COMMIT或PREPARE_ABORT消息写入主题_transaction_state;
* 根据事务meta信息，向事务涉及到的所有TopicPartition的leader发送WriteTxnMarkerRequest请求，将COMMIT或ABORT信息写入用户所使用的普通主题和_consumer_offsets;
* 完成事务，将COMPLETE_COMMIT或COMPLETE_ABORT信息写入内部主题_transaction_state

##### WriteTxnMarkerRequest

WriteTxnMarkersRequest请求是由TransactionCoordinator发向事务中各个分区的leader节点的，Transaction Marker也叫做控制消息(ControlBatch)，它的作用主要是告诉这个事务操作涉
及的TopicPartitions当前的事务操作已经完成，可以执行commit或者abort。

ControlBatch和普通的消息一样存储在对应日志文件中，但控制消息不会被返回给consumer客户端。主要通过扩展RecordBatch中的attribute字段实现，来标识消息是否是控制消息，是否在事务中等。

* org.apache.kafka.common.record.RecordBatch
```
boolean isTransactional();

int partitionLeaderEpoch();

boolean isControlBatch();

```
* org.apache.kafka.common.record.ControlRecordType

```
public enum ControlRecordType {
    ABORT((short) 0),
    COMMIT((short) 1),

    // Raft quorum related control messages.
    LEADER_CHANGE((short) 2),
    SNAPSHOT_HEADER((short) 3),
    SNAPSHOT_FOOTER((short) 4),

    // UNKNOWN is used to indicate a control type which the client is not aware of and should be ignored
    UNKNOWN((short) -1);
```


##### Writing the final Commit or Abort Message

当这个事务涉及到所有TopicPartition都已经把WriteTxnMarkerRequest信息持久化到日志文件之后，TransactionCoordinator将最终的COMPLETE_COMMIT 或COMPLETE_ABORT信息写入主题_transaction_state以表明当前事务已经结束，
此时TransactionCoordinator缓存的很多关于这个事务的数据可以被清除,且主题_transaction_state中所有关于该事务的消息也可以被设置为墓碑消息，等到日志压缩处理。


### 事务成功保证

Producer调用完commit接口后，TransactionCoordinator将事务状态转化为了PreCommit，此时如果向其他的TopicPartition发送WriteTxnMarkerRequest请求处理失败（机器挂了等等），那么这个事务是不是要回滚？如果回滚的话，
已经部分成功的TopicPartition已经对Consumer可见了，是不是可能会出现中间状态?

如果一个事务的状态转换为PreCommit，那么是可以保证这个事务最终成功的，不可能出现回滚的情况，具体是怎么做的呢，有如下几步关键点：
1. TransactionCoordinator在收到Producer的commit请求后，只要写入本地_transaction_stat成功，那么就会返回Producer success（并不会等待WriteTxnMarker的处理结果）；
2. WriteTxnMarker的请求，本质类似Produce发送消息的处理，这个消息的处理只需要是at least once即可（有重复也是无所谓的），这个是Kafka本身机制保证的。

### Kafka Fencing机制

在分布式系统中，一个instance的宕机或失联，集群往往会自动启动或选举一个新的实例来代替它的工作。此时若原实例恢复了，那么集群中就产生了两个具有相同职责的实例，最终使得整个集群处于混乱状态,这就是脑裂问题，
此时前一个instance就被称为“僵尸实例（Zombie Instance）”。

在Kafka的事务场景下，用到Fencing机制有两个地方：

* TransactionCoordinator Fencing
* Producer Fencing

#### TransactionCoordinator Fencing

当TransactionCoordinator所在Broke实例发生LongTime FullGC时，由于STW，可能会与ZK连接超时导致临时节点消失进而触发leader选举，如果_transaction_state发生了leader选举，
TransactionCoordinator就会切换，如果此时旧的TransactionCoordinator FGC完成，在还没来得及同步到最细meta之前，会有一个短暂的时刻，对于一个txn.id而言就是这个时刻可能出现了两个TransactionCoordinator。

Kafka通过TransactionCoordinator对应的Epoch来判断那个实例是有效的。此处的Epoch就是对应_transaction_state Partition的Epoch值（每当leader切换一次，该值就会自增1）。

#### Producer Fencing

对于相同PID和txn.id的Producer，Broker端会记录最新的Epoch值，拒绝来自zombie Producer（Epoch值小的Producer）的请求。比如当同配置的Producer2在启动时，会向TransactionCoordinator发送InitPIDRequest请求，
此时TransactionCoordinator已经有了这个txn.id对应的meta，会返回之前分配的PID，并把对应Epoch自增1返回，这样Producer2就被认为是最新的Producer，而Producer1就会被认为是zombie Producer，
因此，TransactionCoordinator在处理Producer1的事务请求时，会返回相应的异常信息。