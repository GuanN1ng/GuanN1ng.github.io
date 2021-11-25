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

# 幂等

Kafka引入**幂等特性来解决异常重试机制导致的消息重复问题**，开启幂等后，当发送同一条消息时，数据在Broker端只会被持久化一次，避免生产者重试导致的消息重复写入。通过将参数**enable.idempotence设置为true即可开启幂等**功能，
此时retries参数默认为Integer.MAX_VALUE，acks默认为all，并确保max.in.flight.requests.per.connection(指定了Producer在收到Broker响应之前单连接可以发送多少个Batch)不能大于5。

### 实现原理

幂等要求Broker端能够鉴别消息的唯一性，鉴于主题多分区及多Producer的情况，Kafka引入了两个机制实现幂等：

* PID(Producer ID)，用来标识生产者，全局唯一。初始化时被分配，对用户透明，KafkaProducer重启后会重新申请新的PID。
* sequence number，标识消息，KafkaProducer会为每个TopicPartition单独维护一个sequence number，消息(ProducerBatch)发送时，会设置对应分区的sequence number，从0单调递增，超出Integer.MAX_VALUE后，再次从0递增。

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
//RecordAccumulator#shouldStopDrainBatchesForPartition

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



# 事务

Kafka通过引入幂等实现了单会话单TopicPartition的Exactly-Once语义，但幂等无法提供跨多个TopicPartition和跨会话场景下的Exactly-Once保证,Kafka引入事务来弥补这个缺陷。
开启事务可以保证**Producer对多个分区写入操作的原子性，以及跨会话的事务恢复**，但不能保证已提交的事务中所有消息都能够被消费，原因有以下几点：

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

#### 高可用(_transaction_state)

为防止TransactionCoordinator突然宕机导致事务丢失，Kafka会将事务数据持久化到一个内部Topic **"_transaction_state"**内，通过消息的多副本机制，即**min.isr + acks**确保事务数据不丢失，
TransactionCoordinator在故障恢复时可从这个topic中读取数据，确保事务事务可恢复。

Producer与TransactionCoordinator的对应关系通过`TransactionId`的hash与`_transaction_state`的分区数取模实现：

```
def partitionFor(transactionalId: String): Int = Utils.abs(transactionalId.hashCode) % transactionTopicPartitionCount
```

其中transactionTopicPartitionCount为主题`_transaction_state`的分区个数 ，可通过broker端参数transaction.state.log.num.partitions来配置，默认值为50。**获得的分区编号对应的分区Leader副本
所在Broker实例中的TransactionCoordinator对象即为该Producer从属的TransactionCoordinator**。当Leader副本宕机后，从follower副本中重新选举出的新的leader副本继续承担TransactionCoordinator角色
确保事务的正常进行。


#### 事务状态

Server端事务状态定义(kafka.coordinator.transaction.TransactionState)：

| 状态枚举 | 说明  |
|----------|-------|
| Empty   | 未收到任何事务消息 <br> 转移条件: <br> received AddPartitionsToTxnRequest => Ongoing <br>             received AddOffsetsToTxnRequest => Ongoing  |
| Ongoing    | 正在接收事务消息 <br> 转移条件:<br> received EndTxnRequest with commit => PrepareCommit<br>            received EndTxnRequest with abort => PrepareAbort<br>            received AddPartitionsToTxnRequest => Ongoing<br>            received AddOffsetsToTxnRequest => Ongoing  |
| PrepareCommit    | 提交事务中，通知参与事务的分区commit <br> 转移条件: <br>received acks from all partitions => CompleteCommit |
| PrepareAbort    | 中止事务中，通知参与事务的分区abort <br> 转移条件:<br> received acks from all partitions => CompleteAbort  |
| CompleteCommit    | commit完成,所有分区已正确响应 <br>  Will soon be removed from the ongoing transaction cache |
| CompleteAbort    | abort完成,所有分区已正确响应 <br> Will soon be removed from the ongoing transaction cache|
| Dead    | TransactionalId has expired and is about to be removed from the transaction cache  |
| PrepareEpochFence    | We are in the middle of bumping the epoch and fencing out older producers.  |


Client端事务状态定义(org.apache.kafka.clients.producer.internals.TransactionManager.State)：

| 状态枚举 | 说明  |
|----------|-------|
| UNINITIALIZED   |  未开始事务 |
| INITIALIZING    | Producer调用initTransactions()方法初始化事务相关的内容  |
| READY    | Producer收到来自TransactionCoordinator的InitProducerIdResponse后，其状态会置为READY（对于已有的事务而言，是当前事务完成后Producer的状态会转移为 READY) |
| IN_TRANSACTION    |  Producer调用beginTransaction()方法，状态置为IN_TRANSACTION |
| COMMITTING_TRANSACTION    | Producer调用commitTransaction()，状态置为COMMITTING_TRANSACTION |
| ABORTING_TRANSACTION    | Producer调用abortTransaction()，状态置为ABORTING_TRANSACTION |
| ABORTABLE_ERROR    |  消息发送异常，本地事务状态置为ABORTABLE_ERROR，之后进行Abort操作 |
| FATAL_ERROR    | 无法修复的异常，事务可由任一状态转移至FATAL_ERROR，且无法再转移至其它任一状态。Producer只能关闭或生产非事务消息  | 
 
 
#### ControlRecord

在Kafka的事务实现中，**无论最终事务状态是Commit或Abort，事物执行期间发送的消息都会持久化到对应分区的消息日志**中，当事务结束时，会**将事务的最终结果以消息的形式也追加到消息日志**中。这个表征事务结果的
消息就是ControlRecord，即控制消息，事务控制消息有两种类型：COMMIT和ABORT，分别表示事务成功提交和事务失败中止。

```
val controlRecords = partitionsWithCompatibleMessageFormat.map { partition =>
  val controlRecordType = marker.transactionResult match {
    case TransactionResult.COMMIT => ControlRecordType.COMMIT
    case TransactionResult.ABORT => ControlRecordType.ABORT
  }
  val endTxnMarker = new EndTransactionMarker(controlRecordType, marker.coordinatorEpoch)
  partition -> MemoryRecords.withEndTransactionMarker(producerId, marker.producerEpoch, endTxnMarker)
}.toMap

```

通过ControlRecord可以避免事务中止时对期间的消息日志进行删改(**频繁的无序的磁盘读写**)，保证Broker的性能。
    

### 执行流程

KafkaProducer开启事务调用的第一个API是initTransactions()方法，作用是从TransactionCoordinator获取ProducerId、Epoch，完成事务初始化。

```
public void initTransactions() {
    throwIfNoTransactionManager();
    throwIfProducerClosed();
    //进行初始化
    TransactionalRequestResult result = transactionManager.initializeTransactions();
    唤醒Sender
    sender.wakeup();
    result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
}
```

TransactionManager#initializeTransactions()方法的作用是构建InitProducerIdRequest，并将请求加入待发送队列，而后由Sender进行发送。

```
public synchronized TransactionalRequestResult initializeTransactions() {
    return initializeTransactions(ProducerIdAndEpoch.NONE);
}

synchronized TransactionalRequestResult initializeTransactions(ProducerIdAndEpoch producerIdAndEpoch) {
    boolean isEpochBump = producerIdAndEpoch != ProducerIdAndEpoch.NONE;
    return handleCachedTransactionRequestResult(() -> {
        if (!isEpochBump) {
            transitionTo(State.INITIALIZING); //本地事务状态更新为INITIALIZING
            log.info("Invoking InitProducerId for the first time in order to acquire a producer ID");
        } else {
            log.info("Invoking InitProducerId with current producer ID and epoch {} in order to bump the epoch", producerIdAndEpoch);
        }
        InitProducerIdRequestData requestData = new InitProducerIdRequestData().setTransactionalId(transactionalId).setTransactionTimeoutMs(transactionTimeoutMs).setProducerId(producerIdAndEpoch.producerId).setProducerEpoch(producerIdAndEpoch.epoch);
        //响应处理器，更新本地ProducerId 和 Epoch
        InitProducerIdHandler handler = new InitProducerIdHandler(new InitProducerIdRequest.Builder(requestData),isEpochBump);
        //加入待发送队列
        enqueueRequest(handler);
        return handler.result;
    }, State.INITIALIZING);
}
```

幂等部分的内容已经分析过，Sender#runOnce()方法会调用`maybeSendAndPollTransactionalRequest()`方法先将事务相关的请求发送，实现如下：

```
private boolean maybeSendAndPollTransactionalRequest() {
    ... //省略其余源码
    //从队列中获取请求
    TransactionManager.TxnRequestHandler nextRequestHandler = transactionManager.nextRequest(accumulator.hasIncomplete());
    if (nextRequestHandler == null)
        return false;
    AbstractRequest.Builder<?> requestBuilder = nextRequestHandler.requestBuilder();
    Node targetNode = null;
    try {
        FindCoordinatorRequest.CoordinatorType coordinatorType = nextRequestHandler.coordinatorType();
        //事务请求 coordinatorType == FindCoordinatorRequest.CoordinatorType.TRANSACTION
        targetNode = coordinatorType != null ? transactionManager.coordinator(coordinatorType) : client.leastLoadedNode(time.milliseconds());
        if (targetNode != null) {
            ...// 第一次初始化 transactionCoordinator == null
        } else if (coordinatorType != null) {
            //查找对应的transactionCoordinator
            maybeFindCoordinatorAndRetry(nextRequestHandler);
            return true;
        } else {
        ... //请求发送 省略
    } catch (IOException e) {
        maybeFindCoordinatorAndRetry(nextRequestHandler);
        return true;
    }
}
```
当发送事务消息时，目标节点为对应的TransactionCoordinator所在节点，此时，事务执行的第一步转化为对`TransactionCoordinator`的获取，入口方法为maybeFindCoordinatorAndRetry()。

#### FindCoordinatorRequest

TransactionCoordinator负责分配PID和事务管理，因此KafkaProducer完成事务初始化的第一步就是找出对应的TransactionCoordinator。Producer通过发送FindCoordinatorRequest请求获取对应的TransactionCoordinator节点。
请求发送源码如下：

```
private void maybeFindCoordinatorAndRetry(TransactionManager.TxnRequestHandler nextRequestHandler) {
    if (nextRequestHandler.needsCoordinator()) {
        //事务消息，需要获取TransactionCoordinator
        transactionManager.lookupCoordinator(nextRequestHandler);
    } else {
        time.sleep(retryBackoffMs);
        metadata.requestUpdate();
    }
    //获取到对应的TransactionCoordinator后，重试未发送InitProducerIdRequest
    transactionManager.retry(nextRequestHandler);
}

private void lookupCoordinator(FindCoordinatorRequest.CoordinatorType type, String coordinatorKey) {
    switch (type) {
        case GROUP:
            //消费者组相关，后续介绍
            consumerGroupCoordinator = null; break;
        case TRANSACTION:
            transactionCoordinator = null; break;
        default:
            throw new IllegalStateException("Invalid coordinator type: " + type);
    }
    //构建FindCoordinatorRequest，加入待发送队列，由sender线程发送
    FindCoordinatorRequest.Builder builder = new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData().setKeyType(type.id()).setKey(coordinatorKey));
    enqueueRequest(new FindCoordinatorHandler(builder));
}
```

Broker端收到FindCoordinatorRequest请求后，由KafkaApis#handleFindCoordinatorRequest()方法进行处理：

```
  def handleFindCoordinatorRequest(request: RequestChannel.Request): Unit = {
    val findCoordinatorRequest = request.body[FindCoordinatorRequest]
    ...//验证
    val (partition, internalTopicName) = CoordinatorType.forId(findCoordinatorRequest.data.keyType) match {
      case CoordinatorType.GROUP =>
         (groupCoordinator.partitionFor(findCoordinatorRequest.data.key), GROUP_METADATA_TOPIC_NAME)
      case CoordinatorType.TRANSACTION =>
         //根据transactionId获取对应分区   Utils.abs(transactionalId.hashCode) % transactionTopicPartitionCount
         (txnCoordinator.partitionFor(findCoordinatorRequest.data.key), TRANSACTION_STATE_TOPIC_NAME)
      //获取内部主题_transaction_state的元数据
      val topicMetadata = metadataCache.getTopicMetadata(Set(internalTopicName), request.context.listenerName)

      ...// createFindCoordinatorResponse()定义

      if (topicMetadata.headOption.isEmpty) {
        //元数据为空
        val controllerMutationQuota = quotas.controllerMutation.newPermissiveQuotaFor(request)
        //自动创建_transaction_state 内部主题，
        autoTopicCreationManager.createTopics(Seq(internalTopicName).toSet, controllerMutationQuota)
        //返回Errors.COORDINATOR_NOT_AVAILABLE ，Producer进行重试
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => createFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode, requestThrottleMs))
      } else {
        //存在元数据
        def createResponse(requestThrottleMs: Int): AbstractResponse = {
          val responseBody = if (topicMetadata.head.errorCode != Errors.NONE.code) {
            createFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode, requestThrottleMs)
          } else {
            val coordinatorEndpoint = topicMetadata.head.partitions.asScala
              //获取目标分区的leader副本所在节点返回
              .find(_.partitionIndex == partition)
              .filter(_.leaderId != MetadataResponse.NO_LEADER_ID)
              .flatMap(metadata => metadataCache.getAliveBroker(metadata.leaderId))
              .flatMap(_.endpoints.get(request.context.listenerName.value()))
              .filterNot(_.isEmpty)
            coordinatorEndpoint match {
              case Some(endpoint) =>
                //返回目标节点
                createFindCoordinatorResponse(Errors.NONE, endpoint, requestThrottleMs)
              case _ =>
                createFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode, requestThrottleMs)
            }
          }
          responseBody
        }
        requestHelper.sendResponseMaybeThrottle(request, createResponse)
      }
    }
  }

```

Broker收到请求后，**根据transactionalId的哈希值与主题_transaction_state的分区数取模获取分区编号，若_transaction_state元数据不存在，则先进行主题的创建，若存在则找出分区Leader副本所在的Broker节点并返回**，
该Broker节点即为这个transactionalId对应的TransactionCoordinator节点。

#### InitProducerIdRequest

找到TransactionCoordinator后，Producer即可向TransactionCoordinator发送InitProducerIdRequest获取PID，PID的生成同幂等，这里主要关注事务相关的处理。Broker端处理请求的是TransactionCoordinator#handleInitProducerId()方法，实现如下：

```
  def handleInitProducerId(transactionalId: String, transactionTimeoutMs: Int, expectedProducerIdAndEpoch: Option[ProducerIdAndEpoch], responseCallback: InitProducerIdCallback): Unit = {

    if (transactionalId == null) {
      //幂等PID生成
      val producerId = producerIdGenerator.generateProducerId()
      responseCallback(InitProducerIdResult(producerId, producerEpoch = 0, Errors.NONE))
    } else if (transactionalId.isEmpty) {
      responseCallback(initTransactionError(Errors.INVALID_REQUEST))
    } else if (!txnManager.validateTransactionTimeoutMs(transactionTimeoutMs)) {
      responseCallback(initTransactionError(Errors.INVALID_TRANSACTION_TIMEOUT))
    } else {
      //获取transactionalId的事务元数据TransactionMetadata
      val coordinatorEpochAndMetadata = txnManager.getTransactionState(transactionalId).flatMap {
        case None =>
          //Broker本地缓存中不存在transactionalId对应的TransactionMetadata,创建一个新的并放入缓存
          val producerId = producerIdGenerator.generateProducerId()
          val createdMetadata = new TransactionMetadata(...)
          txnManager.putTransactionStateIfNotExists(createdMetadata)
          //否则直接从本地缓存中取出
        case Some(epochAndTxnMetadata) => Right(epochAndTxnMetadata)
      }

      val result: ApiResult[(Int, TxnTransitMetadata)] = coordinatorEpochAndMetadata.flatMap {
        existingEpochAndMetadata =>
          val coordinatorEpoch = existingEpochAndMetadata.coordinatorEpoch
          val txnMetadata = existingEpochAndMetadata.transactionMetadata
          txnMetadata.inLock {
            //校验事务元数据，以及该transactionId对应的事务状态
            prepareInitProducerIdTransit(transactionalId, transactionTimeoutMs, coordinatorEpoch, txnMetadata,expectedProducerIdAndEpoch)
          }
      }
      //根据返回结果构建响应
      result match {
        case Left(error) =>
          //异常
          responseCallback(initTransactionError(error))
        case Right((coordinatorEpoch, newMetadata)) =>
          if (newMetadata.txnState == PrepareEpochFence) {
            //元数据中事务状态为PrepareEpochFence
            def sendRetriableErrorCallback(error: Errors): Unit = {
              if (error != Errors.NONE) {
                responseCallback(initTransactionError(error))
              } else {
                responseCallback(initTransactionError(Errors.CONCURRENT_TRANSACTIONS))
              }
            }
            //abort未完成的事务
            endTransaction(transactionalId, newMetadata.producerId, newMetadata.producerEpoch, TransactionResult.ABORT, isFromClient = false, sendRetriableErrorCallback)
          } else {
            def sendPidResponseCallback(error: Errors): Unit = {
              if (error == Errors.NONE) {
                //不存在异常，正常返回
                responseCallback(initTransactionMetadata(newMetadata))
              } else {
                responseCallback(initTransactionError(error))
              }
            }
            //将transactionalId与最新的TransactionMetadata持久化到事务日志中，主要目的是保存<transactionId,pid>的关系信息
            txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, sendPidResponseCallback)
          }
      }
    }
  }
```

prepareInitProducerIdTransit()源码如下：

```
  private def prepareInitProducerIdTransit(transactionalId: String,transactionTimeoutMs: Int,coordinatorEpoch: Int,txnMetadata: TransactionMetadata,expectedProducerIdAndEpoch: Option[ProducerIdAndEpoch]): ApiResult[(Int, TxnTransitMetadata)] = {

    def isValidProducerId(producerIdAndEpoch: ProducerIdAndEpoch): Boolean = {
      txnMetadata.producerEpoch == RecordBatch.NO_PRODUCER_EPOCH ||producerIdAndEpoch.producerId == txnMetadata.producerId ||
        (producerIdAndEpoch.producerId == txnMetadata.lastProducerId && TransactionMetadata.isEpochExhausted(producerIdAndEpoch.epoch))
    }
    //正在进行事务状态转换
    if (txnMetadata.pendingTransitionInProgress) {
      // return a retriable exception to let the client backoff and retry
      Left(Errors.CONCURRENT_TRANSACTIONS)
    }
    else if (!expectedProducerIdAndEpoch.forall(isValidProducerId)) {
      Left(Errors.PRODUCER_FENCED)
    } else {
      txnMetadata.state match {
        //上一个事务正在提交或终止，返回 Errors.CONCURRENT_TRANSACTIONS，让producer后续重试
        case PrepareAbort | PrepareCommit =>
          Left(Errors.CONCURRENT_TRANSACTIONS)
        //上一个事务已完结或不存在事务
        case CompleteAbort | CompleteCommit | Empty =>
          val transitMetadataResult =
            if (txnMetadata.isProducerEpochExhausted && expectedProducerIdAndEpoch.forall(_.epoch == txnMetadata.producerEpoch)) {
              epoch为short类型，耗尽后，重新申请新的PID epoch从0开始递增
              val newProducerId = producerIdGenerator.generateProducerId()
              Right(txnMetadata.prepareProducerIdRotation(newProducerId, transactionTimeoutMs, time.milliseconds(),expectedProducerIdAndEpoch.isDefined))
            } else {
              //否则根据沿用同一个producerId，但epoch自增1
              txnMetadata.prepareIncrementProducerEpoch(transactionTimeoutMs, expectedProducerIdAndEpoch.map(_.epoch),time.milliseconds())
            }
          transitMetadataResult match {
            case Right(transitMetadata) => Right((coordinatorEpoch, transitMetadata))
            case Left(err) => Left(err)
          }
        //上一个事务正在进行，赋值pendingState，后续进行事务abort
        case Ongoing =>
          Right(coordinatorEpoch, txnMetadata.prepareFenceProducerEpoch())
        //异常，无法继续进行事务，抛出异常
        case Dead | PrepareEpochFence =>
          fatal(errorMsg)
          throw new IllegalStateException(errorMsg)
      }
    }
  }
```
Broker端的处理流程可概括为：

* 1、获取transactionalId对应的事务元数据TransactionMetadata，若不存在，完成PID的申请，创建新的TransactionMetadata并放入缓存；**若存在则直接返回，说明配置该transactionId的Producer发生了重启，才会再次申请PID**；
* 2、对TransactionMetadata进行校验，若存在事务状态，需要根据其事务状态TransactionState进行相应的事务恢复操作：
    * `PrepareAbort | PrepareCommit`，Producer崩溃时，上一个事务未完成，TransactionCoordinator正在等待所有参与事务的分区全部返回响应，将Errors.CONCURRENT_TRANSACTIONS异常返回Producer，让其进行重试；
    * `CompleteAbort | CompleteCommit | Empty`，将事务状态置为Empty(CompleteAbort或CompleteCommit时),判断ProducerEpoch是否达到**Short.MAX_VALUE**，若达到，则**需重新申请PID，epoch恢复为0，否则epoch自增**；
    * `Ongoing`，Producer崩溃时，上一个事务正在进行中，事务状态转移为PrepareEpochFence，并中止(Abort)当前事务，返回客户端Errors.CONCURRENT_TRANSACTIONS，让其重试；
    * `Dead | PrepareEpochFence`，返回FATAL_ERROR，Producer无法再进行事务请求。
* 3、将结果<transactionId,PID>持久化到事务日志中。

KafkaProducer收到到响应后，会更新ProducerIdAndEpoch对象，至此，Producer端的事务状态由INITIALIZING转为READY，事务初始化完成。

#### BeginTransaction

KafkaProducer#beginTransaction()方法无需与TransactionCoordinator进行交互，只是将本地事务状态由READY转为INITIALIZING，表明本地开启一个事务。

```
public synchronized void beginTransaction() {
    ensureTransactional();
    maybeFailWithError();
    transitionTo(State.IN_TRANSACTION); READY => IN_TRANSACTION
}
```

#### Send

开启事务后，即可调用KafkaProducer#send进行事务消息的发送，具体实现可分为以下几步：

##### AddPartitionsToTxnRequest

KafkaProducer调用doSend()向一个TopicPartition发送事务消息前，需要先向TransactionCoordinator发送AddPartitionsToTxnRequest请求，通知TransactionCoordinator此分区将会参与本次事务。
doSend()方法中，消息追加入RecordAccumulator后，会调用`maybeAddPartitionToTransaction()`方法添加分区，doSend()的详细分析请见[Kafka Producer 概述](https://guann1ng.github.io/kafka/2021/08/03/Kafka-Producer-%E6%A6%82%E8%BF%B0/)。

```
private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
    TopicPartition tp = null;
    try {
        ...//消息处理流程
        
        //RecordAccumulator消息追加
        RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey,serializedValue, headers, interceptCallback, remainingWaitMs, true, nowMs);
        if (transactionManager != null && transactionManager.isTransactional())
            //事务消息，添加分区
            transactionManager.maybeAddPartitionToTransaction(tp);

        if (result.batchIsFull || result.newBatchCreated) {
            //唤醒Sender
            this.sender.wakeup();
        }
        return result.future;
    } catch (){
    ...//省略异常处理
    }
}

```

maybeAddPartitionToTransaction()方法实现如下，会将新的TopicPartition添加到newPartitionsInTransaction中，后续由Sender线程将newPartitionsInTransaction中
的Tp发送至TransactionCoordinator.

```
public synchronized void maybeAddPartitionToTransaction(TopicPartition topicPartition) {
    if (isPartitionAdded(topicPartition) || isPartitionPendingAdd(topicPartition))
        //已添加过
        return;
    topicPartitionBookkeeper.addPartition(topicPartition);
    //添加入待同步List
    newPartitionsInTransaction.add(topicPartition);
}
```
Sender线程每次调用runOnce()发送事务消息前，都会判断newPartitionsInTransaction中是否有待发送的分区，若有则构建AddPartitionsToTxnRequest优先发送。

```
synchronized TxnRequestHandler nextRequest(boolean hasIncompleteBatches) {
    if (!newPartitionsInTransaction.isEmpty())
        enqueueRequest(addPartitionsToTransactionHandler());
    ... // 其他事务消息    
}

private TxnRequestHandler addPartitionsToTransactionHandler() {
    pendingPartitionsInTransaction.addAll(newPartitionsInTransaction);
    newPartitionsInTransaction.clear();
    AddPartitionsToTxnRequest.Builder builder = new AddPartitionsToTxnRequest.Builder(transactionalId, producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, new ArrayList<>(pendingPartitionsInTransaction));
    return new AddPartitionsToTxnHandler(builder);
}
```

TransactionCoordinator收到请求后，会将TopicPartitionList更新到TransactionId对应的TransactionMetadata中，并将新的metadata持久化到_transaction_state对应分区的事务日志中，具体实现在
`TransactionCoordinator#handleAddPartitionsToTransaction()`方法中。


##### ProduceRequest

ProduceRequest请求的发送和普通消息相同，但在消息内容上，事务消息的ProducerBatch中会包PID、producerEpoch和sequence number参数。Sender线程从RecordAccumulator中拉取消息时完成设置，源码详见
RecordAccumulator#drainBatchesForOneNode。

##### AddOffsetsToTxnRequest & TxnOffsetsCommitRequest

`sendOffsetsToTransaction()`方法**主要用于consume-transform-produce loop模式中**，用于保存当前消费进度，主要分2步：

* 1、AddOffsetsToTxnRequest
       
    `sendOffsetsToTransaction()`方法会向TransactionCoordinator发送AddOffsetsToTxnRequest请求，将groupId所属GroupCoordinator分区更新到事务对应的meta中，
    并持久化到主题_transaction_state中。
   
* 2、TxnOffsetsCommitRequest
    
    Producer在收到TransactionCoordinator关于AddOffsetsToTxnRequest请求的结果后，后再次发送TxnOffsetsCommitRequest请求给对应的GroupCoordinator，从而将本次事务
    中包含的消费位移信息offsets持久化到主题_consumer_offsets中，但**不会更新对应的元数据缓存**，即对当前消息者组不可见，事务commit后才会更新恢复组可见(事务abort后，
    可再次从事务开始时的offset进行消费)。
    

示意图如下：

![sendOffsetsToTransaction](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/sendOffsetsToTransaction.png)

#### Committing or Aborting

上述事务流程处理完成后，Producer需要调用commitTransaction()或者abortTransaction()方法来commit或者abort这个事务操作。

* commitTransaction()

```
//KafkaProducer#commitTransaction
public void commitTransaction() throws ProducerFencedException {
    throwIfNoTransactionManager();
    throwIfProducerClosed();
    TransactionalRequestResult result = transactionManager.beginCommit();
    sender.wakeup();
    result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
}

//TransactionManager#beginCommit
public synchronized TransactionalRequestResult beginCommit() {
    return handleCachedTransactionRequestResult(() -> {
        maybeFailWithError();
        transitionTo(State.COMMITTING_TRANSACTION); //IN_TRANSACTION => COMMITTING_TRANSACTION
        return beginCompletingTransaction(TransactionResult.COMMIT);
    }, State.COMMITTING_TRANSACTION);
}
```

* abortTransaction()

```
//KafkaProducer#abortTransaction
public void abortTransaction() throws ProducerFencedException {
    throwIfNoTransactionManager();
    throwIfProducerClosed();
    TransactionalRequestResult result = transactionManager.beginAbort();
    sender.wakeup();
    result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
}

//TransactionManager#beginAbort
public synchronized TransactionalRequestResult beginAbort() {
    return handleCachedTransactionRequestResult(() -> {
        if (currentState != State.ABORTABLE_ERROR) maybeFailWithError();
        transitionTo(State.ABORTING_TRANSACTION); //IN_TRANSACTION => ABORTING_TRANSACTION
        newPartitionsInTransaction.clear(); //清理待发送的分区
        return beginCompletingTransaction(TransactionResult.ABORT);
    }, State.ABORTING_TRANSACTION);
}
```

上述两个方法完成本地事务状态修改后，均调用了TransactionManager#beginCompletingTransaction()方法进行最后的事务操作。

##### EndTxnRequest

###### SendEndTxnRequest

beginCompletingTransaction()方法的作用是向TransactionCoordinator发送EndTxnRequest请求，以此来通知它提交(Commit)事务还是中止(Abort)事务。实现如下：

```
private TransactionalRequestResult beginCompletingTransaction(TransactionResult transactionResult) {
    if (!newPartitionsInTransaction.isEmpty())
        //最后判断是否需要发送AddPartitionsToTxnRequest
        enqueueRequest(addPartitionsToTransactionHandler());
    if (!(lastError instanceof InvalidPidMappingException)) {
        //发送EndTxnRequest
        EndTxnRequest.Builder builder = new EndTxnRequest.Builder(new EndTxnRequestData().setTransactionalId(transactionalId).setProducerId(producerIdAndEpoch.producerId).setProducerEpoch(producerIdAndEpoch.epoch).setCommitted(transactionResult.id));
        EndTxnHandler handler = new EndTxnHandler(builder);
        //加入待发送队列，由Sender发送
        enqueueRequest(handler);
        if (!epochBumpRequired) {
            return handler.result;
        }
    }
    return initializeTransactions(this.producerIdAndEpoch);
}
```

###### HandleEndTxnRequest

Broker端处理EndTxnRequest的核心方法为TransactionCoordinator#endTransaction():

```
  private def endTransaction(transactionalId: String, producerId: Long, producerEpoch: Short, txnMarkerResult: TransactionResult, isFromClient: Boolean, responseCallback: EndTxnCallback): Unit = {

    ...//参数校验及异常处理

    val preAppendResult: ApiResult[(Int, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).right.flatMap {
        txnMetadata.state match {
          //正常状态下，发送EndTxnRequest前，TransactionCoordinator的事务状态为Ongoing
          case Ongoing =>
            //根据传入的TransactionResult决定下一个状态为PrepareCommit 或 PrepareAbort
            val nextState = if (txnMarkerResult == TransactionResult.COMMIT)
              PrepareCommit
            else
              PrepareAbort
            if (nextState == PrepareAbort && txnMetadata.pendingState.contains(PrepareEpochFence)) {
              //
              isEpochFence = true
              txnMetadata.pendingState = None
              txnMetadata.producerEpoch = producerEpoch
              txnMetadata.lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH
            }
            //更新metadata
            Right(coordinatorEpoch, txnMetadata.prepareAbortOrCommit(nextState, time.milliseconds()))

          ... // case mmore   
        }
     }    

      preAppendResult match {
        case Left(err) => ... //异常返回
        case Right((coordinatorEpoch, newMetadata)) =>
          //定义事务成功写入日志后的回调的方法 该回调方法用于发送事务Marker
          def sendTxnMarkersCallback(error: Errors): Unit = {
            if (error == Errors.NONE) {
              val preSendResult: ApiResult[(TransactionMetadata, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).flatMap {
                ...
                //修改事务状态
                case Some(epochAndMetadata) =>
                  if (epochAndMetadata.coordinatorEpoch == coordinatorEpoch) {
                      ...// 异常处理
                        case PrepareCommit =>
                          if (txnMarkerResult != TransactionResult.COMMIT)
                            logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                          else
                            //prepareComplete事务状态修改 CompleteCommit
                            Right(txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                        case PrepareAbort =>
                          if (txnMarkerResult != TransactionResult.ABORT)
                            logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                          else
                            //prepareComplete事务状态修改  CompleteAbort
                            Right(txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                        ...// other code
              }

              preSendResult match {
                case Left(err) => responseCallback(err)
                //响应Producer
                case Right((txnMetadata, newPreSendMetadata)) => responseCallback(Errors.NONE)
                //发送
                txnMarkerChannelManager.addTxnMarkersToSend(coordinatorEpoch, txnMarkerResult, txnMetadata, newPreSendMetadata)
              }
            } else {
             ...//异常处理
            }
          }
          //记录事务日志
          txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, sendTxnMarkersCallback)
      }
    }
  }
```

上面省略了较多源码，只保留了关键步骤，可概括为以下三步：

* 1、更新事务meta信息，将事务状态由Ongoing转为PREPARE_COMMIT或PREPARE_ABORT；
* 2、将更新后的metadata持久化到_transaction_state日志中，**分区的所有副本均完成写入后，执行回调函数sendTxnMarkersCallback()**;
* 3、sendTxnMarkersCallback方法的内容如下：
    * 将事务状态更新为CompleteCommit或CompleteAbort；
    * 响应结果给Producer;
    * 进入发送WriteTxnMarkerRequest流程。
    
    
###### HandleEndTxnResponse

收到EndTxnResponse后，由TransactionManager.EndTxnHandler#handleResponse()进行处理，如果没有任何错误则设置本地状态及清空相关的缓存队列用于下一次事务提交准备，
如果有错误则根据不同的错误类型有不同的处理方法

```
public void handleResponse(AbstractResponse response) {
    EndTxnResponse endTxnResponse = (EndTxnResponse) response;
    Errors error = endTxnResponse.error();
    if (error == Errors.NONE) {
        //事务结束
        completeTransaction();
        result.done();
    } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
        lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
        reenqueue();
    } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.CONCURRENT_TRANSACTIONS) {
        reenqueue();
    } else if (error == Errors.INVALID_PRODUCER_EPOCH || error == Errors.PRODUCER_FENCED) {
        fatalError(Errors.PRODUCER_FENCED.exception());
    } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
        fatalError(error.exception());
    } else if (error == Errors.INVALID_TXN_STATE) {
        fatalError(error.exception());
    } else if (error == Errors.UNKNOWN_PRODUCER_ID || error == Errors.INVALID_PRODUCER_ID_MAPPING) {
        abortableErrorIfPossible(error.exception());
    } else {
        fatalError(new KafkaException("Unhandled error in EndTxnResponse: " + error.message()));
    }
}

private void completeTransaction() {
    if (epochBumpRequired) {
        transitionTo(State.INITIALIZING);
    } else {
        transitionTo(State.READY); //状态置为Ready
    }
    //清理事务缓存
    lastError = null;
    epochBumpRequired = false;
    transactionStarted = false;
    newPartitionsInTransaction.clear();
    pendingPartitionsInTransaction.clear();
    partitionsInTransaction.clear();
}
```    
    
##### WriteTxnMarkerRequest

EndTxnRequest请求处理的最后一步是开始WriteTxnMarkerRequest的发送，**WriteTxnMarkerRequest是TransactionCoordinator收到Producer的EndTxnRequest请求后
向其它Broker(参与事务分区的leader副本所在Broker)发送的请求，作用是通知参与事务的主题分区最终的事务结果，commit或abort**。

###### SendWriteTxnMarkerRequest

入口方法为TransactionMarkerChannelManager#addTxnMarkersToSend()，实现如下：

```
  def addTxnMarkersToSend(coordinatorEpoch: Int, txnResult: TransactionResult, txnMetadata: TransactionMetadata, newMetadata: TxnTransitMetadata): Unit = {
    val transactionalId = txnMetadata.transactionalId
    val pendingCompleteTxn = PendingCompleteTxn( transactionalId, coordinatorEpoch, txnMetadata,newMetadata)
    //缓存等待WriteTxnMarkerRequest响应的事务id
    transactionsWithPendingMarkers.put(transactionalId, pendingCompleteTxn)
    //添加通知参与事务分区事务结果的任务
    addTxnMarkersToBrokerQueue(transactionalId, txnMetadata.producerId, txnMetadata.producerEpoch, 
                    txnResult,  //事务结果 
                    coordinatorEpoch, 
                    txnMetadata.topicPartitions.toSet)  //待通知的分区

    //通知完毕后执行
    maybeWriteTxnCompletion(transactionalId)
  }
```

addTxnMarkersToBrokerQueue实现如下：

```
  def addTxnMarkersToBrokerQueue(transactionalId: String,producerId: Long,producerEpoch: Short,result: TransactionResult,coordinatorEpoch: Int,topicPartitions: immutable.Set[TopicPartition]): Unit = {
    val txnTopicPartition = txnStateManager.partitionFor(transactionalId)
    //遍历获取分区leader副本所在节点
    val partitionsByDestination: immutable.Map[Option[Node], immutable.Set[TopicPartition]] = topicPartitions.groupBy { topicPartition: TopicPartition =>
      metadataCache.getPartitionLeaderEndpoint(topicPartition.topic, topicPartition.partition, interBrokerListenerName)
    }

    for ((broker: Option[Node], topicPartitions: immutable.Set[TopicPartition]) <- partitionsByDestination) {
      broker match {
        case Some(brokerNode) =>
          //构建marker
          val marker = new TxnMarkerEntry(producerId, producerEpoch, coordinatorEpoch, result, topicPartitions.toList.asJava)
          val txnIdAndMarker = TxnIdAndMarkerEntry(transactionalId, marker)

          if (brokerNode == Node.noNode) {
            //broker未知的topicpartition的marker,sender线程会查找对应的broker
            markersQueueForUnknownBroker.addMarkers(txnTopicPartition, txnIdAndMarker)
          } else {
            //添加入目标broker对应的brokerRequestQueue中
            addMarkersForBroker(brokerNode, txnTopicPartition, txnIdAndMarker)
          }
        case None =>
          ... //异常情况处理
      }
    }
    //唤醒发送线程 InterBrokerSendThread
    wakeup()
  }
```

后续请求的发送与Producer请求的发送类似，Broker端也有一个单独的线程负责Broker间的请求发送，该线程类为**InterBrokerSendThread**，WriteTxnMarkerRequest封装的调用链为：

```
InterBrokerSendThread#pollOnce() =>  InterBrokerSendThread#drainGeneratedRequests() => TransactionMarkerChannelManager#generateRequests()
```

网络IO的部分与KafkaProducer完全一致，均是使用基于Java NIO封装的NetworkClient发送，这里不再展开。

###### handleWriteTxnMarkerRequest

事务分区的leader副本所在Broker收到WriteTxnMarkerRequest请求后，由KafkaApis#handleWriteTxnMarkersRequest()进行处理，主要是遍历请求内的marker，封装为ControlRecord写入对应的分区消息日志中，后续Consumer消费时，
可根据ControlRecord判断事务是被提交或被中止。Broker完成所有marker的持久化后，返回响应给TransactionCoordinator。源码如下：

```
 def handleWriteTxnMarkersRequest(request: RequestChannel.Request): Unit = {
    ensureInterBrokerVersion(KAFKA_0_11_0_IV0)
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val writeTxnMarkersRequest = request.body[WriteTxnMarkersRequest]
    val errors = new ConcurrentHashMap[java.lang.Long, util.Map[TopicPartition, Errors]]()
    val markers = writeTxnMarkersRequest.markers
    //marker数量  任务合并，且可能有多个分区的leader副本在这个broker上
    val numAppends = new AtomicInteger(markers.size)

    ...//参数校验

    //controlRecord写入回调 会被调用多次，原因同上，任务合并，可能多个分区的leader副本在这个broker上
    def maybeSendResponseCallback(producerId: Long, result: TransactionResult)(responseStatus: Map[TopicPartition, PartitionResponse]): Unit = {
        if (successfulOffsetsPartitions.nonEmpty) {
           ...
            if (successfulOffsetsPartitions.nonEmpty) {
            // as soon as the end transaction marker has been written for a transactional offset commit,
            // call to the group coordinator to materialize the offsets into the cache
            try {
                //更新offset缓存，确认消息已被消费
                groupCoordinator.scheduleHandleTxnCompletion(producerId, successfulOffsetsPartitions, result)
            } catch {
                case e: Exception =>
            }
        }
      //所有分区已全部完成controlRecords写入，返回响应给TransactionCoordinator
      if (numAppends.decrementAndGet() == 0)
        requestHelper.sendResponseExemptThrottle(request, new WriteTxnMarkersResponse(errors))
    }

    var skippedMarkers = 0
    for (marker <- markers.asScala) {
      //遍历所有的marker
      val producerId = marker.producerId
      val partitionsWithCompatibleMessageFormat = new mutable.ArrayBuffer[TopicPartition]
        //构建controlRecord
        val controlRecords = partitionsWithCompatibleMessageFormat.map { partition =>
          val controlRecordType = marker.transactionResult match {
            case TransactionResult.COMMIT => ControlRecordType.COMMIT
            case TransactionResult.ABORT => ControlRecordType.ABORT
          }
          val endTxnMarker = new EndTransactionMarker(controlRecordType, marker.coordinatorEpoch)
          partition -> MemoryRecords.withEndTransactionMarker(producerId, marker.producerEpoch, endTxnMarker)
        }.toMap
        //将controlRecord写入消息日志中 acks=-1
        replicaManager.appendRecords(
          timeout = config.requestTimeoutMs.toLong,
          requiredAcks = -1,
          internalTopicsAllowed = true,
          origin = AppendOrigin.Coordinator,
          entriesPerPartition = controlRecords,
          responseCallback = maybeSendResponseCallback(producerId, marker.transactionResult))
        }
    }
    ...
  }
```

###### handleWriteTxnMarkersResponse

构建WriteTxnMarkerRequest时，也完成了TransactionMarkerRequestCompletionHandler的构建，负责处理WriteTxnMarkersResponse，onComplete()实现如下：

```
  override def onComplete(response: ClientResponse): Unit = {
    val requestHeader = response.requestHeader
    val correlationId = requestHeader.correlationId
    if (response.wasDisconnected) {
      ... //网络连接失败  异常处理及判断是否进行重试
    } else {
      debug(s"Received WriteTxnMarker response $response from node ${response.destination} with correlation id $correlationId")

      val writeTxnMarkerResponse = response.responseBody.asInstanceOf[WriteTxnMarkersResponse]

      val responseErrors = writeTxnMarkerResponse.errorsByProducerId;
      for (txnIdAndMarker <- txnIdAndMarkerEntries.asScala) {
        val transactionalId = txnIdAndMarker.txnId
        val txnMarker = txnIdAndMarker.txnMarkerEntry
        val errors = responseErrors.get(txnMarker.producerId)
        if (errors == null)
          throw new IllegalStateException(s"WriteTxnMarkerResponse does not contain expected error map for producer id ${txnMarker.producerId}")
        
        txnStateManager.getTransactionState(transactionalId) match {
          ...//异常处理
          case Right(Some(epochAndMetadata)) =>
            val txnMetadata = epochAndMetadata.transactionMetadata
            val retryPartitions: mutable.Set[TopicPartition] = mutable.Set.empty[TopicPartition]
            var abortSending: Boolean = false

            if (epochAndMetadata.coordinatorEpoch != txnMarker.coordinatorEpoch) {
              // coordinator epoch has changed, just cancel it from the purgatory
              //epoch 过期 停止发送
              abortSending = true
            } else {
              txnMetadata.inLock {
                for ((topicPartition, error) <- errors.asScala) {
                  error match {
                    case Errors.NONE =>
                      //将topicpartiton从txnMetadata缓存中移除  
                      txnMetadata.removePartition(topicPartition)
                    case Errors.UNKNOWN_TOPIC_OR_PARTITION | Errors.NOT_LEADER_OR_FOLLOWER | Errors.NOT_ENOUGH_REPLICAS | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND | Errors.REQUEST_TIMED_OUT | Errors.KAFKA_STORAGE_ERROR =>
                      //可重试异常 
                      retryPartitions += topicPartition

                    ... // other condition
                    
                  }
                }
              }
            }
            if (!abortSending) {
              if (retryPartitions.nonEmpty) {
                // 进行重试
                txnMarkerChannelManager.addTxnMarkersToBrokerQueue(transactionalId,txnMarker.producerId,txnMarker.producerEpoch,txnMarker.transactionResult,txnMarker.coordinatorEpoch,retryPartitions.toSet)
              } else {
                //判断transactionalId对应的WriteTxnMarkerRequest是否已结束
                txnMarkerChannelManager.maybeWriteTxnCompletion(transactionalId)
              }
            }
        }
      }
    }
  }
```



##### Writing the final Commit or Abort Message


TransactionCoordinator收到所有Broker返回的WriteTxnMarkersResponse且不存在异常后，TransactionCoordinator将最终的COMPLETE_COMMIT或COMPLETE_ABORT信息持久化到事务日志中，表明当前事务已全部结束，
此时TransactionCoordinator缓存的很多关于这个事务的数据可以被清除,且主题_transaction_state中所有关于该事务的消息也可以被设置为墓碑消息，等到日志压缩处理。

方法调用链为：

```
TransactionMarkerChannelManager#maybeWriteTxnCompletion() => TransactionMarkerChannelManager#writeTxnCompletion() => TransactionMarkerChannelManager#tryAppendToLog => TransactionStateManager#appendTransactionToLog()
```


#### 执行流程图

事务执行的流程图如下：

![Kafka Transaction](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/kafka%20transaction.png)

官网详解可见[Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging#KIP98ExactlyOnceDeliveryandTransactionalMessaging-DataFlow)


### 事务成功保证

收到EndTxnRequest后，TransactionCoordinator根据请求内容完成事务状态的转化(PreCommit或PreAbort)及持久到事务日志中后，便将结果响应给了KafkaProducer，此时Producer客户端的事务已全部结束。
即只要事务的状态转换为PreCommit或PreAbort并完成持久化，那么这个事务就是最终成功的。TransactionCoordinator是如何保证之后向其它Broker发送的WriteTxnMarkerRequest请求一定会被所有的Broker正确处理的呢，
主要有两点：

1. TransactionCoordinator对事务日志进行持久化的acks=all，确保事务信息不丢失，即使TransactionCoordinator发生故障也可恢复；
2. WriteTxnMarkerRequest会重试至TransactionCoordinator收到正确响应，即WriteTxnMarkerRequest的传输保障是at least once，Kafka保证写入重复的ControlRecord也是无所谓的。

### Kafka Fencing机制

在分布式系统中，一个instance的宕机或失联，集群往往会自动启动或选举一个新的实例来代替它的工作。此时若原实例恢复了，那么集群中就产生了两个具有相同职责的实例，最终使得整个集群处于混乱状态,这就是脑裂问题，
前一个instance就被称为“僵尸实例（Zombie Instance）”。

在Kafka的事务场景下，使用Fencing机制来解决上述问题，分别是：

* TransactionCoordinator Fencing
* Producer Fencing

#### TransactionCoordinator Fencing

当TransactionCoordinator所在Broke实例发生LongTime FullGC时，由于STW，可能会与ZK连接超时导致临时节点消失进而触发leader选举，如果_transaction_state发生了leader选举，
TransactionCoordinator就会切换，如果此时旧的TransactionCoordinator FGC完成，在还没来得及同步到最细meta之前，会有一个短暂的时刻，对于一个transactionId而言就是这个时刻可能出现了两个TransactionCoordinator。

Kafka通过TransactionCoordinator对应的Epoch来判断那个实例是有效的。此处的Epoch就是对应_transaction_state Partition的Epoch值（每当leader切换一次，该值就会自增1）。

#### Producer Fencing

对于相同PID和transactionId的Producer，Broker端会记录最新的Epoch值，拒绝来自zombie Producer（Epoch值小的Producer）的请求。比如当同配置的Producer2在启动时，会向TransactionCoordinator发送InitPIDRequest请求，
此时TransactionCoordinator已经有了这个transactionId对应的meta，会返回之前分配的PID，并把对应Epoch自增1返回，这样Producer2就被认为是最新的Producer，而Producer1就会被认为是zombie Producer，
因此，TransactionCoordinator在处理Producer1的事务请求时，会返回相应的异常信息。