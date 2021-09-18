---
title:  Kafka Producer幂等与事务
date:   2021-08-31 21:50:52
categories: Kafka
---

一般而言，消息中间件的消息传输保障有3个层级：

* at most once：至多一次，消息可能会丢失，但绝对不会重复传输。
* at least once：最少一次，消息绝不会丢失，但可能重复传输。
* exactly once：正好一次，消息不会丢失，也不会被重复发送。

Kafka多副本机制确保消息一旦提交成功写入日志文件，这条消息就不会丢失。当遇到网络问题而导致请求超时，生产者无法判断是否提交成功，此时生产者可以通过多次重试来确保消息
成功提交，但重试过程可能造成消息的重复写入，此时，Kafka提供的消息传输保障为at least once。

**Kafka通过引入幂等和事务特性实现exactly once**。

## 幂等

Kafka引入**幂等性来解决消息重复问题**，开启幂等特性后，当发送同一条消息时，数据在Broker端只会被持久化一次，避免生产者重试导致的消息重复写入。此处的幂等是有条件的：

* 只能保证Producer在单个会话内的幂等
* 只能保证单个TopicPartition内的幂等

通过将参数**enable.idempotence设置为true即可开启幂等**功能，此时retries参数默认为Integer.MAX_VALUE，acks默认为all，并确保max.in.flight.requests.per.connection不能大于5。

### 实现原理

Kafka引入了两个机制实现幂等：

* PID(Producer ID)，用来标识生产者，全局唯一。初始化时被分配，对用户透明，重启后会重新分配到新的PID。
* sequence number，标识消息，每一个PID，每个TopicPartition都有对应的sequence number，从0单调递增。


### 幂等消息发送流程

#### 1、获取PID InitProducerIdRequest

Sender线程在发送消息前，会判断是否开启幂等，以及是否完成PID参数的获取，如没有，则发送InitProducerIdRequest请求，完成PID的初始化。入口方法为bumpIdempotentEpochAndResetIdIfNeeded()。

```
Sender#runOnce

void runOnce() {
    //省略...
    if (transactionManager != null) {
        // Check whether we need a new producerId. If so, we will enqueue an InitProducerIdrequest which will be sent below
        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded();
    }
    //...      
}

ransactionManager#bumpIdempotentEpochAndResetIdIfNeeded

synchronized void bumpIdempotentEpochAndResetIdIfNeeded() {
    if (!isTransactional()) {
        if (epochBumpRequired) {
            bumpIdempotentProducerEpoch();
        }
        if (currentState != State.INITIALIZING && !hasProducerId()) {
            transitionTo(State.INITIALIZING);
            InitProducerIdRequestData requestData = new InitProducerIdRequestData()
                    .setTransactionalId(null)
                    .setTransactionTimeoutMs(Integer.MAX_VALUE);
            InitProducerIdHandler handler = new InitProducerIdHandler(new InitProducerIdRequest.Builder(requestData), false);
            enqueueRequest(handler);
        }
    }
}
```

#### 2、Broker端处理 InitProducerIdRequest

InitProducerIdRequest会被**KafakApis#handleInitProducerIdRequest**方法接受处理，方法内调用**TransactionCoordinator#handleInitProducerId()**，
最终是通过**ProducerIdManage#generateProducerId**方法产生一个PID，如下：

```
def generateProducerId(): Long = {
    this synchronized {
      // grab a new block of producerIds if this block has been exhausted
      if (nextProducerId > currentProducerIdBlock.producerIdEnd) {
        //申请新的PID段
        allocateNewProducerIdBlock()
        nextProducerId = currentProducerIdBlock.producerIdStart
      }
      nextProducerId += 1
      nextProducerId - 1
    }
  }
}
```

ProducerIdManager是在TransactionCoordinator对象初始化时初始化的，这个对象主要是用来管理PID信息，有两个作用：

* 在本地的PID段用完了或者处于新建状态时，申请PID段（默认情况下，每次申请1000个PID）；
* TransactionCoordinator对象通过generateProducerId()方法获取下一个可以使用的PID；

PID段的申请是通过Zookeeper实现。在ZK中维护/latest_producer_id_block节点，每个Broker向zk申请一个PID段后，都会把自己申请的PID段信息写入到这个节点，这样当其他Broker再申请PID段时，
会首先读写这个节点的信息，然后根据block_end选择一个PID段，最后再把信息写会到zk的这个节点，这个节点信息格式如下所示：

```
{"version":1,"broker":35,"block_start":"4000","block_end":"4999"}
```

方法如下：

```
private def allocateNewProducerIdBlock(): Unit = {
    this synchronized {
      currentProducerIdBlock = ZkProducerIdManager.getNewProducerIdBlock(brokerId, zkClient, this)
    }
}

ZkProducerIdManager#getNewProducerIdBlock
def getNewProducerIdBlock(brokerId: Int, zkClient: KafkaZkClient, logger: Logging): ProducerIdsBlock = {
    var zkWriteComplete = false
    while (!zkWriteComplete) {
      // refresh current producerId block from zookeeper again
      val (dataOpt, zkVersion) = zkClient.getDataAndVersion(ProducerIdBlockZNode.path)

      // generate the new producerId block
      val newProducerIdBlock = dataOpt match {
        case Some(data) =>
          val currProducerIdBlock = ProducerIdBlockZNode.parseProducerIdBlockData(data)
          logger.debug(s"Read current producerId block $currProducerIdBlock, Zk path version $zkVersion")
           //PID已耗尽，Long类型
          if (currProducerIdBlock.producerIdEnd > Long.MaxValue - ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE) {
            // we have exhausted all producerIds (wow!), treat it as a fatal error
            logger.fatal(s"Exhausted all producerIds as the next block's end producerId is will has exceeded long type limit (current block end producerId is ${currProducerIdBlock.producerIdEnd})")
            throw new KafkaException("Have exhausted all producerIds.")
          }
        
          new ProducerIdsBlock(brokerId, currProducerIdBlock.producerIdEnd + 1L, ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE)
        case None =>
         //节点不存在，第一次获取PID段 
         logger.debug(s"There is no producerId block yet (Zk path version $zkVersion), creating the first block")
          new ProducerIdsBlock(brokerId, 0L, ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE)
      }

      val newProducerIdBlockData = ProducerIdBlockZNode.generateProducerIdBlockJson(newProducerIdBlock)

      //将新的PID信息写回ZK，version 乐观锁
      val (succeeded, version) = zkClient.conditionalUpdatePath(ProducerIdBlockZNode.path, newProducerIdBlockData, zkVersion, None)
      zkWriteComplete = succeeded

      if (zkWriteComplete) {
        logger.info(s"Acquired new producerId block $newProducerIdBlock by writing to Zk with path version $version")
        return newProducerIdBlock
      }
    }
    throw new IllegalStateException()
}

```

#### 3、幂等消息发送

成功获取的PID信息，由KafkaProducer侧的TransactionManager维护，该类的作用主要有以下几个部分：

* 记录本地事务状态(开始事务时)
* 记录幂等状态信息：每个TopicPartition对应的下一个sequence number和last acked batch(最近一个已经确认的batch)的最大的sequence number等；
* 记录 ProducerIdAndEpoch信息（PID 信息）。


Sender线程从RecordAccumulator中拉取消息时，完成PID及sequence number的设置。

```
private List<ProducerBatch> drainBatchesForOneNode(Cluster cluster, Node node, int maxSize, long now) {
        
    //省略...
    //判断当前TopicPartition是否可以进行发送，如PID是否有效，epoch是否一致，分区sequence number是否正确 
    if (shouldStopDrainBatchesForPartition(first, tp))
            break;

        boolean isTransactional = transactionManager != null && transactionManager.isTransactional();
        ProducerIdAndEpoch producerIdAndEpoch =
            transactionManager != null ? transactionManager.producerIdAndEpoch() : null;
        org.apache.kafka.clients.producer.internals.ProducerBatch batch = deque.pollFirst();
        if (producerIdAndEpoch != null && !batch.hasSequence()) {
            // 
            transactionManager.maybeUpdateProducerIdAndEpoch(batch.topicPartition);

            // 给这个 batch 设置相应的 pid、seq等信息
            batch.setProducerState(producerIdAndEpoch, transactionManager.sequenceNumber(batch.topicPartition), isTransactional);
            // seq自增
            transactionManager.incrementSequenceNumber(batch.topicPartition, batch.recordCount);
          
            transactionManager.addInFlightBatch(batch);
        }
    //省略...
}

```

sequence number大于Integer.MAX_VALUE后，再次从0开始递增。

```
public static int incrementSequence(int sequence, int increment) {
    if (sequence > Integer.MAX_VALUE - increment)
        return increment - (Integer.MAX_VALUE - sequence) - 1;
    return sequence + increment;
}
```

#### 4、Broker处理

Broker内会为每一对<PID,TopicPartition>记录一个sequence number，当一个RecordBatch到来时，会先检查PID是否已过期，然后再检查序列号：

* RecordBatch中的firstSeq = Broker中维护的序列号 + 1，保存数据
* 否则，抛出异常OutOfOrderSequenceException

```
private def maybeValidateDataBatch(producerEpoch: Short, firstSeq: Int, offset: Long): Unit = {
    checkProducerEpoch(producerEpoch, offset)
    if (origin == AppendOrigin.Client) {
      checkSequence(producerEpoch, firstSeq, offset)
    }
}


private def checkSequence(producerEpoch: Short, appendFirstSeq: Int, offset: Long): Unit = {
    if (producerEpoch != updatedEntry.producerEpoch) {
      if (appendFirstSeq != 0) {
        if (updatedEntry.producerEpoch != RecordBatch.NO_PRODUCER_EPOCH) {
          throw new OutOfOrderSequenceException(s"Invalid sequence number for new epoch of producer $producerId " +
            s"at offset $offset in partition $topicPartition: $producerEpoch (request epoch), $appendFirstSeq (seq. number), " +
            s"${updatedEntry.producerEpoch} (current producer epoch)")
        }
      }
    } else {
      val currentLastSeq = if (!updatedEntry.isEmpty)
        updatedEntry.lastSeq
      else if (producerEpoch == currentEntry.producerEpoch)
        currentEntry.lastSeq
      else
        RecordBatch.NO_SEQUENCE

      // If there is no current producer epoch (possibly because all producer records have been deleted due to
      // retention or the DeleteRecords API) accept writes with any sequence number
      if (!(currentEntry.producerEpoch == RecordBatch.NO_PRODUCER_EPOCH || inSequence(currentLastSeq, appendFirstSeq))) {
        throw new OutOfOrderSequenceException(s"Out of order sequence number for producer $producerId at " +
          s"offset $offset in partition $topicPartition: $appendFirstSeq (incoming seq. number), " +
          s"$currentLastSeq (current end sequence number)")
      }
    }
}

//序列号比较
private def inSequence(lastSeq: Int, nextSeq: Int): Boolean = {
    nextSeq == lastSeq + 1L || (nextSeq == 0 && lastSeq == Int.MaxValue)
}

```

### 幂等消息有序性

我们可通过将max.in.flight.requests.per.connection设置为1来保证有序性，因为只允许一个请求正在发送，但这会导致性能的下降，Kafka2.0.0后的版本如果开启幂等后，
能够动态调整max.in.flight.requests.per.connection的值，并根据序列号将Batch放到合适的位置：

```
private void insertInSequenceOrder(Deque<ProducerBatch> deque, ProducerBatch batch) {
    //不存在序列号 抛出异常
    if (batch.baseSequence() == RecordBatch.NO_SEQUENCE)
        throw new IllegalStateException("Trying to re-enqueue a batch which doesn't have a sequence even " +
            "though idempotency is enabled.");

    if (transactionManager.nextBatchBySequence(batch.topicPartition) == null)
        throw new IllegalStateException("We are re-enqueueing a batch which is not tracked as part of the in flight " +
            "requests. batch.topicPartition: " + batch.topicPartition + "; batch.baseSequence: " + batch.baseSequence());
      
    ProducerBatch firstBatchInQueue = deque.peekFirst();
    //通过序列号比较
    if (firstBatchInQueue != null && firstBatchInQueue.hasSequence() && firstBatchInQueue.baseSequence() < batch.baseSequence()) {
        
        List<ProducerBatch> orderedBatches = new ArrayList<>();
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

Kafka通过引入幂等实现了单会话单TopicPartition的Exactly-Once语义，但因为PID机制无法提供跨多个TopicPartition和跨会话场景下的Exactly-Once保证,Kafka引入事务来弥补这个缺陷，
**事务可以保证对多个分区写入操作的原子性**。