---
title: Kafka RecordAccumulator
date:   2021-08-09 Kafka Accumulator
categories: Kafka
---

KafkaProducer#doSend方法中将消息通过生产者拦截器、序列化器、分区器处理后，调用了RecordAccumulator#append方法，并根据append结果判断是否唤醒Sender线程，至此KafkaProducer#doSend方法的职责已全部结束。
下面将通过RecordAccumulator#append的实现来了解RecordAccumulator。

## append

append()方法实现如下：

```
public RecordAppendResult append(TopicPartition tp,long timestamp,byte[] key,byte[] value,Header[] headers,Callback callback,long maxTimeToBlock,boolean abortOnNewBatch,long nowMs) throws InterruptedException {
    appendsInProgress.incrementAndGet();
    ByteBuffer buffer = null;
    if (headers == null) headers = Record.EMPTY_HEADERS;
    try {
        //根据主题分区获取对应的缓存队列
        Deque<ProducerBatch> dq = getOrCreateDeque(tp);
        //加锁 保证线程安全
        synchronized (dq) {
            if (closed) throw new KafkaException("Producer closed while send in progress");
            //追加入队列
            RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq, nowMs);
            if (appendResult != null)
                return appendResult;
        }
        //追加失败，若abortOnNewBatch为true，返回，创建新ProducerBatch前，调用分区器的onNewBatch方法
        if (abortOnNewBatch) {
            return new RecordAppendResult(null, false, false, true);
        }
        //申请内存创建新的ProducerBatch
        byte maxUsableMagic = apiVersions.maxUsableProduceMagic();
        //① 预估大小 batch.size默认是16KB，通过Math.max函数取batch.size和消息大小二者最大值
        int size = Math.max(this.batchSize, AbstractRecords.estimateSizeInBytesUpperBound(maxUsableMagic, compression, key, value, headers));
        //② 申请buffer缓冲区
        buffer = free.allocate(size, maxTimeToBlock);

        nowMs = time.milliseconds();
        synchronized (dq) {
            if (closed) throw new KafkaException("Producer closed while send in progress");
            // double check  判断其他线程是否完成ProducerBatch创建
            RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq, nowMs);
            if (appendResult != null) {
                return appendResult;
            }
            //创建新的ProducerBatch
            MemoryRecordsBuilder recordsBuilder = recordsBuilder(buffer, maxUsableMagic);
            ProducerBatch batch = new ProducerBatch(tp, recordsBuilder, nowMs);
            FutureRecordMetadata future = Objects.requireNonNull(batch.tryAppend(timestamp, key, value, headers,callback, nowMs));
            //添加入队列
            dq.addLast(batch);
            incomplete.add(batch);

            buffer = null;
            return new RecordAppendResult(future, dq.size() > 1 || batch.isFull(), true, false);
        }
    } finally {
        if (buffer != null)
            //释放未使用的buffer
            free.deallocate(buffer);
        appendsInProgress.decrementAndGet();
    }
}
```

可以看出，RecordAccumulator#append方法只是将消息数据添加到缓存队列中，可分为以下几步：

* 1、调用`getOrCreateDeque(TopicPartition tp)`获取缓存队列。数据结构为双端队列ArrayDeque<ProducerBatch>，队列内元素为ProducerBatch；
* 2、对缓存队列**加锁**，调用`tryAppend()`将消息数据写入ArrayDeque<ProducerBatch>中；
* 3、追加失败，若需要重新计算消息分区，返回，否则申请内存`free.allocate()`，申请的内存大小取配置值batch.size(默认16KB)和消息大小中的较大值，创建新的ProducerBatch，完成消息写入并将新的ProducerBatch添加入队列；
* 4、若上一步申请的内存未使用，释放未使用内存。

而这些步骤都是围绕`ArrayDeque<ProducerBatch>`和`free`这两个数据进行缓存操作，下面开始介绍RecordAccumulator实现缓存的数据结构。


## 数据结构

RecordAccumulator中涉及消息缓存的数据结构主要有2个：

* `CopyOnWriteMap<TopicPartition, Deque<ProducerBatch>> batches`：分区信息与待发送消息队列的映射关系

* `BufferPool free`：通过池化思想管理RecordAccumulator缓冲区，可用内存大小由参数buffer.memory设置，默认32MB。

二者关系如下，ProducerBatch内的消息是通过从BufferPool申请到的ByteBuffer存储的 ：

![RecordAccumulator](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/recordAccumulator.png)

### batches

写入RecordAccumulator的消息通过CopyOnWriteMap进行管理，key为消息分区TopicPartition,value为ArrayDeque的双端队列，队列内的元素为ProducerBatch对象，
ProducerBatch表示一个消息发送批次，用于存储一条或多条消息。

#### CopyOnWriteMap

batches维护的是分区信息与消息队列的映射关系，发生写的场景仅存在于当batches中不存在这个分区对应的deque，需要创建这个分区对应的deque，而每次进行append操作同会调用getOrCreateDeque进行读取，
基于**batches是"读多写少"的数据结构**，Kafka使用自定义的CopyOnWriteMap实现(只给出部分源码)：

```
public class CopyOnWriteMap<K, V> implements ConcurrentMap<K, V> {
    private volatile Map<K, V> map; // volatile 保证可见
    
    public CopyOnWriteMap() {
        //java.util.Collections.EmptyMap   不支持写操作
        this.map = Collections.emptyMap();
    }
    
    @Override
    public V get(Object k) { return map.get(k); }
    
    //同步方法
    public synchronized V put(K k, V v) {
        Map<K, V> copy = new HashMap<K, V>(this.map);
        V prev = copy.put(k, v);  //操作复制对象
        //返回不可修改视图
        this.map = Collections.unmodifiableMap(copy);
        return prev;
    }

    public synchronized V putIfAbsent(K k, V v) {
        if (!containsKey(k))
            return put(k, v);
        else
            return get(k);
    }
}
```

CopyOnWriteMap采用**写时复制(Copy-On-Write)及volatile关键字**，来保证batches对象的线程安全及并发性能。但存在延迟更新的问题，getOrCreateDeque()实现如下：

```
private Deque<ProducerBatch> getOrCreateDeque(TopicPartition tp) {
    Deque<ProducerBatch> d = this.batches.get(tp);
    if (d != null)
        return d;
    d = new ArrayDeque<>();
    //put时再判断一次
    Deque<ProducerBatch> previous = this.batches.putIfAbsent(tp, d);
    if (previous == null)
        return d;
    else
        return previous;
}

```

#### ProducerBatch

ProducerBatch一个消息发送批次，内部有一条或多条消息，具体取决于batch.size和消息大小。

```
public final class ProducerBatch {
    private enum FinalState { ABORTED, FAILED, SUCCEEDED }
    final TopicPartition topicPartition; //分区信息
    private final MemoryRecordsBuilder recordsBuilder; //内部维护消息存储的ByteBuffer
    int recordCount; // 本批次内的消息数量
    int maxRecordSize; // 本批次内最大的消息字节数
}
```

### BufferPool

ProducerBatch底层是通过ByteBuffer实现消息的存储的，消息发送成功后需要释放对应的ByteBuffer，频繁的创建和释放ByteBuffer是比较耗费性能的，Kafka引入BufferPool实现ByteBuffer的复用。

```
public class BufferPool {
    private final long totalMemory;  // buffer.buffer.memory配置，缓存空间大小
    private final int poolableSize;  // batch.size   缓存空间内存块大小
    private final ReentrantLock lock;  //锁
    private final Deque<ByteBuffer> free; //空闲的空间
    private final Deque<Condition> waiters;  //等待分配空间的线程
    /**
    * 未被池化管理的可用内存空间,用于承担大小超过batch.size的消息内存分配。
    * 当前可用内存=nonPooledAvailableMemory + free.size * poolableSize
    */
    private long nonPooledAvailableMemory; 
}
```


## tryAppend

数据结构了解后，下面通过tryAppend()方法，分析消息数据的写入过程：

```
private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers,
                                     Callback callback, Deque<ProducerBatch> deque, long nowMs) {
    ProducerBatch last = deque.peekLast();
    if (last != null) {
        //获取队尾ProducerBatch尝试追加
        FutureRecordMetadata future = last.tryAppend(timestamp, key, value, headers, callback, nowMs);
        if (future == null)
            //ProducerBatch空间不足写入失败
            last.closeForRecordAppends();
        else
            return new RecordAppendResult(future, deque.size() > 1 || last.isFull(), false, false);
    }
    return null;
}
```

继续看ProducerBatch#tryAppend()方法：

```
public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers, Callback callback, long now) {
    if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
        //没有足够的空间写入
        return null;
    } else {
        //写入
        this.recordsBuilder.append(timestamp, key, value, headers);
        this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),recordsBuilder.compressionType(), key, value, headers));
        this.lastAppendTime = now;
        FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,timestamp,key == null ? -1 : key.length,value == null ? -1 : value.length,Time.SYSTEM);
        thunks.add(new Thunk(callback, future));
        this.recordCount++;
        return future;
    }
}
```

逻辑很简单，将消息写入队尾的ProducerBatch，若为null或ProducerBatch没有足够空间写入，返回null，创建新的ProducerBatch。


## BufferPool allocate & deallocate

BufferPool是基于池化思想对消息缓存区进行管理，下面分别介绍下申请缓存的allocate方法和归还缓存的deallocate方法。

### allocate

当需要创建新的ProducerBatch时，首先需要向BufferPool申请足够的ByteBuffer，allocate方法如下：


```
public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (size > this.totalMemory) //内存不足异常
            throw new IllegalArgumentException();
    ByteBuffer buffer = null;
    //加锁 线程安全
    this.lock.lock();
    if (this.closed) {
        this.lock.unlock();
        throw new KafkaException("Producer closed while allocating memory");
    }

    try {
        if (size == poolableSize && !this.free.isEmpty())
            //如果申请的空间大小等于batch.size且有空闲buffer， 直接获取并返回
            return this.free.pollFirst();
        int freeListSize = freeSize() * this.poolableSize;
        if (this.nonPooledAvailableMemory + freeListSize >= size) {
            //申请空间大于batch.size，且剩余空间足够，如有需要合并free至nonPooledAvailableMemory
            freeUp(size);
            this.nonPooledAvailableMemory -= size;
        } else {
            //空间不足
            int accumulated = 0;
            Condition moreMemory = this.lock.newCondition();
            try {
                long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                this.waiters.addLast(moreMemory);
                //循环直至空间足够
                while (accumulated < size) {
                    long startWaitNs = time.nanoseconds();
                    long timeNs;
                    boolean waitingTimeElapsed;
                    try {
                        //添加当前线程至阻塞队列
                        waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                    } finally {
                        long endWaitNs = time.nanoseconds();
                        timeNs = Math.max(0L, endWaitNs - startWaitNs);
                        recordWaitTime(timeNs);
                    }
                    if (this.closed)
                        throw new KafkaException("Producer closed while allocating memory");
                    //等待超时，抛出异常
                    if (waitingTimeElapsed) {
                        this.metrics.sensor("buffer-exhausted-records").record();
                        throw new BufferExhaustedException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                    }
                    //查看可用空间
                    remainingTimeToBlockNs -= timeNs;
                    if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                        //free 列表可用
                        buffer = this.free.pollFirst();
                        accumulated = size;
                    } else {
                        //累加已获取空间
                        freeUp(size - accumulated);
                        int got = (int) Math.min(size - accumulated, this.nonPooledAvailableMemory);
                        this.nonPooledAvailableMemory -= got;
                        accumulated += got;
                    }
                }
                accumulated = 0;
            } finally {
                this.nonPooledAvailableMemory += accumulated;
                this.waiters.remove(moreMemory);
            }
        }
    } finally {
        try {
            if (!(this.nonPooledAvailableMemory == 0 && this.free.isEmpty()) && !this.waiters.isEmpty())
               //唤醒下一个等待线程
                this.waiters.peekFirst().signal();
        } finally {
            lock.unlock();
        }
    }
    if (buffer == null)
        return safeAllocateByteBuffer(size);
    else
        return buffer;
}

```

为保证线程安全，操作前必须获取到锁。当BufferPool中有足够空间时，直接获取指定的内存返回。当BufferPool中可用空间不足时，当前线程加入等待队列，等待超时抛出异常，
当前一个线程获成功取到空间后，会唤醒等待队列的第一个线程，继续收集空闲内存，直到内存足够。

执行流程图如下：

![allocate 执行流程](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/bufferPool.png)

### deallocate

内存归还比较简单，分两种情况：

* 若归还大小等于batch.size，直接将ByteBuffer清空，并添加到free deque中；
* 否则直接将增加PooledAvailableMemory的大小，**ByteBuffer中的对象需要靠GC回收**；

归还buffer后唤醒等待队列的第一个线程。

```
public void deallocate(ByteBuffer buffer, int size) {
    lock.lock();
    try {
        if (size == this.poolableSize && size == buffer.capacity()) {
            buffer.clear();
            this.free.add(buffer);
        } else {
            this.nonPooledAvailableMemory += size;
        }
        Condition moreMem = this.waiters.peekFirst();
        if (moreMem != null)
            moreMem.signal();
    } finally {
        lock.unlock();
    }
}
```

### 总结

1、 **BufferPool只会针对batch.size大小的ByteBuffer进行池化管理**，开发中，需要评估消息的大小，并合理调整batch.size的配置，尽可能的重复利用缓存。

2、如果生产者发送消息的速度超过发送到服务器的速度，则会导致BufferPool可用空间长时间的不足，此时**KafkaProducer#send方法调用会被阻塞，抛出异常**，这个取决于参数**max.block.ms的配置，此参数的默认值为60000，即60s**。
需评估项目中消息生产的速度及BufferPool的大小。