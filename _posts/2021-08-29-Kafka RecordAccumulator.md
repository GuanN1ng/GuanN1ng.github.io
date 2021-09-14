---
title: Kafka RecordAccumulator
date:   2021-08-29 Kafka Accumulator
categories: Kafka
---

RecordAccumulator主要用来缓存消息以便Sender线程可以批量的发送消息，进而减少网络传输的资源消耗以提升性能。RecordAccumulator中涉及消息缓存的主要有2个参数：

* CopyOnWriteMap<TopicPartition, Deque<ProducerBatch>> batches：分区信息与消息队列的映射关系

* BufferPool：通过池化思想管理RecordAccumulator缓冲区，可用内存大小由参数buffer.memory设置，默认32MB。

二者关系如下，写入ProducerBatch内的消息是通过BufferPool管理的ByteBuffer存储的：

![RecordAccumulator](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/recordAccumulator.png)

### batches

写入RecordAccumulator的消息通过CopyOnWriteMap进行管理，key为消息分区TopicPartition,value为ArrayDeque的双端队列，队列内的元素为ProducerBatch对象，
用于存储一条或多条消息。

#### CopyOnWriteMap

batches维护的是分区信息与消息队列的映射关系，它写的场景仅存在于当batches中不存在这个分区对应的deque，需要创建这个分区对应的deque，所以**batches是"读多写少下"的数据结构**，Kafka使用自定义的CopyOnWriteMap实现，
与JDK中的CopyOnWriteArrayList采用相同的实现思路，**即写时复制(Copy-On-Write)的思想来通过延时更新的策略来实现数据的最终一致性，再加上读写分离保证读线程间不阻塞**，来保证batches对象的线程安全及并发性能。

```
public class CopyOnWriteMap<K, V> implements ConcurrentMap<K, V> {
    private volatile Map<K, V> map;
    
    @Override
    public synchronized V put(K k, V v) {  //加锁
        Map<K, V> copy = new HashMap<K, V>(this.map);
        V prev = copy.put(k, v);  //操作复制对象
        this.map = Collections.unmodifiableMap(copy);
        return prev;
    }
}
```

#### ProducerBatch

Producer调用RecordAccumulator#append()方法，将消息追加写入ProducerBatch中，每个ProducerBatch内部有一条或多条消息，具体取决于batch.size和消息大小。
当一条消息(ProducerRecord)写入RecordAccumulator时，具体流程如下：

* 从batches中获取消息TopicPartition对应的Deque，若无则创建；
* 从队列尾部获取一个ProducerBatch，尝试将消息写入该ProducerBatch中；
* 上一步失败(获取的ProducerBatch为null或空间不足)，创建新的ProducerBatch，向BufferPool申请内存，此时评估消息的大小是否超过batch.size，若未超过，以batch.size参数的大小创建ProducerBatch，默认16KB，否则以
消息的大小创建。
* 完成ProducerBatch构建，并添加的队列尾部。

```
public final class ProducerBatch {
    private enum FinalState { ABORTED, FAILED, SUCCEEDED }
    final TopicPartition topicPartition; //消息分区
    private final MemoryRecordsBuilder recordsBuilder; //消息存储的ByteBuffer
}
```

### BufferPool

ProducerBatch底层是通过ByteBuffer实现消息的存储的，消息发送成功后需要释放对应的ByteBuffer，频繁的创建和释放ByteBuffer是比较耗费性能的，Kafka引入BufferPool实现ByteBuffer的复用。

#### 主要属性

```
private final long totalMemory;  // buffer.buffer.memory配置，缓存空间大小
private final int poolableSize;  // batch.size   缓存空间内存块大小
private final ReentrantLock lock;  //锁
private final Deque<ByteBuffer> free; //空闲的空间
private final Deque<Condition> waiters;  //等待分配空间的线程
/**
 * 未被池化管理的可用内存空间,用于承担大小超过batch.size的消息内存分配。
 * 可用内存=nonPooledAvailableMemory + free.size * poolableSize
 */
private long nonPooledAvailableMemory; 

```

#### allocate

allocate操作发生在新建ProducerBatch时， 未保证线程安全，操作前必须获取到锁。**当BufferPool中有足够空间时：** 直接获取指定的内存返回。

```
//如果申请的空间大小等于batch.size且有空闲buffer， 直接获取并返回
if (size == poolableSize && !this.free.isEmpty()) 
                return this.free.pollFirst();
//申请空间大于batch.size，且剩余空间足够，如有需要合并free至nonPooledAvailableMemory
int freeListSize = freeSize() * this.poolableSize;
if (this.nonPooledAvailableMemory + freeListSize >= size) {
    freeUp(size);
    this.nonPooledAvailableMemory -= size;
}

private void freeUp(int size) {
    while (!this.free.isEmpty() && this.nonPooledAvailableMemory < size)
        this.nonPooledAvailableMemory += this.free.pollLast().capacity();
}
```

**当BufferPool中可用空间不足时：** 当前线程加入等待队列，当有内存归还时，会唤醒等待队列的第一个线程，收集空闲内存，直到内存足够或超时。

```
int accumulated = 0;
Condition moreMemory = this.lock.newCondition();
try {
    long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
    //加入阻塞队列
    this.waiters.addLast(moreMemory);
、   //while循环中获取空闲内存
    while (accumulated < size) {
        long startWaitNs = time.nanoseconds();
        long timeNs;
        boolean waitingTimeElapsed;
        try {
            //await阻塞当前线程，等待有内存归还时唤醒
            waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
        } finally {
            long endWaitNs = time.nanoseconds();
            timeNs = Math.max(0L, endWaitNs - startWaitNs);
            recordWaitTime(timeNs);
        }

        if (this.closed)
            throw new KafkaException("Producer closed while allocating memory");
        //阻塞超时
        if (waitingTimeElapsed) {
            this.metrics.sensor("buffer-exhausted-records").record();
            throw new BufferExhaustedException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
        }
        remainingTimeToBlockNs -= timeNs;
        if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
            buffer = this.free.pollFirst();
            accumulated = size;
        } else {
            //申请大小超过batch.size
            freeUp(size - accumulated);
            int got = (int) Math.min(size - accumulated, this.nonPooledAvailableMemory);
            this.nonPooledAvailableMemory -= got;
            accumulated += got;
        }
    }
    // Don't reclaim memory on throwable since nothing was thrown
    accumulated = 0;
} finally {
    // When this loop was not able to successfully terminate don't loose available memory
    this.nonPooledAvailableMemory += accumulated;
    this.waiters.remove(moreMemory);
}
```



执行流程图如下：

![allocate 执行流程](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/bufferPool.png)

#### deallocate

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

1、 **BufferPool只会针对batch.size大小的ByteBuffer进行池化管理**，开发中，需要评估消息的大小，适当的调大batch.size，尽可能的重复利用缓存。

2、如果生产者发送消息的速度超过发送到服务器的速度，则会导致BufferPool可用空间长时间的不足，此时**KafkaProducer#send方法调用会被阻塞，抛出异常**，这个取决于参数**max block ms的配置，此参数的默认值为60000，即60s**。
需评估项目中消息生产的速度及BufferPool的大小。