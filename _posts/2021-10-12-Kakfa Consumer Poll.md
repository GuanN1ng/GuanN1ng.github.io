---
layout: post
title:  Kafka Poll
date:   2021-10-12 17:18:31
categories: Kafka
---

前面几篇内容我们分析了`updateAssignmentMetadataIfNeeded`的执行流程，包含两部分内容：

* ConsumerCoordinator#poll方法，获取GroupCoordinator，完成JoinGroup及主题分区方案获取，详情见[Kafka Consumer JoinGroup](https://guann1ng.github.io/kafka/2021/09/06/Kafka-Consumer-JoinGroup/)；
* KafkaConsumer#updateFetchPositions方法，更新consumer订阅的TopicPartition的有效offset，确认下次消息拉取的偏移量(offset)，详情见[Kafka Consumer UpdateFetchPosition](https://guann1ng.github.io/kafka/2021/09/17/Kafka-Consumer-UpdateFetchPosition/)。

本篇内容我们继续KafkaConsumer#poll方法的后半部分内容，即消息拉取部分，方法为KafkaConsumer#pollForFetches。

```
private ConsumerRecords<K, V> poll(final Timer timer, final boolean includeMetadataInTimeout) {
     //consumer不是线程安全的， CAS设置当前threadId获取锁，并确认consumer未关闭
    acquireAndEnsureOpen();
    try {
        //记录开始时间，用于超时返回
        this.kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());
        if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
            throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
        }
        //do-while 超时判断
        do {
            //是否有线程调用wakeup方法，若有则抛出异常WakeupException，退出poll方法。
            client.maybeTriggerWakeup();
            //includeMetadataInTimeout 拉取消息的超时时间是否包含更新元数据的时间
            if (includeMetadataInTimeout) {
                updateAssignmentMetadataIfNeeded(timer, false);
            } else {
                //更新元数据请求
                while (!updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE), true)) {
                    log.warn("Still waiting for metadata");
                }
            }
            //拉取消息
            Map<TopicPartition, List<ConsumerRecord<K, V>>> records = pollForFetches(timer);
            if (!records.isEmpty()) {
                //如果拉取到的消息集合不为空，再返回该批消息之前，如果还有挤压的拉取请求，继续发送拉取请求，
                if (fetcher.sendFetches() > 0 || client.hasPendingRequests()) {
                    client.transmitSends();
                }
                //消费者拦截器
                return this.interceptors.onConsume(new ConsumerRecords<>(records));
            }
        } while (timer.notExpired());
        //超时未拉取到消息，返回空集合
        return ConsumerRecords.empty();
    } finally {
        release(); //释放锁
        this.kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
    }
}
```

### pollForFetches

pollForFetches方法的源码如下，方法可分为三部分：

* 发送消息拉取请求的**Fetcher#sendFetches()**；
* 触发网络读写时间的**ConsumerNetworkClient#poll()**，底层为Java NIO;
* 获取本地已拉取完成的消息记录的**Fetcher#fetchedRecords()**。

```
private Map<TopicPartition, List<ConsumerRecord<K, V>>> pollForFetches(Timer timer) {
    //获取拉取超时时间
    long pollTimeout = coordinator == null ? timer.remainingMs() : Math.min(coordinator.timeToNextPoll(timer.currentTimeMs()), timer.remainingMs());
    //本地已有拉取的消息，返回
    Map<TopicPartition, List<ConsumerRecord<K, V>>> records = fetcher.fetchedRecords();
    if (!records.isEmpty()) {
        return records;
    }
    //发送消息拉取请求
    fetcher.sendFetches();
    if (!cachedSubscriptionHashAllFetchPositions && pollTimeout > retryBackoffMs) {
        pollTimeout = retryBackoffMs;
    }
    Timer pollTimer = time.timer(pollTimeout);
    //NetworkClient poll 触发网络读写
    client.poll(pollTimer, () -> {
        return !fetcher.hasAvailableFetches();
    });
    timer.update(pollTimer.currentTimeMs());
    //返回拉取数据
    return fetcher.fetchedRecords();
}
```

可以看到，消息拉取的核心类为Fetcher，下面简单介绍下org.apache.kafka.clients.consumer.internals.Fetcher。

### Fetcher


```
public class Fetcher<K, V> implements Closeable {
    private final Logger log;
    private final LogContext logContext;
    private final ConsumerNetworkClient client;
    private final Time time;
    private final int minBytes;
    private final int maxBytes;
    private final int maxWaitMs;
    private final int fetchSize;
    private final long retryBackoffMs;
    private final long requestTimeoutMs;
    private final int maxPollRecords;
    private final boolean checkCrcs;
    private final String clientRackId;
    private final ConsumerMetadata metadata;
    private final FetchManagerMetrics sensors;
    private final SubscriptionState subscriptions;
    private final ConcurrentLinkedQueue<CompletedFetch> completedFetches;
    private final BufferSupplier decompressionBufferSupplier = BufferSupplier.create();
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final IsolationLevel isolationLevel;
    private final Map<Integer, FetchSessionHandler> sessionHandlers;
    private final AtomicReference<RuntimeException> cachedListOffsetsException = new AtomicReference<>();
    private final AtomicReference<RuntimeException> cachedOffsetForLeaderException = new AtomicReference<>();
    private final OffsetsForLeaderEpochClient offsetsForLeaderEpochClient;
    private final Set<Integer> nodesWithPendingFetchRequests;
    private final ApiVersions apiVersions;
    private final AtomicInteger metadataUpdateVersion = new AtomicInteger(-1);

    private CompletedFetch nextInLineFetch = null;
    ...
}

```

