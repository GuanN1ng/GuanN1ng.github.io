---
layout: post
title:  Kafka GroupCoordinator
date:   2021-10-12 17:18:31
categories: Kafka
---


consumer端


## pollForFetches
poll方法实现如下：
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
                //如果拉取到的消息集合不为空，再返回该批消息之前，如果还有挤压的拉取请求，可以继续发送拉取请求，
                //但此时会禁用warkup，主要的目的是用户在处理消息时，KafkaConsumer还可以继续向broker拉取消息。
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


KafkaConsumer#pollForFetches

```
private Map<TopicPartition, List<ConsumerRecord<K, V>>> pollForFetches(Timer timer) {
    long pollTimeout = coordinator == null ? timer.remainingMs() :
            Math.min(coordinator.timeToNextPoll(timer.currentTimeMs()), timer.remainingMs());

    Map<TopicPartition, List<ConsumerRecord<K, V>>> records = fetcher.fetchedRecords();
    if (!records.isEmpty()) {
        return records;
    }

    fetcher.sendFetches();

    if (!cachedSubscriptionHashAllFetchPositions && pollTimeout > retryBackoffMs) {
        pollTimeout = retryBackoffMs;
    }

    log.trace("Polling for fetches with timeout {}", pollTimeout);

    Timer pollTimer = time.timer(pollTimeout);
    client.poll(pollTimer, () -> {
        return !fetcher.hasAvailableFetches();
    });
    timer.update(pollTimer.currentTimeMs());

    return fetcher.fetchedRecords();
}
```

