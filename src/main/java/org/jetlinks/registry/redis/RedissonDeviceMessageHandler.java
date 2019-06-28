package org.jetlinks.registry.redis;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.jetlinks.core.device.registry.DeviceMessageHandler;
import org.jetlinks.core.enums.ErrorCode;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.DeviceMessageReply;
import org.redisson.api.RBucket;
import org.redisson.api.RSemaphore;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class RedissonDeviceMessageHandler implements DeviceMessageHandler {
    private RedissonClient redissonClient;

    private Map<String, MessageFuture> futureMap = new ConcurrentHashMap<>();

    @Getter
    @Setter
    private long replyExpireTimeSeconds = Long.getLong("device.message.reply.expire-time-seconds", TimeUnit.MINUTES.toSeconds(3));

    @Getter
    @Setter
    private long asyncFlagExpireTimeSeconds = Long.getLong("device.message.async-flag.expire-time-seconds", TimeUnit.MINUTES.toSeconds(30));

    private RTopic replyTopic;

    private Map<String, Consumer<DeviceMessage>> localConsumer = new ConcurrentHashMap<>();


    public RedissonDeviceMessageHandler(RedissonClient redissonClient) {
        this(redissonClient, Executors.newSingleThreadScheduledExecutor());
    }

    public RedissonDeviceMessageHandler(RedissonClient redissonClient, ScheduledExecutorService executorService) {
        this.redissonClient = redissonClient;
        replyTopic = this.redissonClient.getTopic("device:message:reply");

        replyTopic.addListener(String.class, (channel, msg) -> Optional.ofNullable(futureMap.remove(msg))
                .map(MessageFuture::getFuture)
                .ifPresent(future -> {
                    if (!future.isCancelled()) {
                        CompletableFuture
                                .supplyAsync(() -> redissonClient.getBucket("device:message:reply:".concat(msg)).getAndDelete())
                                .whenComplete((data, error) -> {
                                    if (error != null) {
                                        future.completeExceptionally(error);
                                    } else {
                                        future.complete(data);
                                    }
                                });
                    }
                }));

        executorService.scheduleAtFixedRate(() -> {
            futureMap.entrySet()
                    .stream()
                    .filter(e -> System.currentTimeMillis() > e.getValue().expireTime)
                    .forEach((e) -> {
                        try {
                            CompletableFuture<Object> future = e.getValue().future;
                            if (!future.isCancelled()) {
                                future.complete(ErrorCode.TIME_OUT);
                                return;
                            }
                            redissonClient.getBucket("device:message:reply:".concat(e.getKey()))
                                    .getAndDeleteAsync()
                                    .whenComplete((o, throwable) -> {
                                        if (o != null) {
                                            future.complete(o);
                                        } else {
                                            future.completeExceptionally(throwable);
                                        }
                                    });

                        } finally {
                            log.info("设备消息[{}]超时未返回", e.getKey());
                            futureMap.remove(e.getKey());
                        }
                    });
        }, 1, 5, TimeUnit.SECONDS);
    }

    @Override
    public void handleDeviceCheck(String serviceId, Consumer<String> consumer) {
        redissonClient
                .getTopic("device:state:check:".concat(serviceId))
                .addListener(String.class, (channel, msg) -> {
                    if (StringUtils.isEmpty(msg)) {
                        return;
                    }
                    consumer.accept(msg);
                    RSemaphore semaphore = redissonClient
                            .getSemaphore("device:state:check:semaphore:".concat(msg));
                    semaphore.expireAsync(5, TimeUnit.SECONDS)
                            .thenRun(semaphore::releaseAsync);
                });
    }

    @Override
    public void handleMessage(String serverId, Consumer<DeviceMessage> deviceMessageConsumer) {
        localConsumer.put(serverId, deviceMessageConsumer);
        redissonClient.getTopic("device:message:accept:".concat(serverId))
                .addListener(DeviceMessage.class, (channel, message) -> {
                    if (log.isDebugEnabled()) {
                        log.debug("接收到发往设备的消息:{}", message.toJson());
                    }
                    deviceMessageConsumer.accept(message);
                });
    }

    @AllArgsConstructor
    @Getter
    private class MessageFuture {
        private CompletableFuture<Object> future;

        private long expireTime;
    }

    @Override
    public CompletionStage<Object> handleReply(String messageId, long timeout, TimeUnit timeUnit) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        futureMap.put(messageId, new MessageFuture(future, System.currentTimeMillis() + timeUnit.toMillis(timeout)));

        return future;
    }

    public CompletionStage<Long> send(String serverId, DeviceMessage message) {
        Consumer<DeviceMessage> consumer = localConsumer.get(serverId);
        if (consumer != null) {
            consumer.accept(message);
            return CompletableFuture.completedFuture(1L);
        }
        return redissonClient.getTopic("device:message:accept:".concat(serverId))
                .publishAsync(message);
    }

    @Override
    public CompletionStage<Boolean> reply(DeviceMessageReply message) {
        MessageFuture future = futureMap.get(message.getMessageId());

        if (null != future) {
            futureMap.remove(message.getMessageId());
            future.getFuture().complete(message);
            return CompletableFuture.completedFuture(true);
        }

        RBucket<DeviceMessageReply> bucket = redissonClient.getBucket("device:message:reply:".concat(message.getMessageId()));

        return CompletableFuture.runAsync(() -> bucket.set(message, replyExpireTimeSeconds, TimeUnit.SECONDS))
                .thenApply(nil -> true)
                .whenComplete((success, error) -> {
                    long num = replyTopic.publish(message.getMessageId());
                    if (num <= 0) {
                        log.warn("消息回复[{}]没有任何服务消费", message.getMessageId());
                    }
                });

    }

    @Override
    public CompletionStage<Void> markMessageAsync(String messageId) {
        RBucket<Boolean> bucket = redissonClient.getBucket("async-msg:".concat(messageId));
        return bucket.setAsync(true, asyncFlagExpireTimeSeconds, TimeUnit.SECONDS);
    }

    @Override
    public CompletionStage<Boolean> messageIsAsync(String messageId, boolean reset) {
        RBucket<Boolean> bucket = redissonClient.getBucket("async-msg:".concat(messageId));
        if (reset) {
            return bucket.getAndDeleteAsync();
        } else {
            return bucket.getAsync()
                    .whenComplete((s, err) -> {
                        if (s != null) {
                            bucket.expireAsync(replyExpireTimeSeconds, TimeUnit.SECONDS);
                        }
                    });
        }
    }
}
