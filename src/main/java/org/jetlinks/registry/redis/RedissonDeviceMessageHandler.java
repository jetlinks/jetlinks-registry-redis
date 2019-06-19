package org.jetlinks.registry.redis;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.registry.DeviceMessageHandler;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.DeviceMessageReply;
import org.redisson.api.RBucket;
import org.redisson.api.RSemaphore;
import org.redisson.api.RedissonClient;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class RedissonDeviceMessageHandler implements DeviceMessageHandler {
    private RedissonClient redissonClient;

    @Getter
    @Setter
    private long replyExpireTimeSeconds = Long.getLong("device.message.reply.expire-time-seconds", TimeUnit.MINUTES.toSeconds(3));

    @Getter
    @Setter
    private long asyncFlagExpireTimeSeconds = Long.getLong("device.message.async-flag.expire-time-seconds", TimeUnit.MINUTES.toSeconds(30));

    public RedissonDeviceMessageHandler(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    @Override
    public void handleDeviceCheck(String serviceId, Consumer<String> consumer) {
        redissonClient
                .getTopic("device:state:check:".concat(serviceId))
                .addListener(String.class, (channel, msg) -> {
                    consumer.accept(msg);
                    RSemaphore semaphore = redissonClient
                            .getSemaphore("device:state:check:semaphore:".concat(msg));
                    semaphore.expireAsync(5, TimeUnit.SECONDS)
                            .thenRun(semaphore::releaseAsync);
                });
    }

    @Override
    public void handleMessage(String serverId, Consumer<DeviceMessage> deviceMessageConsumer) {
        redissonClient.getTopic("device:message:accept:".concat(serverId))
                .addListener(DeviceMessage.class, (channel, message) -> {
                    if (log.isDebugEnabled()) {
                        log.debug("接收到发往设备的消息:{}", message.toJson());
                    }
                    deviceMessageConsumer.accept(message);
                });
    }

    @Override
    public CompletionStage<Boolean> reply(DeviceMessageReply message) {
        RBucket<DeviceMessageReply> bucket = redissonClient.getBucket("device:message:reply:".concat(message.getMessageId()));
        RSemaphore semaphore = redissonClient.getSemaphore("device:reply:".concat(message.getMessageId()));

        return bucket
                .setAsync(message)
                .thenApply(nil -> {
                    bucket.expireAsync(replyExpireTimeSeconds, TimeUnit.SECONDS)
                            .thenRun(() ->
                                    semaphore.releaseAsync()
                                    .thenRun(() -> semaphore.expireAsync(replyExpireTimeSeconds, TimeUnit.SECONDS)));
                    return true;
                });

    }

    @Override
    public CompletionStage<Void> markMessageAsync(String messageId) {
        RBucket<Boolean> bucket = redissonClient.getBucket("async-msg:".concat(messageId));
        return bucket.setAsync(true)
                .thenRun(() -> bucket.expireAsync(asyncFlagExpireTimeSeconds, TimeUnit.SECONDS));
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
                            bucket.expireAsync(asyncFlagExpireTimeSeconds, TimeUnit.SECONDS);
                        }
                    });
        }
    }
}
