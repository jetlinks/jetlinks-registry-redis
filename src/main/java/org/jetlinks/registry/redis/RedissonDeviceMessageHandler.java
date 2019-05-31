package org.jetlinks.registry.redis;

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


    public RedissonDeviceMessageHandler(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    @Override
    public void handleDeviceCheck(String serviceId, Consumer<String> consumer) {
        redissonClient.getTopic("device:state:check:".concat(serviceId))
                .addListenerAsync(String.class, (channel, msg) -> {
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
                .addListenerAsync(DeviceMessage.class, (channel, message) -> {
                    if (log.isDebugEnabled()) {
                        log.debug("接收到发往设备的消息:{}", message.toJson());
                    }
                    deviceMessageConsumer.accept(message);
                })
                .thenAccept(id -> log.info("开始接收设备消息"));
    }

    @Override
    public CompletionStage<Boolean> reply(DeviceMessageReply message) {
        RBucket<DeviceMessageReply> bucket = redissonClient.getBucket("device:message:reply:".concat(message.getMessageId()));
        RSemaphore semaphore = redissonClient.getSemaphore("device:reply:".concat(message.getMessageId()));
        bucket.expireAsync(2, TimeUnit.MINUTES);
        semaphore.expireAsync(2, TimeUnit.MINUTES);
        return bucket
                .setAsync(message)
                .thenApply(nil -> {
                    semaphore.releaseAsync();
                    return true;
                });

    }
}
