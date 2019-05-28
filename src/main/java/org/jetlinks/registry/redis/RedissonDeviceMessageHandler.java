package org.jetlinks.registry.redis;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.DeviceMessageReply;
import org.redisson.api.*;
import org.jetlinks.core.device.registry.DeviceMessageHandler;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class RedissonDeviceMessageHandler implements DeviceMessageHandler {
    private RedissonClient redissonClient;

    private ExecutorService executorService;

    public RedissonDeviceMessageHandler(RedissonClient redissonClient, ExecutorService executorService) {
        this.redissonClient = redissonClient;
        this.executorService = executorService;
    }

    @Override
    public void handleDeviceCheck(String serviceId, Consumer<String> consumer) {
        redissonClient.getTopic("device:state:check:".concat(serviceId))
                .addListenerAsync(String.class, (channel, msg) -> {
                    consumer.accept(msg);
                    redissonClient
                            .getSemaphore("device:state:check:semaphore:".concat(msg))
                            .releaseAsync();
                });
    }

    @Override
    public void handleMessage(String serverId, Consumer<DeviceMessage> deviceMessageConsumer) {
        redissonClient.getTopic("device:message:accept:".concat(serverId))
                .addListenerAsync(DeviceMessage.class, (channel, message) -> {
                    log.debug("接收到发往设备的消息:{}", message.toJson());
                    deviceMessageConsumer.accept(message);
                })
                .thenAccept(id -> log.info("开始接收设备消息"));
    }

    @Override
    public CompletionStage<Boolean> reply(DeviceMessageReply message) {
        RBucket<DeviceMessageReply> bucket = redissonClient.getBucket("device:message:reply:".concat(message.getMessageId()));
        RSemaphore semaphore = redissonClient.getSemaphore("device:reply:".concat(message.getMessageId()));
        bucket.expire(2, TimeUnit.MINUTES);
        semaphore.expire(2, TimeUnit.MINUTES);
        return bucket
                .setAsync(message)
                .thenApply(nil -> {
                    semaphore.releaseAsync();
                    return true;
                });

    }

    @Override
    public void close() {
    }
}
