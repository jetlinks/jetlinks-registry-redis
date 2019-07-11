package org.jetlinks.registry.redis.lettuce;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.jetlinks.core.device.registry.DeviceMessageHandler;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.DeviceMessageReply;
import org.jetlinks.lettuce.LettucePlus;
import org.jetlinks.lettuce.codec.StringCodec;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class LettuceDeviceMessageHandler implements DeviceMessageHandler {
    private LettucePlus plus;

    private Map<String, MessageFuture> futureMap = new ConcurrentHashMap<>();

    private static int replyExpireTimeSeconds = Integer.getInteger("device.message.reply.expire-time-seconds", (int) TimeUnit.MINUTES.toSeconds(3));

    private static int asyncFlagExpireTimeSeconds = Integer.getInteger("device.message.async-flag.expire-time-seconds", (int) TimeUnit.MINUTES.toSeconds(30));

    private Map<String, Consumer<DeviceMessage>> localConsumer = new ConcurrentHashMap<>();

    public LettuceDeviceMessageHandler(LettucePlus plus) {
        this.plus = plus;

        //监听消息返回
        this.plus.<String>getTopic(StringCodec.getInstance(), "device:message:reply")
                .addListener((channel, msg) -> Optional.ofNullable(futureMap.remove(msg))
                        .map(MessageFuture::getFuture)
                        .ifPresent(future -> tryComplete(msg, future)));

        //定时检查超时消息
        plus.getExecutor().scheduleAtFixedRate(() -> futureMap
                .entrySet()
                .stream()
                .filter(e -> System.currentTimeMillis() > e.getValue().expireTime)
                .forEach((e) -> {
                    try {
                        tryComplete(e.getKey(), e.getValue().future);
                    } finally {
                        log.info("设备消息[{}]超时未返回", e.getKey());
                        futureMap.remove(e.getKey());
                    }
                }), 1, 5, TimeUnit.SECONDS);
    }

    private void tryComplete(String messageId, CompletableFuture<Object> future) {
        if (!future.isCancelled()) {
            plus.getConnection()
                    .thenApply(StatefulRedisConnection::async)
                    .thenCompose(redis -> redis.get("device:message:reply:".concat(messageId)))
                    .whenComplete((data, error) -> {
                        if (error != null) {
                            future.completeExceptionally(error);
                        } else {
                            future.complete(data);
                        }
                    });
        }
    }

    @Override
    public void handleDeviceCheck(String serviceId, Consumer<String> consumer) {
        plus.<String>getTopic("device:state:check:".concat(serviceId))
                .addListener((channel, msg) -> {
                    if (StringUtils.isEmpty(msg)) {
                        return;
                    }
                    consumer.accept(msg);
                });
    }

    @Override
    public void handleMessage(String serverId, Consumer<DeviceMessage> deviceMessageConsumer) {
        localConsumer.put(serverId, deviceMessageConsumer);

        plus.<DeviceMessage>getTopic("device:message:accept:".concat(serverId))
                .addListener((channel, message) -> {
                    if (log.isDebugEnabled()) {
                        log.debug("接收到发往设备的消息:{}", message.toJson());
                    }
                    deviceMessageConsumer.accept(message);
                });
    }

    @AllArgsConstructor
    @Getter
    private class MessageFuture {
        private String messageId;

        private CompletableFuture<Object> future;

        private long expireTime;
    }

    @Override
    public CompletionStage<Object> handleReply(String messageId, long timeout, TimeUnit timeUnit) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        futureMap.put(messageId, new MessageFuture(messageId, future, System.currentTimeMillis() + timeUnit.toMillis(timeout)));

        return future;
    }

    public CompletionStage<Long> send(String serverId, DeviceMessage message) {
        Consumer<DeviceMessage> consumer = localConsumer.get(serverId);
        if (consumer != null) {
            consumer.accept(message);
            return CompletableFuture.completedFuture(1L);
        }
        return plus.getTopic("device:message:accept:".concat(serverId)).publish(message);
    }

    @Override
    public CompletionStage<Boolean> reply(DeviceMessageReply message) {
        String messageId = message.getMessageId();
        MessageFuture future = futureMap.get(messageId);

        if (null != future) {
            futureMap.remove(messageId);
            future.getFuture().complete(message);
            return CompletableFuture.completedFuture(true);
        }
        String script = "" +
                "redis.call('setex',KEYS[1]," + replyExpireTimeSeconds + ",ARGV[1]);" +
                "return redis.call('publish',KEYS[2],KEYS[3]);";

        return plus.<Long>eval(script, ScriptOutputType.INTEGER,
                new Object[]{"device:message:reply:".concat(messageId), "device:message:reply", messageId},
                message)
                .thenApply((num) -> {
                    if (num <= 0) {
                        log.warn("消息回复[{}]没有任何服务消费", message.getMessageId());
                    }
                    return num > 0;
                }).whenComplete((success, error) -> {
                    if (error != null) {
                        log.error("回复消息失败", error);
                    }
                });


    }


    @Override
    public CompletionStage<Void> markMessageAsync(String messageId) {
        return plus.<String, Boolean>getConnection()
                .thenApply(StatefulRedisConnection::async)
                .thenCompose(redis -> redis.setex("async-msg:".concat(messageId), asyncFlagExpireTimeSeconds, true))
                .thenApply(str -> null);
    }

    @Override
    public CompletionStage<Boolean> messageIsAsync(String messageId, boolean reset) {

        return plus.<String, Boolean>getConnection()
                .thenApply(StatefulRedisConnection::async)
                .thenCompose(redis -> redis.get("async-msg:".concat(messageId)));
    }
}
