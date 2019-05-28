package org.jetlinks.registry.redis;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceMessageSender;
import org.jetlinks.core.enums.ErrorCode;
import org.jetlinks.core.message.*;
import org.jetlinks.core.message.function.FunctionInvokeMessage;
import org.jetlinks.core.message.function.FunctionInvokeMessageReply;
import org.jetlinks.core.message.function.FunctionParameter;
import org.jetlinks.core.message.property.ReadPropertyMessage;
import org.jetlinks.core.message.property.ReadPropertyMessageReply;
import org.jetlinks.core.message.property.WritePropertyMessage;
import org.jetlinks.core.message.property.WritePropertyMessageReply;
import org.jetlinks.core.utils.IdUtils;
import org.redisson.api.RSemaphore;
import org.redisson.api.RedissonClient;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.jetlinks.core.enums.ErrorCode.NO_REPLY;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class RedissonDeviceMessageSender implements DeviceMessageSender {

    private RedissonClient redissonClient;

    private Supplier<String> connectionServerIdSupplier;

    private Runnable deviceStateChecker;

    private String deviceId;

    public RedissonDeviceMessageSender(String deviceId,
                                       RedissonClient redissonClient,
                                       Supplier<String> connectionServerIdSupplier,
                                       Runnable deviceStateChecker) {
        this.redissonClient = redissonClient;
        this.connectionServerIdSupplier = connectionServerIdSupplier;
        this.deviceStateChecker = deviceStateChecker;
        this.deviceId = deviceId;
    }

    //最大等待30秒
    @Getter
    @Setter
    private int maxSendAwaitSeconds = Integer.getInteger("device.message.await.max-seconds", 30);

    @SuppressWarnings("all")
    private <R extends DeviceMessageReply> R convertReply(Object deviceReply, RepayableDeviceMessage<R> message, Supplier<R> replyNewInstance) {
        R reply = replyNewInstance.get();
        if (deviceReply == null) {
            reply.error(NO_REPLY);
        } else if (deviceReply instanceof ErrorCode) {
            reply.error((ErrorCode) deviceReply);
        } else {
            try {
                if (reply.getClass().isAssignableFrom(deviceReply.getClass())) {
                    return reply = (R) deviceReply;
                } else if (deviceReply instanceof String) {
                    reply.fromJson(JSON.parseObject(String.valueOf(deviceReply)));
                } else if (deviceReply instanceof DeviceMessage) {
                    reply.fromJson(((DeviceMessage) deviceReply).toJson());
                } else {
                    reply.error(ErrorCode.UNSUPPORTED_MESSAGE);
                    log.warn("不支持的消息类型:{}", deviceReply.getClass());
                }
            } finally {
                log.debug("收到设备回复消息[{}]<={}", reply.getDeviceId(), reply);
            }
        }
        if (message != null) {
            reply.from(message);
        }
        return reply;
    }

    public <R extends DeviceMessageReply> CompletionStage<R> retrieveReply(String deviceId, String messageId, Supplier<R> replyNewInstance) {
        return redissonClient
                .getBucket("device:message:reply:".concat(messageId))
                .getAndDeleteAsync()
                .thenApply(deviceReply -> convertReply(deviceReply, null, replyNewInstance));
    }

    @Override
    public <R extends DeviceMessageReply> CompletionStage<R> send(DeviceMessage message, Function<Object, R> replyMapping) {
        String serverId = connectionServerIdSupplier.get();
        //设备当前没有连接到任何服务器
        if (serverId == null) {
            R reply = replyMapping.apply(null);
            if (reply != null) {
                reply.error(ErrorCode.CLIENT_OFFLINE);
            }
            return CompletableFuture.completedFuture(reply);
        }
        //发送消息给设备当前连接的服务器
        return redissonClient
                .getTopic("device:message:accept:".concat(serverId))
                .publishAsync(message)
                .thenCompose(deviceConnectedServerNumber -> {
                    if (deviceConnectedServerNumber <= 0) {
                        //没有任何服务消费此topic,可能所在服务器已经宕机,注册信息没有更新。
                        //执行设备状态检查,尝试更新设备的真实状态
                        if (deviceStateChecker != null) {
                            deviceStateChecker.run();
                        }
                        return CompletableFuture.completedFuture(ErrorCode.CLIENT_OFFLINE);
                    }
                    //有多个相同名称的设备网关服务,可能是服务配置错误,启动了多台相同id的服务。
                    if (deviceConnectedServerNumber > 1) {
                        log.warn("存在多个相同的网关服务:{}", serverId);
                    }
                    try {
                        if (log.isDebugEnabled()) {
                            log.debug("发送设备消息[{}]=>{}", message.getDeviceId(), message);
                        }
                        //使用信号量异步等待设备回复通知
                        RSemaphore semaphore = redissonClient.getSemaphore("device:reply:".concat(message.getMessageId()));
                        //设置有效期,防止等待超时或者失败后,信号一直存在于redis中
                        semaphore.expireAsync(maxSendAwaitSeconds + 10, TimeUnit.SECONDS);
                        return semaphore
                                .tryAcquireAsync(deviceConnectedServerNumber.intValue(), maxSendAwaitSeconds, TimeUnit.SECONDS)
                                .thenCompose(complete -> {
                                    try {
                                        if (complete) {
                                            //从redis中获取设备返回的数据
                                            return redissonClient
                                                    .getBucket("device:message:reply:".concat(message.getMessageId()))
                                                    .getAndDeleteAsync();
                                        }
                                        return CompletableFuture.completedFuture(null);
                                    } finally {
                                        //删除信号
                                        semaphore.deleteAsync();
                                    }
                                });
                    } catch (Exception e) {
                        //异常直接返回错误
                        log.error(e.getMessage(), e);
                        return CompletableFuture.completedFuture(ErrorCode.SYSTEM_ERROR);
                    }
                })
                .thenApply(replyMapping);
    }

    @Override
    @SuppressWarnings("all")
    public <R extends DeviceMessageReply> CompletionStage<R> send(RepayableDeviceMessage<R> message) {
        return send(message, deviceReply -> convertReply(deviceReply, message, message::newReply));
    }

    @Override
    public FunctionInvokeMessageSender invokeFunction(String function) {
        Objects.requireNonNull(function, "function");
        FunctionInvokeMessage invokeMessage = new FunctionInvokeMessage();
        invokeMessage.setTimestamp(System.currentTimeMillis());
        invokeMessage.setDeviceId(deviceId);
        invokeMessage.setFunctionId(function);
        invokeMessage.setMessageId(IdUtils.newUUID());
        return new FunctionInvokeMessageSender() {
            @Override
            public FunctionInvokeMessageSender addParameter(String name, Object value) {
                invokeMessage.addInput(name, value);
                return this;
            }

            @Override
            public FunctionInvokeMessageSender messageId(String messageId) {
                if (messageId == null || messageId.length() < 16) {
                    throw new IllegalArgumentException("messageId长度不能低于16");
                }
                invokeMessage.setMessageId(messageId);
                return this;
            }

            @Override
            public FunctionInvokeMessageSender setParameter(List<FunctionParameter> parameter) {
                invokeMessage.setInputs(parameter);
                return this;
            }

            @Override
            public FunctionInvokeMessageSender async() {
                invokeMessage.setAsync(true);
                return this;
            }

            @Override
            public CompletionStage<FunctionInvokeMessageReply> send() {
                return RedissonDeviceMessageSender.this.send(invokeMessage);
            }

        };
    }

    @Override
    public ReadPropertyMessageSender readProperty(String... property) {

        ReadPropertyMessage message = new ReadPropertyMessage();
        message.setDeviceId(deviceId);
        message.addProperties(Arrays.asList(property));
        message.setMessageId(IdUtils.newUUID());
        return new ReadPropertyMessageSender() {
            @Override
            public ReadPropertyMessageSender messageId(String messageId) {
                message.setMessageId(messageId);
                return this;
            }

            @Override
            public ReadPropertyMessageSender read(List<String> properties) {
                message.addProperties(properties);
                return this;
            }

            @Override
            public CompletionStage<ReadPropertyMessageReply> send() {
                return RedissonDeviceMessageSender.this.send(message);
            }
        };
    }

    @Override
    public WritePropertyMessageSender writeProperty() {

        WritePropertyMessage message = new WritePropertyMessage();
        message.setDeviceId(deviceId);
        message.setMessageId(IdUtils.newUUID());
        return new WritePropertyMessageSender() {
            @Override
            public WritePropertyMessageSender write(String property, Object value) {
                message.addProperty(property, value);
                return this;
            }

            @Override
            public CompletionStage<WritePropertyMessageReply> send() {
                return RedissonDeviceMessageSender.this.send(message);
            }
        };
    }
}
