package org.jetlinks.registry.redis.lettuce;

import com.alibaba.fastjson.JSON;
import io.lettuce.core.KeyValue;
import io.lettuce.core.Value;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.*;
import org.jetlinks.core.device.registry.DeviceMessageHandler;
import org.jetlinks.core.device.registry.DeviceRegistry;
import org.jetlinks.core.message.DisconnectDeviceMessage;
import org.jetlinks.core.message.DisconnectDeviceMessageReply;
import org.jetlinks.core.message.interceptor.DeviceMessageSenderInterceptor;
import org.jetlinks.core.metadata.DefaultValueWrapper;
import org.jetlinks.core.metadata.DeviceMetadata;
import org.jetlinks.core.metadata.NullValueWrapper;
import org.jetlinks.core.metadata.ValueWrapper;
import org.jetlinks.core.utils.IdUtils;
import org.jetlinks.lettuce.LettucePlus;
import org.jetlinks.registry.redis.NullValue;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class LettuceDeviceOperation implements DeviceOperation {

    private LettucePlus plus;

    private Map<String, Object> localCache = new ConcurrentHashMap<>(32);
    private Map<String, Object> confCache = new ConcurrentHashMap<>(32);

    private ProtocolSupports protocolSupports;

    private DeviceRegistry registry;

    private String deviceId;

    private Consumer<Boolean> changedListener;

    @Setter
    @Getter
    private DeviceMessageSenderInterceptor interceptor;

    private LettuceDeviceMessageSender messageSender;

    private String redisKey;

    public LettuceDeviceOperation(String deviceId,
                                  LettucePlus plus,
                                  ProtocolSupports protocolSupports,
                                  DeviceMessageHandler deviceMessageHandler,
                                  DeviceRegistry registry,
                                  DeviceMessageSenderInterceptor interceptor,
                                  Consumer<Boolean> changedListener) {
        this.redisKey = deviceId.concat(":reg");
        this.deviceId = deviceId;
        this.plus = plus;
        this.protocolSupports = protocolSupports;
        this.registry = registry;
        this.interceptor = interceptor;
        this.changedListener = (isConf) -> {
            clearCache(isConf);
            changedListener.accept(isConf);
        };
        messageSender = new LettuceDeviceMessageSender(deviceId, plus, deviceMessageHandler, this);
        messageSender.setInterceptor(interceptor);
    }

    protected <K, V> CompletionStage<RedisAsyncCommands<K, V>> getAsyncRedis() {
        return plus
                .<K, V>getConnection()
                .thenApply(StatefulRedisConnection::async);
    }

    void clearCache(boolean isConf) {
        if (isConf) {
            confCache.clear();
        } else {
            localCache.clear();
        }

    }

    @Override
    public String getDeviceId() {
        return deviceId;
    }


    @SneakyThrows
    private <K, V, T> T executeSync(Function<RedisAsyncCommands<K, V>, CompletionStage<T>> function) {
        return this.<K, V>getAsyncRedis()
                .thenCompose(function)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
    }

    private <K, V> void executeAsync(Consumer<RedisAsyncCommands<K, V>> consumer) {
        this.<K, V>getAsyncRedis()
                .thenAccept(consumer);
    }

    @SuppressWarnings("all")
    private <T> T tryGetFromLocalCache(String key) {
        Object val = localCache.get(key);
        if (val == NullValue.instance) {
            return null;
        }
        if (val != null) {
            return (T) val;
        } else {
            localCache.put(key, Optional.ofNullable(val = executeSync(redis -> redis.hget(redisKey, key))).orElse(NullValue.instance));
        }
        return (T) val;
    }

    private <T> T tryGetFromConfCache(String key) {
        Object val = confCache.get(key);
        if (val == NullValue.instance) {
            return null;
        }
        if (val != null) {
            return (T) val;
        } else {
            confCache.put(key, Optional.ofNullable(val = executeSync(redis -> redis.hget(redisKey, key))).orElse(NullValue.instance));
        }
        return (T) val;
    }

    @Override
    public String getServerId() {
        String serverId = tryGetFromLocalCache("serverId");
        if (serverId == null || serverId.isEmpty()) {
            return null;
        }
        return serverId;
    }

    @Override
    public String getSessionId() {
        String sessionId = tryGetFromLocalCache("sessionId");
        if (sessionId == null || sessionId.isEmpty()) {
            return null;
        }
        return sessionId;
    }

    @Override
    public byte getState() {
        Byte state = tryGetFromLocalCache("state");
        return state == null ? DeviceState.unknown : state;
    }

    @Override
    @SneakyThrows
    public void putState(byte state) {
        localCache.put("state", state);

        executeAsync(redis -> redis.hset(redisKey, "state", state).thenRun(() -> changedListener.accept(false)));

    }

    @Override
    public CompletionStage<Byte> checkState() {
        String serverId = getServerId();
        if (serverId != null) {
            try {
                return plus.getTopic("device:state:check:".concat(serverId))
                        .publish(deviceId)
                        .thenApply((subscribes) -> {
                            if (subscribes <= 0) {
                                //没有任何服务在监听话题，则认为服务已经不可用
                                if (getState() == DeviceState.online) {
                                    log.debug("设备网关服务[{}]未正常运行,设备[{}]下线", serverId, deviceId);
                                    offline();
                                    return DeviceState.offline;
                                }
                            }
                            return getState();
                        });
            } catch (Exception e) {
                log.error("检查设备状态失败", e);
            }
        } else {
            if (getState() == DeviceState.online) {
                log.debug("设备[{}]未注册到任何设备网关服务", deviceId);
                offline();
            }
        }
        return CompletableFuture.completedFuture(getState());
    }

    @Override
    public long getOnlineTime() {
        return Optional.ofNullable(executeSync(redis -> redis.hget(redisKey, "onlineTime")))
                .map(Long.class::cast)
                .orElse(-1L);
    }

    @Override
    public long getOfflineTime() {
        return Optional.ofNullable(executeSync(redis -> redis.hget(redisKey, "offlineTime")))
                .map(Long.class::cast)
                .orElse(-1L);
    }

    @Override
    public void online(String serverId, String sessionId) {
        Map<String, Object> map = new HashMap<>();
        map.put("serverId", serverId);
        map.put("sessionId", sessionId);
        map.put("state", DeviceState.online);
        map.put("onlineTime", System.currentTimeMillis());
        localCache.putAll(map);

        this.<String, Object>executeAsync(redis -> redis.hmset(redisKey, map).thenRun(() -> changedListener.accept(false)));
    }

    @Override
    public void offline() {
        Map<String, Object> map = new HashMap<>();
        map.put("state", DeviceState.offline);
        map.put("offlineTime", System.currentTimeMillis());
        map.put("serverId", "");
        map.put("sessionId", "");
        localCache.putAll(map);

        this.<String, Object>executeAsync(redis -> redis.hmset(redisKey, map).thenRun(() -> changedListener.accept(false)));
    }

    @Override
    public CompletionStage<Boolean> disconnect() {
        DisconnectDeviceMessage message=new DisconnectDeviceMessage();
        message.setDeviceId(deviceId);
        message.setMessageId(IdUtils.newUUID());

        return messageSender
                .send(message)
                .thenApply(DisconnectDeviceMessageReply::isSuccess);
    }

    @Override
    public CompletionStage<AuthenticationResponse> authenticate(AuthenticationRequest request) {
        try {
            return getProtocol()
                    .authenticate(request, this);
        } catch (Throwable e) {
            CompletableFuture<AuthenticationResponse> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    @Override
    public DeviceMetadata getMetadata() {

        Map<String, Object> deviceInfo = this.<String, Object, List<KeyValue<String, Object>>>
                executeSync(redis -> redis.hmget(redisKey, "metadata", "protocol", "productId"))
                .stream()
                .filter(Value::hasValue)
                .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));

        if (deviceInfo.isEmpty()) {
            throw new NullPointerException("设备信息不存在");
        }
        String metaJson = (String) deviceInfo.get("metadata");
        //设备没有单独的元数据，则获取设备产品型号的元数据
        if (metaJson == null || metaJson.isEmpty()) {
            return registry.getProduct((String) deviceInfo.get("productId")).getMetadata();
        }
        return protocolSupports
                .getProtocol((String) deviceInfo.get("protocol"))
                .getMetadataCodec()
                .decode(metaJson);
    }

    private String getProductId() {
        return tryGetFromLocalCache("productId");
    }

    @Override
    public ProtocolSupport getProtocol() {

        String protocol = tryGetFromLocalCache("protocol");

        if (protocol != null) {
            return protocolSupports.getProtocol(protocol);
        } else {
            return Optional.ofNullable(registry.getProduct(getProductId()))
                    .map(DeviceProductOperation::getProtocol)
                    .orElseThrow(() -> new UnsupportedOperationException("设备[" + deviceId + "]未配置协议以及产品信息"));
        }
    }

    @Override
    public DeviceMessageSender messageSender() {

        return messageSender;
    }

    @Override
    public DeviceInfo getDeviceInfo() {
        Object info = tryGetFromLocalCache("info");
        if (info instanceof String) {
            return JSON.parseObject((String) info, DeviceInfo.class);
        }
        if (info instanceof DeviceInfo) {
            return ((DeviceInfo) info);
        }
        log.warn("设备信息反序列化错误:{}", info);
        return null;
    }

    @Override
    public void update(DeviceInfo deviceInfo) {
        Map<String, Object> all = new HashMap<>();
        all.put("info", JSON.toJSONString(deviceInfo));
        localCache.put("info", deviceInfo);
        if (deviceInfo.getProtocol() != null) {
            all.put("protocol", deviceInfo.getProtocol());
            localCache.put("protocol", deviceInfo.getProtocol());
        }
        if (deviceInfo.getProductId() != null) {
            all.put("productId", deviceInfo.getProductId());
            localCache.put("productId", deviceInfo.getProtocol());
        }

        this.<String, Object>executeAsync(redis -> redis.hmset(redisKey, all).thenRun(() -> changedListener.accept(false)));
    }

    @Override
    public void updateMetadata(String metadata) {
        localCache.put("metadata", metadata);

        this.executeAsync(redis -> redis.hset(redisKey, "metadata", metadata)
                .thenRun(() -> changedListener.accept(false)));
    }

    private String createConfigKey(String key) {
        return "_cfg:".concat(key);
    }

    private String recoverConfigKey(String key) {
        return key.substring(5);
    }

    private Map<String, Object> recoverConfigMap(Map<String, Object> source) {
        return source.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> recoverConfigKey(e.getKey()), Map.Entry::getValue));
    }

    @Override
    @SuppressWarnings("all")
    public CompletionStage<Map<String, Object>> getAllAsync(String... key) {
        //获取全部
        if (key.length == 0) {
            Map<String, Object> productConf = registry.getProduct(getProductId()).getAll();
            Map<String, Object> meConf = (Map<String, Object>) localCache.computeIfAbsent("__all", __ ->
                    this.<String, Object, Map<String, Object>>
                            executeSync(redis -> redis.hgetall(redisKey))
                            .entrySet()
                            .stream()
                            .filter(e -> e.getKey().startsWith("_cfg:"))
                            .collect(Collectors.toMap(e -> recoverConfigKey(e.getKey()), Map.Entry::getValue)));

            Map<String, Object> all = new HashMap<>();

            all.putAll(productConf);
            all.putAll(meConf);
            return CompletableFuture.completedFuture(all);
        }

        Set<String> keSet = Stream.of(key)
                .map(this::createConfigKey)
                .collect(Collectors.toSet());

        String cacheKey = String.valueOf(keSet.hashCode());

        Object cache = confCache.get(cacheKey);

        if (cache instanceof Map) {
            return CompletableFuture.completedFuture((Map) cache);
        }
        //null value 直接获取产品配置
        if (cache instanceof NullValue) {
            return registry.getProduct(getProductId()).getAllAsync(key);
        }

        return this.<String, Object>getAsyncRedis()
                .thenCompose(async -> async.hgetall(redisKey))
                .thenApply(all -> all.entrySet().stream()
                        .filter(e -> e.getKey().startsWith("_cfg:"))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                .thenApply(mine -> {
                    if (mine.isEmpty()) {
                        confCache.put(cacheKey, NullValue.instance);
                        return registry
                                .getProduct(getProductId())
                                .getAll(key);
                    }
                    //只有一部分,尝试从产品中获取
                    if (mine.size() != key.length) {
                        String[] inProductKey = keSet
                                .stream()
                                .filter(k -> !mine.containsKey(k))
                                .map(this::recoverConfigKey)
                                .toArray(String[]::new);

                        return Optional.of(registry
                                .getProduct(getProductId())
                                .getAll(inProductKey))
                                .map(productPart -> {
                                    Map<String, Object> minePart = recoverConfigMap(mine);
                                    minePart.putAll(productPart);
                                    return minePart;
                                }).get();
                    }
                    Map<String, Object> recover = Collections.unmodifiableMap(recoverConfigMap(mine));
                    confCache.put(cacheKey, recover);
                    return recover;
                });
    }

    @Override
    @SuppressWarnings("all")
    @SneakyThrows
    public Map<String, Object> getAll(String... key) {
        return getAllAsync(key).toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
    }

    @Override
    public ValueWrapper get(String key) {
        String confKey = createConfigKey(key);
        Object val = tryGetFromConfCache(confKey);

        if (val == null) {
            String productId = getProductId();
            if (null != productId) {
                //获取产品的配置
                return registry.getProduct(productId).get(key);
            }
            return NullValueWrapper.instance;
        }

        return new DefaultValueWrapper(val);
    }

    @Override
    public void put(String key, Object value) {
        Objects.requireNonNull(value, "value");
        String realKey = createConfigKey(key);
        confCache.remove("__all");
        confCache.put(realKey, value);
        this.executeAsync(redis -> redis.hset(redisKey, realKey, value)
                .thenRun(() -> changedListener.accept(true)));
    }

    @Override
    @SuppressWarnings("all")
    public void putAll(Map<String, Object> conf) {
        if (conf == null || conf.isEmpty()) {
            return;
        }
        confCache.remove("__all");
        Map<String, Object> newMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : conf.entrySet()) {
            newMap.put(createConfigKey(entry.getKey()), entry.getValue());
        }

        this.<String, Object>executeAsync(redis -> redis.hmset(redisKey, newMap).thenRun(() -> changedListener.accept(true)));

        confCache.putAll(newMap);
    }

    @Override
    public Object remove(String key) {
        confCache.remove("__all");
        Object val = get(key).value().orElse(null);

        String configKey = createConfigKey(key);

        executeAsync(redis -> redis.hdel(redisKey, configKey));

        confCache.remove(configKey);
        changedListener.accept(true);
        return val;
    }

    void delete() {
        executeAsync(redis -> redis.del(redisKey));
        changedListener.accept(true);
        confCache.clear();
        localCache.clear();
    }

}
