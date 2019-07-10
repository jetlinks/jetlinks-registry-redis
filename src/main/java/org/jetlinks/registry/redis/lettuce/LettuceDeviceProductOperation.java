package org.jetlinks.registry.redis.lettuce;

import com.alibaba.fastjson.JSON;
import io.lettuce.core.KeyValue;
import io.lettuce.core.Value;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.DeviceProductInfo;
import org.jetlinks.core.device.DeviceProductOperation;
import org.jetlinks.core.metadata.DefaultValueWrapper;
import org.jetlinks.core.metadata.DeviceMetadata;
import org.jetlinks.core.metadata.NullValueWrapper;
import org.jetlinks.core.metadata.ValueWrapper;
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
@SuppressWarnings("all")
public class LettuceDeviceProductOperation implements DeviceProductOperation {

    private Map<String, Object> localCache = new ConcurrentHashMap<>(32);


    private ProtocolSupports protocolSupports;

    private Runnable cacheChangedListener;

    private LettucePlus plus;

    private String redisKey;

    public LettuceDeviceProductOperation(String redisKey,
                                         LettucePlus plus,
                                         ProtocolSupports protocolSupports,
                                         Runnable cacheChangedListener) {
        this.plus = plus;
        this.protocolSupports = protocolSupports;
        this.redisKey = redisKey;
        this.cacheChangedListener = () -> {
            localCache.clear();
            cacheChangedListener.run();
        };
    }

    void clearCache() {
        localCache.clear();
    }

    @Override
    public DeviceMetadata getMetadata() {
        return getProtocol()
                .getMetadataCodec()
                .decode(tryGetFromLocalCache("metadata"));
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

    protected <K, V> CompletionStage<RedisAsyncCommands<K, V>> getAsyncRedis() {
        return plus
                .<K, V>getConnection()
                .thenApply(StatefulRedisConnection::async);
    }

    @Override
    public void updateMetadata(String metadata) {
        executeAsync(redis -> redis.hset(redisKey, "metadata", metadata));

        localCache.put("metadata", metadata);
    }

    @Override
    public DeviceProductInfo getInfo() {
        Object info = tryGetFromLocalCache("info");
        if (info instanceof DeviceProductInfo) {
            return ((DeviceProductInfo) info);
        }

        if (info instanceof String) {
            return JSON.parseObject(((String) info), DeviceProductInfo.class);
        }
        log.warn("设备产品信息反序列化错误:{}", info);
        return null;
    }

    @Override
    public void update(DeviceProductInfo info) {
        Map<String, Object> all = new HashMap<>();
        all.put("info", JSON.toJSONString(info));
        if (info.getProtocol() != null) {
            all.put("protocol", info.getProtocol());
        }
        executeAsync(redis -> redis.hmset(redisKey, (Map) all).thenRun(this.cacheChangedListener::run));

        localCache.putAll(all);
    }

    @Override
    public ProtocolSupport getProtocol() {
        return protocolSupports.getProtocol(tryGetFromLocalCache("protocol"));
    }

    private String createConfigKey(String key) {
        return "_cfg:".concat(key);
    }

    private String recoverConfigKey(String key) {
        return key.substring(5);
    }

    @Override
    public ValueWrapper get(String key) {
        Object conf = tryGetFromLocalCache(createConfigKey(key));
        if (null == conf) {
            return NullValueWrapper.instance;
        }
        return new DefaultValueWrapper(conf);
    }

    @Override
    public CompletionStage<Map<String, Object>> getAllAsync(String... key) {
        if (key.length == 0) {
            if (localCache.containsKey("__all")) {
                return CompletableFuture.completedFuture((Map<String, Object>) localCache.get("__all"));
            } else {
                return this.<String, Object>getAsyncRedis()
                        .thenCompose(redis -> {
                            return redis.hgetall(redisKey);
                        })
                        .thenApply(all -> {
                            return all
                                    .entrySet()
                                    .stream()
                                    .filter(kv -> String.valueOf(kv.getKey()).startsWith("_cfg:"))
                                    .collect(Collectors.toMap(e -> recoverConfigKey(String.valueOf(e.getKey())), Map.Entry::getValue));
                        }).whenComplete((all, error) -> {
                            if (all != null) {
                                localCache.put("__all", all);
                            }
                        });
            }
        }

        Set<String> keSet = Stream.of(key).map(this::createConfigKey).collect(Collectors.toSet());

        String cacheKey = String.valueOf(keSet.hashCode());

        Object cache = localCache.get(cacheKey);

        if (cache instanceof Map) {
            return CompletableFuture.completedFuture((Map) cache);
        }
        if (cache instanceof NullValue) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        return getAsyncRedis()
                .thenCompose(redis -> {
                    return redis.hmget(redisKey, keSet.toArray());
                })
                .thenApply(kv -> kv.stream()
                        .filter(Value::hasValue)
                        .collect(Collectors.toMap(e -> recoverConfigKey(String.valueOf(e.getKey())), KeyValue::getValue, (_1, _2) -> _1)))
                .whenComplete((map, err) -> {
                    if (map != null) {
                        localCache.put(cacheKey, map);
                    }
                });
    }

    @Override
    @SuppressWarnings("all")
    @SneakyThrows
    public Map<String, Object> getAll(String... key) {

        return getAllAsync(key).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Override
    public void putAll(Map<String, Object> conf) {
        if (conf == null || conf.isEmpty()) {
            return;
        }
        Map<String, Object> newMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : conf.entrySet()) {
            newMap.put(createConfigKey(entry.getKey()), entry.getValue());
        }
        localCache.putAll(newMap);

        this.<String, Object>executeAsync(redis -> {
            redis.hmset(redisKey, newMap)
                    .thenRun(this.cacheChangedListener::run);
        });
    }

    @Override
    public void put(String key, Object value) {
        String configKey = createConfigKey(key);
        localCache.put(configKey, value);
        executeAsync(redis -> redis.hset(redisKey, createConfigKey(key), value).thenRun(this.cacheChangedListener::run));
    }

    @Override
    public Object remove(String key) {
        Object val = get(key).value().orElse(null);

        String configKey = createConfigKey(key);
        localCache.remove(configKey);

        executeAsync(redis -> redis.hdel(redisKey, key).thenRun(cacheChangedListener::run));

        return val;
    }
}
