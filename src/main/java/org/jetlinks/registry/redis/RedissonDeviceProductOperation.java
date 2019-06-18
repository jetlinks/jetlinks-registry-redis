package org.jetlinks.registry.redis;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.DeviceProductInfo;
import org.jetlinks.core.device.DeviceProductOperation;
import org.jetlinks.core.metadata.DefaultValueWrapper;
import org.jetlinks.core.metadata.DeviceMetadata;
import org.jetlinks.core.metadata.NullValueWrapper;
import org.jetlinks.core.metadata.ValueWrapper;
import org.redisson.api.RMap;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
@SuppressWarnings("all")
public class RedissonDeviceProductOperation implements DeviceProductOperation {

    private RMap<String, Object> rMap;

    private Map<String, Object> localCache = new ConcurrentHashMap<>(32);


    private ProtocolSupports protocolSupports;

    private Runnable cacheChangedListener;


    public RedissonDeviceProductOperation(RMap<String, Object> rMap, ProtocolSupports protocolSupports, Runnable cacheChangedListener) {
        this.rMap = rMap;
        this.protocolSupports = protocolSupports;
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
        Object val = localCache.computeIfAbsent(key, k -> Optional.ofNullable(rMap.get(k)).orElse(NullValue.instance));
        if (val == NullValue.instance) {
            return null;
        }
        return (T) val;
    }

    @Override
    public void updateMetadata(String metadata) {
        rMap.fastPut("metadata", metadata);
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
        rMap.putAll(all);
        localCache.putAll(all);
        cacheChangedListener.run();
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
    @SuppressWarnings("all")
    public Map<String, Object> getAll(String... key) {

        //获取全部
        if (key.length == 0) {
            return (Map<String, Object>) localCache.computeIfAbsent("__all", __ -> {
                return rMap.entrySet().stream()
                        .filter(e -> e.getKey().startsWith("_cfg:"))
                        .collect(Collectors.toMap(e -> recoverConfigKey(e.getKey()), Map.Entry::getValue));
            });
        }

        Set<String> keSet = Stream.of(key).map(this::createConfigKey).collect(Collectors.toSet());

        String cacheKey = String.valueOf(keSet.hashCode());

        Object cache = localCache.get(cacheKey);

        if (cache instanceof Map) {
            return (Map) cache;
        }
        if (cache instanceof NullValue) {
            return Collections.emptyMap();
        }
        Map<String, Object> inRedis = Collections.unmodifiableMap(rMap.getAll(keSet)
                .entrySet()
                .stream()
                .collect(Collectors.toMap(e -> recoverConfigKey(e.getKey()), Map.Entry::getValue, (_1, _2) -> _1)));

        localCache.put(cacheKey, inRedis);

        return inRedis;
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
        rMap.putAll(newMap);
        cacheChangedListener.run();
    }

    @Override
    public void put(String key, Object value) {
        rMap.fastPut(createConfigKey(key), value);
        cacheChangedListener.run();
    }

    @Override
    public Object remove(String key) {
        Object val = rMap.fastRemove(createConfigKey(key));
        cacheChangedListener.run();
        return val;
    }
}
