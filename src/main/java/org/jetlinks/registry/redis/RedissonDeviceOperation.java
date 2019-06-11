package org.jetlinks.registry.redis;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.*;
import org.jetlinks.core.device.registry.DeviceMessageHandler;
import org.jetlinks.core.device.registry.DeviceRegistry;
import org.jetlinks.core.message.interceptor.DeviceMessageSenderInterceptor;
import org.jetlinks.core.metadata.DefaultValueWrapper;
import org.jetlinks.core.metadata.DeviceMetadata;
import org.jetlinks.core.metadata.NullValueWrapper;
import org.jetlinks.core.metadata.ValueWrapper;
import org.redisson.api.RFuture;
import org.redisson.api.RMap;
import org.redisson.api.RSemaphore;
import org.redisson.api.RedissonClient;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class RedissonDeviceOperation implements DeviceOperation {

    private RedissonClient redissonClient;

    private RMap<String, Object> rMap;

    private Map<String, Object> localCache = new ConcurrentHashMap<>(32);
    private Map<String, Object> confCache = new ConcurrentHashMap<>(32);

    private ProtocolSupports protocolSupports;

    private DeviceRegistry registry;

    private String deviceId;

    private Consumer<Boolean> changedListener;

    @Setter
    @Getter
    private DeviceMessageSenderInterceptor interceptor;

    private DeviceMessageHandler deviceMessageHandler;

    public RedissonDeviceOperation(String deviceId,
                                   RedissonClient redissonClient,
                                   RMap<String, Object> rMap,
                                   ProtocolSupports protocolSupports,
                                   DeviceMessageHandler deviceMessageHandler,
                                   DeviceRegistry registry,
                                   Consumer<Boolean> changedListener) {
        this.deviceId = deviceId;
        this.redissonClient = redissonClient;
        this.rMap = rMap;
        this.protocolSupports = protocolSupports;
        this.registry = registry;
        this.deviceMessageHandler = deviceMessageHandler;
        this.changedListener = (isConf) -> {
            clearCache(isConf);
            changedListener.accept(isConf);
        };
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

    @SuppressWarnings("all")
    private <T> T tryGetFromLocalCache(String key) {
        Object val = localCache.computeIfAbsent(key, k -> Optional.ofNullable(rMap.get(k)).orElse(NullValue.instance));
        if (val == NullValue.instance) {
            return null;
        }
        return (T) val;
    }

    private <T> T tryGetFromConfCache(String key) {
        Object val = confCache.computeIfAbsent(key, k -> Optional.ofNullable(rMap.get(k)).orElse(NullValue.instance));
        if (val == NullValue.instance) {
            return null;
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

        execute(rMap.fastPutAsync("state", state));

        changedListener.accept(false);
    }

    private void execute(RFuture<?> future) {
        try {
            //无论成功失败,最多等待一秒
            future.await(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void checkState() {
        String serverId = getServerId();
        if (serverId != null) {
            long subscribes = redissonClient
                    .getTopic("device:state:check:".concat(serverId))
                    .publish(deviceId);
            if (subscribes <= 0) {
                //没有任何服务在监听话题，则认为服务已经不可用
                if (getState() == DeviceState.online) {
                    log.debug("设备网关服务[{}]未正常运行,设备[{}]下线", serverId, deviceId);
                    offline();
                }
            } else {
                //等待检查返回,检查是异步的,需要等待检查完成的信号
                //一般检查速度很快,所以这里超过2秒则超时,继续执行接下来的逻辑
                try {
                    RSemaphore semaphore = redissonClient
                            .getSemaphore("device:state:check:semaphore:".concat(deviceId));
                    semaphore.expireAsync(5, TimeUnit.SECONDS);
                    boolean success = semaphore.tryAcquire((int) subscribes, 2, TimeUnit.SECONDS);
                    semaphore.deleteAsync();
                    if (!success) {
                        log.debug("设备[{}]状态检查超时,设备网关服务:[{}]", deviceId, serverId);
                    }
                } catch (InterruptedException ignore) {
                }
            }
        } else {
            if (getState() == DeviceState.online) {
                log.debug("设备[{}]未注册到任何设备网关服务", deviceId);
                offline();
            }
        }
    }

    @Override
    public long getOnlineTime() {
        return Optional.ofNullable(rMap.get("onlineTime"))
                .map(Long.class::cast)
                .orElse(-1L);
    }

    @Override
    public long getOfflineTime() {
        return Optional.ofNullable(rMap.get("offlineTime"))
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

        execute(rMap.putAllAsync(map));
        changedListener.accept(false);
    }

    @Override
    public void offline() {
        Map<String, Object> map = new HashMap<>();
        map.put("state", DeviceState.offline);
        map.put("offlineTime", System.currentTimeMillis());
        map.put("serverId", "");
        map.put("sessionId", "");
        localCache.putAll(map);

        execute(rMap.putAllAsync(map));
        changedListener.accept(false);
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

        Map<String, Object> deviceInfo = rMap.getAll(new HashSet<>(Arrays.asList("metadata", "protocol", "productId")));

        if (deviceInfo == null || deviceInfo.isEmpty()) {
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
        RedissonDeviceMessageSender sender = new RedissonDeviceMessageSender(deviceId, redissonClient, deviceMessageHandler, this);
        sender.setInterceptor(interceptor);
        return sender;
    }

    @Override
    public DeviceInfo getDeviceInfo() {
        Object info = rMap.get("info");
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
        if (deviceInfo.getProtocol() != null) {
            all.put("protocol", deviceInfo.getProtocol());
        }
        if (deviceInfo.getProductId() != null) {
            all.put("productId", deviceInfo.getProductId());
        }
        execute(rMap.putAllAsync(all));
        changedListener.accept(false);
    }

    @Override
    public void updateMetadata(String metadata) {
        changedListener.accept(false);
        rMap.fastPut("metadata", metadata);
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
    public Map<String, Object> getAll(String... key) {

        Set<String> keSet = Stream.of(key)
                .map(this::createConfigKey)
                .collect(Collectors.toSet());

        String cacheKey = String.valueOf(keSet.hashCode());

        Object cache = confCache.get(cacheKey);

        if (cache instanceof Map) {
            return (Map) cache;
        }
        //null value 直接获取产品配置
        if (cache instanceof NullValue) {
            return registry.getProduct(getProductId()).getAll(key);
        }
        return Optional.of(rMap
                .getAll(keSet))
                .map(mine -> {
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
                }).get();
    }

    @Override
    @SuppressWarnings("all")
    public CompletionStage<Map<String, Object>> getAllAsync(String... key) {

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
        return rMap
                .getAllAsync(keSet)
                .thenCompose(mine -> {
                    if (mine.isEmpty()) {
                        confCache.put(cacheKey, NullValue.instance);
                        return registry
                                .getProduct(getProductId())
                                .getAllAsync(key);
                    }
                    //只有一部分,尝试从产品中获取
                    if (mine.size() != key.length) {
                        String[] inProductKey = keSet
                                .stream()
                                .filter(k -> !mine.containsKey(k))
                                .map(this::recoverConfigKey)
                                .toArray(String[]::new);

                        return registry
                                .getProduct(getProductId())
                                .getAllAsync(inProductKey)
                                .thenApply(productPart -> {
                                    Map<String, Object> minePart = recoverConfigMap(mine);
                                    minePart.putAll(productPart);
                                    return minePart;
                                });
                    }
                    Map<String, Object> recover = recoverConfigMap(mine);
                    confCache.put(cacheKey, recover);
                    return CompletableFuture.completedFuture(recover);
                });
    }

    @Override
    public ValueWrapper get(String key) {
        String confKey = createConfigKey(key);
        Object val = tryGetFromConfCache(confKey);

        if (val == null) {
            String productId = getProductId();
            if (null != productId) {
                //获取产品的配置
                return registry
                        .getProduct(productId)
                        .get(key);
            }
            return NullValueWrapper.instance;
        }

        return new DefaultValueWrapper(val);
    }

    @Override
    public void put(String key, Object value) {
        rMap.fastPut(key = createConfigKey(key), value);
        confCache.put(key, value);
        changedListener.accept(true);
    }

    @Override
    public void putAll(Map<String, Object> conf) {
        Map<String, Object> newMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : conf.entrySet()) {
            newMap.put(createConfigKey(entry.getKey()), entry.getValue());
        }
        rMap.putAll(newMap);
        confCache.putAll(newMap);
        changedListener.accept(true);
    }

    @Override
    public void remove(String key) {
        rMap.fastRemove(key = createConfigKey(key));
        confCache.remove(key);
        changedListener.accept(true);
    }

    void delete() {
        changedListener.accept(true);
        rMap.delete();
        confCache.clear();
        localCache.clear();

    }

}
