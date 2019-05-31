package org.jetlinks.registry.redis;

import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.*;
import org.jetlinks.core.device.registry.DeviceRegistry;
import org.jetlinks.core.metadata.DefaultValueWrapper;
import org.jetlinks.core.metadata.DeviceMetadata;
import org.jetlinks.core.metadata.NullValueWrapper;
import org.jetlinks.core.metadata.ValueWrapper;
import org.redisson.api.RFuture;
import org.redisson.api.RMap;
import org.redisson.api.RSemaphore;
import org.redisson.api.RedissonClient;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class RedissonDeviceOperation implements DeviceOperation {

    private RedissonClient redissonClient;

    private RMap<String, Object> rMap;

    private Map<String, Object> localCache = new ConcurrentHashMap<>(32);

    private ProtocolSupports protocolSupports;

    private DeviceRegistry registry;

    private String deviceId;

    private Runnable changedListener;

    public RedissonDeviceOperation(String deviceId,
                                   RedissonClient redissonClient,
                                   RMap<String, Object> rMap,
                                   ProtocolSupports protocolSupports,
                                   DeviceRegistry registry,
                                   Runnable changedListener) {
        this.deviceId = deviceId;
        this.redissonClient = redissonClient;
        this.rMap = rMap;
        this.protocolSupports = protocolSupports;
        this.registry = registry;
        this.changedListener = () -> {
            clearCache();
            changedListener.run();
        };
    }

    void clearCache() {
        localCache.clear();
    }

    @Override
    public String getDeviceId() {
        return deviceId;
    }


    @Override
    public String getServerId() {
        return (String) localCache.computeIfAbsent("serverId", rMap::get);
    }

    @Override
    public String getSessionId() {
        return (String) localCache.computeIfAbsent("sessionId", rMap::get);
    }

    @Override
    public byte getState() {
        Byte state = (Byte) localCache.computeIfAbsent("state", rMap::get);
        return state == null ? DeviceState.unknown : state;
    }

    @Override
    @SneakyThrows
    public void putState(byte state) {
        execute(rMap.fastPutAsync("state", state));
        changedListener.run();
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
                        log.warn("device state check time out!");
                    }
                } catch (InterruptedException e) {
                    log.warn("wait check device state error", e);
                }
            }
        } else {
            if (getState() == DeviceState.online) {
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
        execute(rMap.putAllAsync(map));
        changedListener.run();
    }

    @Override
    public void offline() {
        rMap.put("offlineTime", System.currentTimeMillis());
        putState(DeviceState.offline);
        execute(rMap.fastRemoveAsync("serverId", "sessionId"));
        changedListener.run();
    }

    @Override
    public AuthenticationResponse authenticate(AuthenticationRequest request) {
        return getProtocol().authenticate(request, this);
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
        return (String) localCache.computeIfAbsent("productId", rMap::get);
    }

    @Override
    public ProtocolSupport getProtocol() {

        String protocol = (String) localCache.computeIfAbsent("protocol", rMap::get);

        if (protocol != null) {
            return protocolSupports.getProtocol(protocol);
        } else {
            return registry.getProduct(getProductId()).getProtocol();
        }
    }

    @Override
    public DeviceMessageSender messageSender() {
        return new RedissonDeviceMessageSender(deviceId, redissonClient, this);
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
        changedListener.run();
    }

    @Override
    public void updateMetadata(String metadata) {
        changedListener.run();
        rMap.fastPut("metadata", metadata);
    }

    private String createConfigKey(String key) {
        return "_cfg:".concat(key);
    }

    @Override
    public ValueWrapper get(String key) {
        String confKey = createConfigKey(key);
        Object val = localCache.computeIfAbsent(confKey, rMap::get);

        if (val == null) {
            String productId = (String) rMap.get("productId");
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
        rMap.fastPut(createConfigKey(key), value);
        changedListener.run();
    }

    @Override
    public void putAll(Map<String, Object> conf) {
        Map<String, Object> newMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : conf.entrySet()) {
            newMap.put(createConfigKey(entry.getKey()), entry.getValue());
        }
        rMap.putAll(newMap);
        changedListener.run();
    }

    @Override
    public void remove(String key) {
        changedListener.run();
        rMap.fastRemove(createConfigKey(key));
    }

    public void delete() {
        changedListener.run();
        rMap.delete();
    }

}
