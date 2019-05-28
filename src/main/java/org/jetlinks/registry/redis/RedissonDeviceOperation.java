package org.jetlinks.registry.redis;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.*;
import org.jetlinks.core.metadata.DefaultValueWrapper;
import org.jetlinks.core.metadata.DeviceMetadata;
import org.jetlinks.core.metadata.ValueWrapper;
import org.jetlinks.core.device.registry.DeviceRegistry;
import org.redisson.api.RFuture;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class RedissonDeviceOperation implements DeviceOperation {

    private RedissonClient redissonClient;

    private RMap<String, Object> rMap;

    private ProtocolSupports protocolSupports;

    private DeviceRegistry registry;

    private String deviceId;

    public RedissonDeviceOperation(String deviceId,
                                   RedissonClient redissonClient,
                                   RMap<String, Object> rMap,
                                   ProtocolSupports protocolSupports,
                                   DeviceRegistry registry) {
        this.deviceId = deviceId;
        this.redissonClient = redissonClient;
        this.rMap = rMap;
        this.protocolSupports = protocolSupports;
        this.registry = registry;
    }

    @Getter
    private volatile long lastOperationTime = System.currentTimeMillis();

    private void setLastOperationTime() {
        this.lastOperationTime = System.currentTimeMillis();
    }

    @Override
    public String getServerId() {
        setLastOperationTime();
        return Optional.ofNullable(rMap.get("serverId"))
                .map(String::valueOf)
                .orElse(null);
    }

    @Override
    public String getSessionId() {
        setLastOperationTime();
        return Optional.ofNullable(rMap.get("sessionId"))
                .map(String::valueOf)
                .orElse(null);
    }

    @Override
    public byte getState() {
        setLastOperationTime();
        Byte state = (Byte) rMap.get("state");
        return state == null ? DeviceState.unknown : state;
    }

    @Override
    @SneakyThrows
    public void putState(byte state) {
        setLastOperationTime();
        execute(rMap.fastPutAsync("state", state));
    }

    protected void execute(RFuture<?> future) {
        setLastOperationTime();
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
                //一般检查速度很快,所以这里超过1秒则超时,继续执行接下来的逻辑
                try {
                    boolean success = redissonClient
                            .getSemaphore("device:state:check:semaphore:".concat(deviceId))
                            .tryAcquire((int)subscribes,1, TimeUnit.SECONDS);
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
    public void ping() {
    }

    @Override
    public long getLastPingTime() {
        setLastOperationTime();
        return Optional.ofNullable(rMap.get("lastPingTime"))
                .map(Long.class::cast)
                .orElse(-1L);
    }

    @Override
    public long getOnlineTime() {
        setLastOperationTime();
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
        setLastOperationTime();
        Map<String, Object> map = new HashMap<>();
        map.put("serverId", serverId);
        map.put("sessionId", sessionId);
        map.put("state", DeviceState.online);
        map.put("onlineTime", System.currentTimeMillis());
        execute(rMap.putAllAsync(map));
    }

    @Override
    public void offline() {
        setLastOperationTime();
        rMap.put("offlineTime", System.currentTimeMillis() + 1000);
        putState(DeviceState.offline);
        execute(rMap.fastRemoveAsync("serverId", "sessionId"));
    }

    @Override
    public AuthenticationResponse authenticate(AuthenticationRequest request) {
        setLastOperationTime();
        return protocolSupports
                .getProtocol(getDeviceInfo().getProtocol())
                .authenticate(request, this);
    }

    @Override
    public DeviceMetadata getMetadata() {
        setLastOperationTime();
        String metaJson = (String) rMap.get("metadata");
        DeviceInfo deviceInfo = getDeviceInfo();
        if (deviceInfo == null) {
            throw new UnsupportedOperationException("设备信息不存在");
        }
        //设备没有单独的元数据，则获取设备产品型号的元数据
        if (metaJson == null) {
            return registry.getProduct(deviceInfo.getProductId())
                    .getMetadata();
        }
        return protocolSupports
                .getProtocol(deviceInfo.getProtocol())
                .getMetadataCodec()
                .decode(metaJson);
    }

    @Override
    public DeviceMessageSender messageSender() {
        setLastOperationTime();
        return new RedissonDeviceMessageSender(deviceId, redissonClient, this::getServerId, this::checkState);
    }

    @Override
    public DeviceInfo getDeviceInfo() {
        setLastOperationTime();
        return (DeviceInfo) rMap.get("info");
    }

    @Override
    public void update(DeviceInfo deviceInfo) {
        setLastOperationTime();
        execute(rMap.fastPutAsync("info", deviceInfo));
    }

    @Override
    public void updateMetadata(String metadata) {
        rMap.fastPut("metadata", metadata);
    }

    @Override
    public ValueWrapper get(String key) {
        Object conf = rMap.get("_cfg:".concat(key));
        if (conf == null) {
            //获取产品的配置
            return registry
                    .getProduct(getDeviceInfo().getProductId())
                    .get(key);
        }
        return new DefaultValueWrapper(conf);
    }

    @Override
    public void put(String key, Object value) {
        setLastOperationTime();
        rMap.fastPut("_cfg:".concat(key), value);
    }

    @Override
    public void putAll(Map<String, Object> conf) {
        setLastOperationTime();
        Map<String, Object> newMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : conf.entrySet()) {
            newMap.put("_cfg:".concat(entry.getKey()), entry.getValue());
        }
        rMap.putAll(newMap);
    }

    @Override
    public void remove(String key) {
        setLastOperationTime();
        rMap.fastRemove("_cfg:".concat(key));
    }

    public void delete() {
        rMap.delete();
    }

}
