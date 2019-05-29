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
import org.jetlinks.core.metadata.ValueWrapper;
import org.redisson.api.RFuture;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

import java.util.*;
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

    @Override
    public String getDeviceId() {
        return deviceId;
    }

    @Override
    public String getServerId() {
        return Optional.ofNullable(rMap.get("serverId"))
                .map(String::valueOf)
                .orElse(null);
    }

    @Override
    public String getSessionId() {
        return Optional.ofNullable(rMap.get("sessionId"))
                .map(String::valueOf)
                .orElse(null);
    }

    @Override
    public byte getState() {
        Byte state = (Byte) rMap.get("state");
        return state == null ? DeviceState.unknown : state;
    }

    @Override
    @SneakyThrows
    public void putState(byte state) {
        execute(rMap.fastPutAsync("state", state));
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
                    boolean success = redissonClient
                            .getSemaphore("device:state:check:semaphore:".concat(deviceId))
                            .tryAcquire((int) subscribes, 2, TimeUnit.SECONDS);
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
    }

    @Override
    public void offline() {
        rMap.put("offlineTime", System.currentTimeMillis());
        putState(DeviceState.offline);
        execute(rMap.fastRemoveAsync("serverId", "sessionId"));
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

    @Override
    public ProtocolSupport getProtocol() {

        Map<String, Object> all = rMap.getAll(new HashSet<>(Arrays.asList("protocol", "productId")));
        String protocol = (String) all.get("protocol");

        if (protocol != null) {
            return protocolSupports.getProtocol(protocol);
        } else {

            return registry.getProduct( (String) all.get("productId"))
                    .getProtocol();
        }
    }

    @Override
    public DeviceMessageSender messageSender() {
        return new RedissonDeviceMessageSender(deviceId, redissonClient, this::getServerId, this::checkState);
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
    }

    @Override
    public void updateMetadata(String metadata) {
        rMap.fastPut("metadata", metadata);
    }

    private String createConfigKey(String key) {
        return "_cfg:".concat(key);
    }

    @Override
    public ValueWrapper get(String key) {
        String confKey = createConfigKey(key);

        Map<String, Object> conf = rMap.getAll(new HashSet<>(Arrays.asList(confKey, "productId")));
        String val = (String) conf.get(confKey);
        String productId = (String) conf.get("productId");

        if (null == val && null != productId) {
            //获取产品的配置
            return registry
                    .getProduct(productId)
                    .get(key);
        }
        return new DefaultValueWrapper(val);
    }

    @Override
    public void put(String key, Object value) {
        rMap.fastPut(createConfigKey(key), value);
    }

    @Override
    public void putAll(Map<String, Object> conf) {
        Map<String, Object> newMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : conf.entrySet()) {
            newMap.put(createConfigKey(entry.getKey()), entry.getValue());
        }
        rMap.putAll(newMap);
    }

    @Override
    public void remove(String key) {
        rMap.fastRemove(createConfigKey(key));
    }

    public void delete() {
        rMap.delete();
    }

}
