package org.jetlinks.registry.redis;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.DeviceInfo;
import org.jetlinks.core.device.DeviceOperation;
import org.jetlinks.core.device.DeviceProductOperation;
import org.jetlinks.core.device.DeviceState;
import org.jetlinks.core.device.registry.DeviceMessageHandler;
import org.jetlinks.core.device.registry.DeviceRegistry;
import org.jetlinks.core.message.interceptor.DeviceMessageSenderInterceptor;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class RedissonDeviceRegistry implements DeviceRegistry {

    private RedissonClient client;

    private ProtocolSupports protocolSupports;

    private Map<String, SoftReference<RedissonDeviceOperation>> localCache = new ConcurrentHashMap<>(1024);

    private Map<String, SoftReference<RedissonDeviceProductOperation>> productLocalCache = new ConcurrentHashMap<>(128);

    private RTopic cacheChangedTopic;

    private final CompositeDeviceMessageSenderInterceptor interceptor = new CompositeDeviceMessageSenderInterceptor();

    private DeviceMessageHandler messageHandler;

    public RedissonDeviceRegistry(RedissonClient client,
                                  DeviceMessageHandler handler,
                                  ProtocolSupports protocolSupports) {
        this.client = client;
        this.protocolSupports = protocolSupports;
        this.cacheChangedTopic = client.getTopic("device:registry:cache:changed", StringCodec.INSTANCE);
        this.messageHandler = handler;

        cacheChangedTopic.addListener(String.class, (t, id) -> {

            String[] split = id.split("[@]");
            byte clearType = 1;
            if (split.length == 2) {
                id = split[0];
                clearType = Byte.valueOf(split[1]);
            } else if (split.length > 2) {
                log.warn("本地缓存可能出错,id[{}]不合法", id);
            }
            boolean clearConf = clearType == 1;

            Optional.ofNullable(localCache.get(id))
                    .map(SoftReference::get)
                    .ifPresent(cache -> cache.clearCache(clearConf));

            Optional.ofNullable(productLocalCache.get(id))
                    .map(SoftReference::get)
                    .ifPresent(RedissonDeviceProductOperation::clearCache);

        });
    }


    public void addInterceptor(DeviceMessageSenderInterceptor interceptor) {
        this.interceptor.addInterceptor(interceptor);
    }

    @Override
    public DeviceProductOperation getProduct(String productId) {
        if (productId == null || productId.isEmpty()) {
            return null;
        }
        SoftReference<RedissonDeviceProductOperation> reference = productLocalCache.get(productId);

        if (reference == null || reference.get() == null) {

            productLocalCache.put(productId, reference = new SoftReference<>(doGetProduct(productId)));

        }
        return reference.get();
    }

    @Override
    public RedissonDeviceOperation getDevice(String deviceId) {
        SoftReference<RedissonDeviceOperation> reference = localCache.get(deviceId);

        if (reference == null || reference.get() == null) {
            RedissonDeviceOperation operation = doGetOperation(deviceId);
            //unknown的设备不使用缓存
            if (operation.getState() == DeviceState.unknown) {
                return operation;
            }
            localCache.put(deviceId, reference = new SoftReference<>(operation));

        }
        return reference.get();
    }

    private RedissonDeviceProductOperation doGetProduct(String productId) {
        return new RedissonDeviceProductOperation(client.getMap("product:".concat(productId).concat(":reg"))
                , protocolSupports,
                () -> cacheChangedTopic.publishAsync(productId.concat("@-1")));
    }

    private RedissonDeviceOperation doGetOperation(String deviceId) {
        RedissonDeviceOperation operation = new RedissonDeviceOperation(deviceId, client,
                client.getMap(deviceId.concat(":reg")),
                protocolSupports,
                messageHandler,
                this, (isConf) -> cacheChangedTopic.publishAsync(deviceId.concat("@").concat(isConf ? "1" : "0")));
        operation.setInterceptor(interceptor);

        return operation;
    }

    @Override
    public DeviceOperation registry(DeviceInfo deviceInfo) {
        DeviceOperation operation = getDevice(deviceInfo.getId());
        operation.update(deviceInfo);
        operation.putState(DeviceState.offline);
        return operation;
    }

    @Override
    public void unRegistry(String deviceId) {
        getDevice(deviceId).delete();
    }

}
