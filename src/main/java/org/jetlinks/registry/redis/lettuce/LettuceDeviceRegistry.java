package org.jetlinks.registry.redis.lettuce;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.DeviceInfo;
import org.jetlinks.core.device.DeviceOperation;
import org.jetlinks.core.device.DeviceState;
import org.jetlinks.core.device.registry.DeviceMessageHandler;
import org.jetlinks.core.device.registry.DeviceRegistry;
import org.jetlinks.core.message.interceptor.DeviceMessageSenderInterceptor;
import org.jetlinks.lettuce.LettucePlus;
import org.jetlinks.lettuce.RedisTopic;
import org.jetlinks.registry.redis.CompositeDeviceMessageSenderInterceptor;

import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class LettuceDeviceRegistry implements DeviceRegistry {

    private LettucePlus plus;

    private ProtocolSupports protocolSupports;

    private Map<String, SoftReference<LettuceDeviceOperation>> localCache = new ConcurrentHashMap<>(1024);

    private Map<String, SoftReference<LettuceDeviceProductOperation>> productLocalCache = new ConcurrentHashMap<>(128);

    private RedisTopic<String> cacheChangedTopic;

    private final CompositeDeviceMessageSenderInterceptor interceptor = new CompositeDeviceMessageSenderInterceptor();

    private DeviceMessageHandler messageHandler;

    public LettuceDeviceRegistry(LettucePlus plus,
                                 DeviceMessageHandler handler,
                                 ProtocolSupports protocolSupports) {
        this.plus = plus;
        this.protocolSupports = protocolSupports;
        this.cacheChangedTopic = plus.getTopic("device:registry:cache:changed");
        this.messageHandler = handler;

        cacheChangedTopic.addListener((t, id) -> {

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
                    .ifPresent(LettuceDeviceProductOperation::clearCache);

        });
    }

    public void addInterceptor(DeviceMessageSenderInterceptor interceptor) {
        this.interceptor.addInterceptor(interceptor);
    }

    @Override
    public LettuceDeviceProductOperation getProduct(String productId) {
        if (productId == null || productId.isEmpty()) {
            return null;
        }
        SoftReference<LettuceDeviceProductOperation> reference = productLocalCache.get(productId);

        if (reference == null || reference.get() == null) {

            productLocalCache.put(productId, reference = new SoftReference<>(doGetProduct(productId)));

        }
        return reference.get();
    }

    @Override
    public LettuceDeviceOperation getDevice(String deviceId) {
        SoftReference<LettuceDeviceOperation> reference = localCache.get(deviceId);

        if (reference == null || reference.get() == null) {
            LettuceDeviceOperation operation = doGetOperation(deviceId);
            //unknown的设备不使用缓存
            if (operation.getState() == DeviceState.unknown) {
                return operation;
            }
            localCache.put(deviceId, reference = new SoftReference<>(operation));

        }
        return reference.get();
    }

    private LettuceDeviceProductOperation doGetProduct(String productId) {
        return new LettuceDeviceProductOperation("product:".concat(productId).concat(":reg")
                , plus
                , protocolSupports,
                () -> cacheChangedTopic.publish(productId.concat("@-1")));
    }

    private LettuceDeviceOperation doGetOperation(String deviceId) {
        return new LettuceDeviceOperation(deviceId, plus,
                protocolSupports,
                messageHandler,
                this,
                interceptor,
                (isConf) -> cacheChangedTopic.publish(deviceId.concat("@").concat(isConf ? "1" : "0")));
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
        LettuceDeviceOperation operation = getDevice(deviceId);
        operation.putState(DeviceState.unknown);

        operation.disconnect()
                .whenComplete((r, err) -> operation.delete());

    }

}
