package org.jetlinks.registry.redis;

import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.DeviceInfo;
import org.jetlinks.core.device.DeviceOperation;
import org.jetlinks.core.device.DeviceProductOperation;
import org.jetlinks.core.device.DeviceState;
import org.jetlinks.core.device.registry.DeviceRegistry;
import org.redisson.api.RedissonClient;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class RedissonDeviceRegistry implements DeviceRegistry {

    private RedissonClient client;

    private ProtocolSupports protocolSupports;


    public RedissonDeviceRegistry(RedissonClient client,
                                  ProtocolSupports protocolSupports) {
        this.client = client;
        this.protocolSupports = protocolSupports;
    }


    @Override
    public DeviceProductOperation getProduct(String productId) {
        return new RedissonDeviceProductOperation(client.getMap("product:".concat(productId).concat(":reg")), protocolSupports);
    }

    @Override
    public RedissonDeviceOperation getDevice(String deviceId) {
        return new RedissonDeviceOperation(deviceId, client,
                client.getMap(deviceId.concat(":reg")),
                protocolSupports,
                this);
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
