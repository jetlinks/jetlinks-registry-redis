package org.jetlinks.registry.redis;

import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.metadata.DefaultValueWrapper;
import org.jetlinks.core.metadata.DeviceMetadata;
import org.jetlinks.core.metadata.NullValueWrapper;
import org.jetlinks.core.metadata.ValueWrapper;
import org.jetlinks.core.device.DeviceProductInfo;
import org.jetlinks.core.device.DeviceProductOperation;
import org.redisson.api.RMap;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class RedissonDeviceProductOperation implements DeviceProductOperation {

    private RMap<String, Object> rMap;

    private ProtocolSupports protocolSupports;

    public RedissonDeviceProductOperation(RMap<String, Object> rMap, ProtocolSupports protocolSupports) {
        this.rMap = rMap;
        this.protocolSupports = protocolSupports;
    }

    @Override
    public DeviceMetadata getMetadata() {
        return protocolSupports.getProtocol(getInfo().getProtocol())
                .getMetadataCodec()
                .decode((String) rMap.get("metadata"));
    }

    @Override
    public void updateMetadata(String metadata) {
        rMap.fastPut("metadata", metadata);
    }

    @Override
    public DeviceProductInfo getInfo() {
        return (DeviceProductInfo) rMap.get("info");
    }

    @Override
    public void update(DeviceProductInfo info) {
        rMap.fastPut("info", info);
    }

    @Override
    public ProtocolSupport getProtocol() {
        return protocolSupports.getProtocol(getInfo().getProtocol());
    }

    @Override
    public ValueWrapper get(String key) {
        Object conf = rMap.get("_cfg:" + key);
        if (null == conf) {
            return NullValueWrapper.instance;
        }
        return new DefaultValueWrapper(conf);
    }

    @Override
    public void putAll(Map<String, Object> conf) {
        Map<String, Object> newMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : conf.entrySet()) {
            newMap.put("_cfg:" + entry.getKey(), entry.getValue());
        }
        rMap.putAll(newMap);
    }

    @Override
    public void put(String key, Object value) {
        rMap.fastPut("_cfg:" + key, value);
    }

    @Override
    public void remove(String key) {
        rMap.fastRemove("_cfg:" + key);
    }
}
