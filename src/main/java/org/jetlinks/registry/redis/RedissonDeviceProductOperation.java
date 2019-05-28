package org.jetlinks.registry.redis;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class RedissonDeviceProductOperation implements DeviceProductOperation {

    private RMap<String, Object> rMap;

    private ProtocolSupports protocolSupports;

    public RedissonDeviceProductOperation(RMap<String, Object> rMap, ProtocolSupports protocolSupports) {
        this.rMap = rMap;
        this.protocolSupports = protocolSupports;
    }

    @Override
    public DeviceMetadata getMetadata() {
        return getProtocol()
                .getMetadataCodec()
                .decode((String) rMap.get("metadata"));
    }

    @Override
    public void updateMetadata(String metadata) {
        rMap.fastPut("metadata", metadata);
    }

    @Override
    public DeviceProductInfo getInfo() {

        Object info = rMap.get("info");
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
    }

    @Override
    public ProtocolSupport getProtocol() {
        String protocol = (String) rMap.get("protocol");
        return protocolSupports.getProtocol(protocol);
    }

    private String buildConfigKey(String key) {
        return "_cfg:".concat(key);
    }

    @Override
    public ValueWrapper get(String key) {
        Object conf = rMap.get(buildConfigKey(key));
        if (null == conf) {
            return NullValueWrapper.instance;
        }
        return new DefaultValueWrapper(conf);
    }

    @Override
    public void putAll(Map<String, Object> conf) {
        Map<String, Object> newMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : conf.entrySet()) {
            newMap.put(buildConfigKey(entry.getKey()), entry.getValue());
        }
        rMap.putAll(newMap);
    }

    @Override
    public void put(String key, Object value) {
        rMap.fastPut(buildConfigKey(key), value);
    }

    @Override
    public void remove(String key) {
        rMap.fastRemove(buildConfigKey(key));
    }
}
