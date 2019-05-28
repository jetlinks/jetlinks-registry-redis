package org.jetlinks.registry.redis;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.TransportMode;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class RedissonHelper {

    public static RedissonClient newRedissonClient() {
        Config config = new Config();
        config.useSingleServer()
                .setAddress(System.getProperty("redis.host", "redis://127.0.0.1:6379"))
                .setDatabase(0);
//        config.setTransportMode(TransportMode.EPOLL);

        return Redisson.create(config);
    }
}
