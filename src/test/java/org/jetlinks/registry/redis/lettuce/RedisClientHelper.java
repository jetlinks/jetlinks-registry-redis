package org.jetlinks.registry.redis.lettuce;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;

public class RedisClientHelper {


    public static RedisClient createRedisClient() {
        RedisURI uri = RedisURI.create(System.getProperty("redis.host", "redis://127.0.0.1:6379"));
        uri.setDatabase(8);

        return RedisClient.create(uri);
    }


}
