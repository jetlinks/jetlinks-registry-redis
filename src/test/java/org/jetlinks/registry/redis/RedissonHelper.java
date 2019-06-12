package org.jetlinks.registry.redis;

import lombok.SneakyThrows;
import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.concurrent.*;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class RedissonHelper {

    public static RedissonClient newRedissonClient() {
        Config config = new Config();
        config.useSingleServer()
                .setAddress(System.getProperty("redis.host", "redis://127.0.0.1:6379"))
                .setDatabase(0)
                .setTimeout(10000)
                .setRetryAttempts(1000)
                .setRetryInterval(10)
                .setConnectionPoolSize(1024)
                .setConnectTimeout(10000);
        config.setThreads(32);
        config.setNettyThreads(32);

        return Redisson.create(config);
    }

    @SneakyThrows
    public static void main(String[] args) {
        RedissonClient client = newRedissonClient();
        ExecutorService executorService = Executors.newFixedThreadPool(32);

        RMap<String, String> map = client.getMap("test-map");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");

        CountDownLatch latch = new CountDownLatch(1000);
        long startWith = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {
            CompletableFuture
                    .supplyAsync(() -> map.get("key1"), executorService)
                    .thenRun(latch::countDown);
        }
        System.out.println("executorService:" + (System.currentTimeMillis() - startWith) + "ms");
        latch.await();

        CountDownLatch latch2 = new CountDownLatch(1000);
        startWith = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            map.getAsync("key1")
                    .thenRun(latch2::countDown);
        }
        latch2.await();
        System.out.println("async:" + (System.currentTimeMillis() - startWith) + "ms");
        executorService.shutdown();
        client.shutdown();
    }
}
