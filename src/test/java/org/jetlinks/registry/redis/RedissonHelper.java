package org.jetlinks.registry.redis;

import lombok.SneakyThrows;
import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
                .setConnectionPoolSize(1024)
                .setConnectTimeout(10000);
        config.setThreads(32);
        config.setNettyThreads(32);

        return Redisson.create(config);
    }

    @SneakyThrows
    public static void main(String[] args) {
        RedissonClient client = newRedissonClient();
        ExecutorService executorService= Executors.newFixedThreadPool(32);

        RMap<String, Object> map = client.getMap("test-map");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        CountDownLatch latch = new CountDownLatch(100000);
        for (int i = 0; i < 100000; i++) {
            int fi = i;
            executorService.submit(()->{

               return map.getAsync("key1")
                        .whenComplete((val, err) -> {
                            System.out.println(val + " => " + fi + " =>" + Thread.currentThread().getName());

                            if (null != err) {
                                err.printStackTrace();
                            }
                            latch.countDown();

                        }).toCompletableFuture()
                        .get(10, TimeUnit.SECONDS);
            });

        }
        latch.await();
        client.shutdown();
    }
}
