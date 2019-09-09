package org.jetlinks.registry.redis.lettuce;

import io.vavr.control.Try;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.*;
import org.jetlinks.core.device.registry.DeviceMessageHandler;
import org.jetlinks.core.message.CommonDeviceMessageReply;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.DeviceMessageReply;
import org.jetlinks.core.message.interceptor.DeviceMessageSenderInterceptor;
import org.jetlinks.lettuce.LettucePlus;
import org.jetlinks.lettuce.supports.DefaultLettucePlus;
import org.jetlinks.registry.redis.MockProtocolSupports;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;


/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class LettuceDeviceRegistryTest {

    private LettuceDeviceRegistry registry;

    private DeviceMessageHandler messageHandler;
    private LettucePlus client;

    @Before
    public void init() {
        client = DefaultLettucePlus.standalone(RedisClientHelper.createRedisClient());

        messageHandler = new LettuceDeviceMessageHandler(client);

        registry = new LettuceDeviceRegistry(client, messageHandler, new MockProtocolSupports());

        registry.addInterceptor(new DeviceMessageSenderInterceptor() {
            @Override
            public DeviceMessage preSend(DeviceOperation device, DeviceMessage message) {
                return message;
            }

            @Override
            public <R extends DeviceMessageReply> CompletionStage<R> afterReply(DeviceOperation device, DeviceMessage message, R reply) {
                return CompletableFuture.supplyAsync(() -> {
                    log.debug("reply Interceptor :{}", reply);
                    return reply;
                });
            }
        });
    }

    @SneakyThrows
    @After
    public void cleanDb() {
        client.getConnection()
                .toCompletableFuture()
                .get()
                .sync()
                .flushdb();
        client.shutdown();
    }


    public DeviceInfo newDeviceInfo() {
        DeviceInfo deviceInfo = new DeviceInfo();
        deviceInfo.setId(UUID.randomUUID().toString());
        deviceInfo.setCreatorId("admin");
        deviceInfo.setCreatorName("admin");
        deviceInfo.setProjectId("p001");
        deviceInfo.setProjectName("项目1");
        deviceInfo.setProtocol("jet-links");
        deviceInfo.setType((byte) 0);
        return deviceInfo;
    }

    @Test
    @SneakyThrows
    public void testConfig() {
        DeviceInfo info = newDeviceInfo();
        DeviceProductInfo productInfo = new DeviceProductInfo();
        productInfo.setId("test");
        productInfo.setName("测试");
        productInfo.setProjectId("test");
        productInfo.setProtocol("jet-links");
        info.setProductId(productInfo.getId());
        info.setProductName(productInfo.getName());

        DeviceProductOperation productOperation = registry.getProduct(productInfo.getId());
        productOperation.update(productInfo);
        productOperation.put("test_config", "1234");
        productOperation.put("test_config__", "aaa");
        try {
            Assert.assertNotNull(productOperation.getProtocol());

            DeviceOperation operation = registry.registry(info);

            Assert.assertNotNull(operation.getProtocol());

            Assert.assertEquals(operation.get("test_config").asString().orElse(null), "1234");

            Map<String, Object> conf = operation.getAll("test_config");
            System.out.println(conf);
            Assert.assertEquals(conf.get("test_config"), "1234");

            operation.put("test_config", "2345");
            operation.put("test_config2", 1234);

            Assert.assertEquals(operation.get("test_config").asString().orElse(null), "2345");
            conf = operation.getAll("test_config", "test_config__", "test_config2");
            System.out.println(conf);
            Assert.assertEquals(conf.get("test_config"), "2345");
            Assert.assertEquals(conf.get("test_config2"), 1234);
            Assert.assertEquals(conf.get("test_config__"), "aaa");

            Map<String, Object> all = operation.getAll();
            Assert.assertEquals(all.get("test_config"), "2345");
            Assert.assertEquals(all.get("test_config2"), 1234);
            Assert.assertEquals(all.get("test_config__"), "aaa");
            System.out.println(all);

            Assert.assertEquals(operation.remove("test_config"), "2345");
            Assert.assertTrue(operation.get("test_config").isPresent());

            operation.putAll(all);
            Assert.assertEquals(all.get("test_config"), "2345");

            operation.putAll(null);
            operation.putAll(Collections.emptyMap());

            Assert.assertFalse(Try.of(() -> {

                operation.put("test", null);

                return true;
            }).recover(NullPointerException.class, false).get());


        } finally {
            registry.unRegistry(info.getId());
        }
    }

    @Test
    @SneakyThrows
    public void testBenchmark() {
        DeviceInfo info = newDeviceInfo();
        DeviceProductInfo productInfo = new DeviceProductInfo();
        productInfo.setId("test2");
        productInfo.setName("测试");
        productInfo.setProjectId("test");
        productInfo.setProtocol("jet-links");
        info.setProductId(productInfo.getId());

        registry.registry(info);

        registry.getProduct(productInfo.getId()).update(productInfo);

        registry.getProduct(productInfo.getId()).put("test", "1234");
        Thread.sleep(100);

        long time = System.currentTimeMillis();

        CountDownLatch latch = new CountDownLatch(1000);
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        for (int i = 0; i < 1000; i++) {
            int fi = i;
            CompletableFuture.runAsync(() -> {
                try {
                    registry.getDevice(info.getId()).put("test", 123);
                    registry.getDevice(info.getId()).get("test:" + fi);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }, executorService);
        }
        latch.await(30, TimeUnit.SECONDS);
        executorService.shutdown();
        System.out.println(System.currentTimeMillis() - time);
    }

    @Test
    @SneakyThrows
    public void testRegistry() {
        DeviceInfo info = newDeviceInfo();
        try {
            DeviceOperation operation = registry.registry(info);
            Assert.assertNotNull(operation);
            Assert.assertEquals(operation.getState(), DeviceState.offline);
            operation.online("server-01", "session-01");
            Assert.assertEquals(operation.getState(), DeviceState.online);
            Assert.assertEquals(operation.getServerId(), "server-01");
            Assert.assertEquals(operation.getSessionId(), "session-01");
            Assert.assertTrue(operation.isOnline());
            operation.offline();
            Assert.assertFalse(operation.isOnline());
            Assert.assertNull(operation.getServerId());
            Assert.assertNull(operation.getSessionId());
        } finally {
            registry.unRegistry(info.getId());
            Thread.sleep(500);
            DeviceOperation operation = registry.getDevice(info.getId());
            Assert.assertEquals(operation.getState(), DeviceState.unknown);
        }
    }

    @Test
    public void benchmarkTest() {
        int size = 1000;

        List<DeviceOperation> operations = new ArrayList<>(size);
        long time = System.currentTimeMillis();
        System.out.println("RAM：" + Runtime.getRuntime().totalMemory() / 1024 / 1024 + "MB");
        for (int i = 0; i < size; i++) {
            DeviceInfo info = newDeviceInfo();
            DeviceOperation operation = registry.registry(info);
            operations.add(operation);
        }
        long completeTime = System.currentTimeMillis();
        log.debug("registry {} devices use {}ms", size, completeTime - time);

        time = System.currentTimeMillis();
        for (DeviceOperation operation : operations) {
            operation.authenticate(new AuthenticationRequest() {
            });
            operation.online("server_01", "session_0");
            operation.getDeviceInfo();
        }
        completeTime = System.currentTimeMillis();
        log.debug("online {} devices use {}ms", size, completeTime - time);
        System.out.println("RAM：" + Runtime.getRuntime().totalMemory() / 1024 / 1024 + "MB");
        for (DeviceOperation operation : operations) {
            registry.unRegistry(operation.getDeviceInfo().getId());
        }
    }

    @Test
    @SneakyThrows
    public void testSendMessage() {
        DeviceInfo device = newDeviceInfo();
        device.setId("test");
        //注册
        DeviceOperation operation = registry.registry(device);

        //上线
        operation.online("test", "test");

        messageHandler.handleMessage("test", msg -> {
            CommonDeviceMessageReply reply = msg.toJson().toJavaObject(CommonDeviceMessageReply.class);
            reply.setSuccess(true);
            reply.setMessage("成功");
            messageHandler.reply(reply);
        });

        CommonDeviceMessageReply reply = operation.messageSender()
                .invokeFunction("test")
                .send()
                .toCompletableFuture()
                .get(1, TimeUnit.SECONDS);
        Assert.assertNotNull(reply);
        Assert.assertTrue(reply.isSuccess());

        long time = System.currentTimeMillis();

        long len = 100;
        for (int i = 0; i < len; i++) {
            reply = operation.messageSender()
                    .invokeFunction("test")
                    .send()
                    .toCompletableFuture()
                    .get(5, TimeUnit.SECONDS);
            Assert.assertNotNull(reply);
            Assert.assertTrue(reply.isSuccess());
        }
        System.out.println("执行" + len + "次消息收发,耗时:" + (System.currentTimeMillis() - time) + "ms");
        registry.unRegistry(device.getId());
    }

}