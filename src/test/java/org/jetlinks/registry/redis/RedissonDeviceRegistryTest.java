package org.jetlinks.registry.redis;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.*;
import org.jetlinks.core.device.registry.DeviceMessageHandler;
import org.jetlinks.core.message.CommonDeviceMessageReply;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.redisson.api.RedissonClient;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class RedissonDeviceRegistryTest {

    private RedissonDeviceRegistry registry;

    private DeviceMessageHandler messageHandler;

    @Before
    public void init() {
        RedissonClient client = RedissonHelper.newRedissonClient();

        ExecutorService service = Executors.newScheduledThreadPool(2);

        messageHandler = new RedissonDeviceMessageHandler(client, service);
        registry = new RedissonDeviceRegistry(RedissonHelper.newRedissonClient(), new MockProtocolSupports());
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
        try {
            DeviceOperation operation = registry.registry(info);
            Assert.assertEquals(operation.get("test_config").asString().orElse(null), "1234");
            operation.put("test_config", "2345");
            Assert.assertEquals(operation.get("test_config").asString().orElse(null), "2345");
        } finally {
            registry.unRegistry(info.getId());
        }
    }

    @Test
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
        CountDownLatch downLatch = new CountDownLatch(1);
        messageHandler.handleDeviceCheck("test", deviceId -> {
            log.info("check state:{}",deviceId);
            downLatch.countDown();
        });

        operation.checkState();
        Assert.assertTrue(downLatch.await(20, TimeUnit.SECONDS));

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