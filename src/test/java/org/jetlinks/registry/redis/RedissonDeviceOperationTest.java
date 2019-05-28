package org.jetlinks.registry.redis;

import lombok.SneakyThrows;
import org.jetlinks.core.device.DeviceOperation;
import org.jetlinks.core.device.DeviceState;
import org.jetlinks.core.device.registry.DeviceRegistry;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.DeviceMessageReply;
import org.jetlinks.core.message.RepayableDeviceMessage;
import org.jetlinks.core.message.function.FunctionInvokeMessageReply;
import org.jetlinks.core.message.property.ReadPropertyMessage;
import org.jetlinks.core.message.property.ReadPropertyMessageReply;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.redisson.api.RedissonClient;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class RedissonDeviceOperationTest {

    RedissonClient client = RedissonHelper.newRedissonClient();


    public DeviceRegistry registry;

    @Before
    public void init() {
        registry = new RedissonDeviceRegistry(client, protocol -> null);

    }

    //设备网关服务宕机
    //场景: 设备网关服务宕机，未及时更新设备状态信息。继续往设备发消息时，会执行设备状态检查，更新状态
    @Test
    @SneakyThrows
    public void testServerOfflineCheckState() {

        DeviceOperation operation = registry.getDevice("test2");
        //模拟上线
        operation.online("test2-server", "test");

        Assert.assertEquals(operation.getState(), DeviceState.online);

        //模拟发送一条消息，该设备实际上并不在线。应该会自动执行状态检查
        FunctionInvokeMessageReply reply = operation.messageSender()
                .invokeFunction("test")
                .send()
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);

        Assert.assertFalse(reply.isSuccess());

        //调用了设备状态检查并自动更新了设备状态
        Assert.assertEquals(operation.getState(), DeviceState.offline);

    }


    //设备网关服务正常运行，但是设备未连接到当前网关服务
    //场景: 设备网关宕机，未及时将设备更新为离线。当网关服务重新启动后，设备其实已经没有连接到这台服务器了。
    @Test
    @SneakyThrows
    public void testServerOnlineNotConnectCheckState() {

        CountDownLatch latch = new CountDownLatch(1);


        DeviceOperation operation = registry.getDevice("test2");
        //模拟上线
        operation.online("test2-server", "test");

        Assert.assertEquals(operation.getState(), DeviceState.online);

        //消息处理器
        RedissonDeviceMessageHandler handler = new RedissonDeviceMessageHandler(client, Executors.newFixedThreadPool(6));

        handler.handleDeviceCheck("test2-server", deviceId -> {

            //模拟设备并没有连接到本服务器,修改设备状态离线.
            operation.offline();
            latch.countDown();

        });

        //主动调用设备状态检查
        operation.checkState();

        //调用了设备状态检查
        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        Assert.assertEquals(operation.getState(), DeviceState.offline);

    }

    @Test
    @SneakyThrows
    public void testSendAndReplyMessage() {

        RedissonDeviceMessageSender sender = new RedissonDeviceMessageSender("test2", client, () -> "test-server", () -> {
        });

        RedissonDeviceMessageHandler handler = new RedissonDeviceMessageHandler(client, Executors.newFixedThreadPool(6));


        AtomicReference<DeviceMessage> messageReference = new AtomicReference<>();
        //处理发往设备的消息
        handler.handleMessage("test-server", message -> {
            messageReference.set(message);

            if (message instanceof RepayableDeviceMessage) {
                //模拟设备回复消息
                DeviceMessageReply reply = ((RepayableDeviceMessage) message).newReply();
                reply.from(message);
                handler.reply(reply);
            }

        });
        //发送消息s
        ReadPropertyMessageReply reply = sender.readProperty("test")
                .send()
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        Assert.assertNotNull(messageReference.get());
        Assert.assertTrue(messageReference.get() instanceof ReadPropertyMessage);
        Assert.assertNotNull(reply);

    }

}