package org.jetlinks.registry.redis;

import lombok.SneakyThrows;
import org.jetlinks.core.device.DeviceOperation;
import org.jetlinks.core.device.DeviceState;
import org.jetlinks.core.device.registry.DeviceRegistry;
import org.jetlinks.core.enums.ErrorCode;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.DeviceMessageReply;
import org.jetlinks.core.message.RepayableDeviceMessage;
import org.jetlinks.core.message.exception.FunctionUndefinedException;
import org.jetlinks.core.message.exception.IllegalParameterException;
import org.jetlinks.core.message.exception.ParameterUndefinedException;
import org.jetlinks.core.message.function.FunctionInvokeMessageReply;
import org.jetlinks.core.message.property.ReadPropertyMessage;
import org.jetlinks.core.message.property.ReadPropertyMessageReply;
import org.jetlinks.core.support.JetLinksProtocolSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.redisson.api.RedissonClient;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.StreamUtils;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.vavr.API.Try;

public class RedissonDeviceOperationTest {

    RedissonClient client = RedissonHelper.newRedissonClient();


    public DeviceRegistry registry;

    @Before
    public void init() {
        JetLinksProtocolSupport jetLinksProtocolSupport = new JetLinksProtocolSupport();

        registry = new RedissonDeviceRegistry(client, protocol -> jetLinksProtocolSupport);

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
                .trySend(10, TimeUnit.SECONDS)
                .recoverWith(TimeoutException.class, (__) -> FunctionInvokeMessageReply.failureTry(ErrorCode.TIME_OUT))
                .get();

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
        RedissonDeviceMessageHandler handler = new RedissonDeviceMessageHandler(client);

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
        DeviceOperation operation = registry.getDevice("test2");
        operation.online("test-server", "12");
        RedissonDeviceMessageSender sender = new RedissonDeviceMessageSender("test2", client, operation);

        RedissonDeviceMessageHandler handler = new RedissonDeviceMessageHandler(client);

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


    @Test
    @SneakyThrows
    public void testValidateParameter() {
        DeviceOperation operation = registry.getDevice("test3");
        String metaData = StreamUtils.copyToString(new ClassPathResource("testValidateParameter.meta.json").getInputStream(), StandardCharsets.UTF_8);
        operation.updateMetadata(metaData);

        Assert.assertNotNull(operation.getMetadata());

        //function未定义

        Assert.assertTrue(Try(() -> operation.messageSender().invokeFunction("getSysInfoUndefined").validate())
                .map(r -> false)
                .recover(FunctionUndefinedException.class, true)
                .recover(err -> false)
                .get());
        //参数错误
        Assert.assertTrue(Try(() -> operation.messageSender().invokeFunction("getSysInfo").validate())
                .map(r -> false)
                .recover(IllegalArgumentException.class, true)
                .recover(err -> false)
                .get());

        //参数未定义
        Assert.assertTrue(Try(() -> operation.messageSender()
                .invokeFunction("getSysInfo")
                .addParameter("test", "123")
                .validate())
                .map(r -> false)
                .recover(ParameterUndefinedException.class, true)
                .recover(err -> false)
                .get());

        //参数值类型错误
        Assert.assertTrue(Try(() -> operation.messageSender()
                .invokeFunction("getSysInfo")
                .addParameter("useCache", "2")
                .validate())
                .map(r -> false)
                .recover(IllegalParameterException.class, true)
                .recover(err -> false)
                .get());

        //通过
        operation.messageSender()
                .invokeFunction("getSysInfo")
                .addParameter("useCache", "1")
                .validate();

        operation.messageSender()
                .invokeFunction("getSysInfo")
                .addParameter("useCache", "0")
                .validate();

        registry.unRegistry("test3");
    }

}