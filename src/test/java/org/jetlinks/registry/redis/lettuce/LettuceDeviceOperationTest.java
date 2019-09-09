package org.jetlinks.registry.redis.lettuce;

import lombok.SneakyThrows;
import org.jetlinks.core.device.DeviceMessageSender;
import org.jetlinks.core.device.DeviceOperation;
import org.jetlinks.core.device.DeviceState;
import org.jetlinks.core.enums.ErrorCode;
import org.jetlinks.core.message.*;
import org.jetlinks.core.message.exception.FunctionUndefinedException;
import org.jetlinks.core.message.exception.IllegalParameterException;
import org.jetlinks.core.message.exception.ParameterUndefinedException;
import org.jetlinks.core.message.function.FunctionInvokeMessageReply;
import org.jetlinks.core.message.interceptor.DeviceMessageSenderInterceptor;
import org.jetlinks.core.message.property.ReadPropertyMessage;
import org.jetlinks.core.message.property.ReadPropertyMessageReply;
import org.jetlinks.lettuce.LettucePlus;
import org.jetlinks.lettuce.supports.DefaultLettucePlus;
import org.jetlinks.supports.official.JetLinksProtocolSupport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.StreamUtils;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static io.vavr.API.Try;

public class LettuceDeviceOperationTest {

    private LettucePlus client;

    public LettuceDeviceRegistry registry;

    @Before
    @SneakyThrows
    public void init() {
        client = DefaultLettucePlus.standalone(RedisClientHelper.createRedisClient());

        JetLinksProtocolSupport jetLinksProtocolSupport = new JetLinksProtocolSupport();

        registry = new LettuceDeviceRegistry(client, new LettuceDeviceMessageHandler(client), protocol -> jetLinksProtocolSupport);

        registry.addInterceptor(new DeviceMessageSenderInterceptor() {
            @Override
            public DeviceMessage preSend(DeviceOperation device, DeviceMessage message) {
                return message;
            }

            @Override
            public <R extends DeviceMessageReply> CompletionStage<R> afterReply(DeviceOperation device, DeviceMessage message, R reply) {
                reply.addHeader("ts", System.currentTimeMillis());
                return CompletableFuture.completedFuture(reply);
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


    //设备网关服务宕机
    //场景: 设备网关服务宕机，未及时更新设备状态信息。继续往设备发消息时，会执行设备状态检查，更新状态
    @Test
    @SneakyThrows
    public void testServerOfflineCheckState() {
        DeviceOperation operation = registry.getDevice("test2");

        try {
            //模拟上线
            operation.online("test2-server", "test");

            Assert.assertEquals(operation.getState(), DeviceState.online);

            //模拟发送一条消息，该设备实际上并不在线。应该会自动执行状态检查
            FunctionInvokeMessageReply reply = operation.messageSender()
                    .invokeFunction("test")
                    .trySend(10, TimeUnit.SECONDS)
                    .recover(TimeoutException.class, (__) -> FunctionInvokeMessageReply.create().error(ErrorCode.TIME_OUT))
                    .get();

            Assert.assertFalse(reply.isSuccess());

            //调用了设备状态检查并自动更新了设备状态
            Assert.assertEquals(operation.getState(), DeviceState.offline);
            Assert.assertNull(operation.getServerId());
        } finally {
            registry.unRegistry("test2");
        }

    }

    @Test
    public void testSendOfflineServer() {
        DeviceOperation operation = registry.getDevice("test2");
        operation.online("test3-server", "test");

        Assert.assertEquals(operation.getState(), DeviceState.online);

        Assert.assertEquals(operation.messageSender()
                .readProperty("test")
                .custom(Headers.async.setter())
                .trySend(10, TimeUnit.SECONDS)
                .map(CommonDeviceMessageReply::getCode)
                .get(), ErrorCode.CLIENT_OFFLINE.name());

        Assert.assertEquals(operation.getState(), DeviceState.offline);

    }

    //设备网关服务正常运行，但是设备未连接到当前网关服务
    //场景: 设备网关宕机，未及时将设备更新为离线。当网关服务重新启动后，设备其实已经没有连接到这台服务器了。
    @Test
    @SneakyThrows
    public void testServerOnlineNotConnectCheckState() {

        try {
            CountDownLatch latch = new CountDownLatch(1);


            DeviceOperation operation = registry.getDevice("test2");
            //模拟上线
            operation.online("test2-server", "test");

            Assert.assertEquals(operation.getState(), DeviceState.online);

            //消息处理器
            LettuceDeviceMessageHandler handler = new LettuceDeviceMessageHandler(client);

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
        } finally {
            registry.unRegistry("test2");
        }
    }

    @Test
    @SneakyThrows
    public void testSendAndReplyMessage() {
        try {
            DeviceOperation operation = registry.getDevice("test2");
            operation.online("test-server", "12");
            DeviceMessageSender sender = operation.messageSender();


            LettuceDeviceMessageHandler handler = new LettuceDeviceMessageHandler(client);

            handler.markMessageAsync("testId").toCompletableFuture().get(10, TimeUnit.SECONDS);
            Assert.assertTrue(handler.messageIsAsync("testId").toCompletableFuture().get(10, TimeUnit.SECONDS));

            AtomicReference<DeviceMessage> messageReference = new AtomicReference<>();
            //处理发往设备的消息
            handler.handleMessage("test-server", message -> {
                messageReference.set(message);

                if (message instanceof RepayableDeviceMessage) {
                    try {
                        Thread.sleep(2000);
                    } catch (Exception e) {

                    }
                    //模拟设备回复消息
                    DeviceMessageReply reply = ((RepayableDeviceMessage) message).newReply();
                    reply.from(message);
                    reply.error(ErrorCode.REQUEST_HANDLING);
                    handler.reply(reply);
                }

            });
            //发送消息s
            ReadPropertyMessageReply reply = sender.readProperty("test")
                    .messageId("test-message")
                    .send()
                    .toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
            Assert.assertNotNull(messageReference.get());
            Assert.assertTrue(messageReference.get() instanceof ReadPropertyMessage);
            Assert.assertNotNull(reply);
            System.out.println(reply);

            messageReference.set(null);

            sender.readProperty("test")
                    .messageId("test-retrieve-msg")
                    .trySend(1, TimeUnit.MILLISECONDS);

            TimeUnit.SECONDS.sleep(5);
            ReadPropertyMessageReply retrieve = sender.readProperty("test")
                    .messageId("test-retrieve-msg")
                    .retrieveReply()
                    .toCompletableFuture()
                    .get(1, TimeUnit.SECONDS);
            Assert.assertNotNull(messageReference.get());
            Assert.assertNotNull(retrieve);
            Assert.assertEquals(retrieve.getCode(), ErrorCode.REQUEST_HANDLING.name());
            System.out.println(retrieve);

        } finally {
            registry.unRegistry("test2");
        }

    }


    @Test
    @SneakyThrows
    public void testValidateParameter() {
        try {
            DeviceOperation operation = registry.getDevice("test3");
            String metaData = StreamUtils.copyToString(new ClassPathResource("testValidateParameter.meta.json").getInputStream(), StandardCharsets.UTF_8);
            operation.updateMetadata(metaData);
            Thread.sleep(100);
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
        } finally {
            registry.unRegistry("test3");
        }
    }

}