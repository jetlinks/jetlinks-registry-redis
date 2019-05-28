package org.jetlinks.registry.redis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.netty.buffer.Unpooled;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.AuthenticationRequest;
import org.jetlinks.core.device.AuthenticationResponse;
import org.jetlinks.core.device.DeviceOperation;
import org.jetlinks.core.message.CommonDeviceMessageReply;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.codec.*;
import org.jetlinks.core.metadata.DeviceMetadataCodec;

import java.nio.charset.StandardCharsets;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class MockProtocolSupports implements ProtocolSupports {
    @Override
    public ProtocolSupport getProtocol(String protocol) {
        return new ProtocolSupport() {
            @Override
            public String getId() {
                return "mock";
            }

            @Override
            public String getName() {
                return "模拟协议";
            }

            @Override
            public String getDescription() {
                return "";
            }

            @Override
            public DeviceMessageCodec getMessageCodec() {

                return new DeviceMessageCodec() {
                    @Override
                    public EncodedMessage encode(Transport transport, MessageEncodeContext context) {
                        return EncodedMessage.mqtt(context.getMessage().getDeviceId(), "command",
                                Unpooled.copiedBuffer(context.getMessage().toJson().toJSONString().getBytes()));
                    }

                    @Override
                    public DeviceMessage decode(Transport transport, MessageDecodeContext context) {
                        JSONObject jsonObject = JSON.parseObject(context.getMessage().getByteBuf().toString(StandardCharsets.UTF_8));
                        if ("read-property".equals(jsonObject.get("type"))) {
//                            return jsonObject.toJavaObject(GettingPropertyMessageReply.class);
                        }
                        return jsonObject.toJavaObject(CommonDeviceMessageReply.class);
                    }
                };
            }

            @Override
            public DeviceMetadataCodec getMetadataCodec() {
                throw new UnsupportedOperationException();
            }

            @Override
            public AuthenticationResponse authenticate(AuthenticationRequest request, DeviceOperation deviceOperation) {
                return AuthenticationResponse.success();
            }
        };
    }
}
