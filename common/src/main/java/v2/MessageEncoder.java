package v2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;

public class MessageEncoder extends MessageToByteEncoder<Message> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) {
        out.writeByte(msg.getMessageType().getId());

        byte[] topicBytes = msg.getTopic().getBytes(StandardCharsets.UTF_8);
        out.writeInt(topicBytes.length);
        out.writeBytes(topicBytes);

        int length = msg.getLength();

        UnsafeBuffer unsafeBuffer = msg.getByteBuf();
        out.writeInt(msg.getOffset());
        out.writeInt(length);

        //out.writeBytes(unsafeBuffer.byteBuffer().array(), 0, length);
    }
}

