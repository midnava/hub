package v2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class MessageDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        MessageType messageType = MessageType.find(in.readByte());

        int topicLength = in.readInt();
        byte[] topicBytes = new byte[topicLength];
        in.readBytes(topicBytes);
        String topic = new String(topicBytes, StandardCharsets.UTF_8);

        int offset = in.readInt();
        int length = in.readInt();
//        UnsafeBuffer buffer = new UnsafeBuffer(in.memoryAddress(), in.readableBytes()); //TODO IMPORTANT

        out.add(new Message(messageType, topic, null, offset, length));
    }
}
