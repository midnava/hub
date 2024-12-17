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
        long seqNo = in.readLong();

        int topicLength = in.readInt();

        byte[] topicBytes = new byte[topicLength];
        in.readBytes(topicBytes);

        String topic = new String(topicBytes, StandardCharsets.UTF_8);

        out.add(new Message(messageType, topic, seqNo));
    }
}
