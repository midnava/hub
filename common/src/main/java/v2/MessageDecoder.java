package v2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class MessageDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < 13) { //8 + 4 + 1
            return;
        }

        in.markReaderIndex();
        MessageType messageType = MessageType.find(in.readByte());
        long seqNo = in.readLong();

        int topicLength = in.readInt();
        if (in.readableBytes() < topicLength + 8) {
            in.resetReaderIndex();
            return;
        }

        byte[] topicBytes = new byte[topicLength];
        in.readBytes(topicBytes);

        String topic = new String(topicBytes, StandardCharsets.UTF_8);

        out.add(new Message(messageType, topic, seqNo));
    }
}
