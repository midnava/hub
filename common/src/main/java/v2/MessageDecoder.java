package v2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class MessageDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < 12) {
            return;
        }

        MessageType messageType = MessageType.find(in.readByte());

        int topicLength = in.readInt();

        if (in.readableBytes() < topicLength + 4) {
            in.resetReaderIndex();
            return;
        }

        byte[] topicBytes = new byte[topicLength];
        in.readBytes(topicBytes);
        String topic = new String(topicBytes, StandardCharsets.UTF_8);

        if (in.readableBytes() < 4 + 4) {
            in.resetReaderIndex();
            return;
        }

        int offset = in.readInt();
        int length = in.readInt();

        if (in.readableBytes() < length) {
            in.resetReaderIndex();
            return;
        }

        UnsafeBuffer buffer = new UnsafeBuffer(in.memoryAddress(), in.readableBytes()); //TODO IMPORTANT

        out.add(new Message(messageType, topic, buffer, offset, length));
    }
}
