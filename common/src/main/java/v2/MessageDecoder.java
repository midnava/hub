package v2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class MessageDecoder extends ByteToMessageDecoder {
    private final ThreadLocal<UnsafeBuffer> bufferThreadLocal = ThreadLocal
            .withInitial(() -> new UnsafeBuffer(ByteBuffer.allocate(128 * 1024)));

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        MessageType messageType = MessageType.find(in.readByte());
        long seqNo = in.readLong();

        int topicLength = in.readInt();

        byte[] topicBytes = new byte[topicLength];
        in.readBytes(topicBytes);

        String topic = new String(topicBytes, StandardCharsets.US_ASCII);

        UnsafeBuffer buffer = bufferThreadLocal.get();
        int offset = in.readInt();
        int bufferLength = in.readInt();

//        buffer.wrap(in.memoryAddress(), in.readableBytes()); //IMPORTANT
//        in.skipBytes(bufferLength);

        in.readBytes(buffer.byteBuffer().array(), 0, bufferLength);

        out.add(new Message(messageType, topic, seqNo, buffer, offset, bufferLength));
    }
}
