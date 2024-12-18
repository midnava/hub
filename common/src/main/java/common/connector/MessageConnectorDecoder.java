package common.connector;

import common.HubMessage;
import common.MessageType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class MessageConnectorDecoder extends ByteToMessageDecoder {
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

//        ByteBuffer byteBuffer = in.nioBuffer();
//        if (byteBuffer.isDirect()) {
//            long address = ByteBufferAddressHelper.getAddress(byteBuffer);
//            buffer.wrap(address, bufferLength); //IMPORTANT
//            in.skipBytes(bufferLength);
//        } else {
        in.readBytes(buffer.byteBuffer().array(), 0, bufferLength);
//        }
        out.add(new HubMessage(messageType, topic, seqNo, buffer, offset, bufferLength));

    }
}
