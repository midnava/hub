package hub.adapters;

import common.HubMessage;
import common.MessageType;
import io.netty.buffer.ByteBuf;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class NettyHubMessageAdapter {
    private static final ThreadLocal<UnsafeBuffer> bufferThreadLocal = ThreadLocal
            .withInitial(() -> new UnsafeBuffer(ByteBuffer.allocate(128 * 1024)));

    public static void serialize(HubMessage msg, ByteBuf out) {
        byte[] topicBytes = msg.getTopic().getBytes(StandardCharsets.US_ASCII);
        int topicLength = topicBytes.length;
        int offset = msg.getOffset();
        int buffLength = msg.getBuffLength();

        // msgType + seqNo + topicLength + topicBytes + offset + buffLength + buffer
        int messageLength = 1 + 8 + 4 + topicLength + 4 + 4 + buffLength;

        //total
        out.writeInt(messageLength);

        out.writeByte(msg.getMessageType().getId());
        out.writeLong(msg.getSeqNo());
        //string
        out.writeInt(topicLength);
        out.writeBytes(topicBytes);

        //buffer
        out.writeInt(offset);
        out.writeInt(buffLength);
        out.writeBytes(msg.getByteBuf().byteBuffer().array(), 0, buffLength);
    }

    public static HubMessage deserialize(ByteBuf in) {
        MessageType messageType = MessageType.find(in.readByte());
        long seqNo = in.readLong();

        int topicLength = in.readInt();

        byte[] topicBytes = new byte[topicLength];
        in.readBytes(topicBytes);

        String topic = new String(topicBytes, StandardCharsets.US_ASCII);

        if (messageType == MessageType.MESSAGE) { //need just redirect
            return new HubMessage(messageType, topic, seqNo, null, 0, 0);
        } else {
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
            return new HubMessage(messageType, topic, seqNo, buffer, offset, bufferLength);
        }
    }

    public static HubMessage deserializeHeader(ByteBuf in) {
        MessageType messageType = MessageType.find(in.readByte());
        long seqNo = in.readLong();

        int topicLength = in.readInt();

        byte[] topicBytes = new byte[topicLength];
        in.readBytes(topicBytes);

        String topic = new String(topicBytes, StandardCharsets.US_ASCII);

        return new HubMessage(messageType, topic, seqNo, null, 0, 0);
    }
}

