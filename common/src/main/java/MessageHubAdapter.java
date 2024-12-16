import io.netty.buffer.ByteBuf;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;

public class MessageHubAdapter {

    public static ByteBuf serialize(HubMessage msg, ByteBuf byteBuf) {
        byteBuf.writeByte(msg.getMsgType().getId());
        byteBuf.writeInt(msg.getTopic().length());
        byteBuf.writeCharSequence(msg.getTopic(), StandardCharsets.US_ASCII);

        UnsafeBuffer buffer = msg.getMsgBytes();
        int length = msg.getMsgBytesLength();

        byteBuf.writeInt(length);
        byteBuf.writeBytes(buffer.byteBuffer().array(), 0, length);

        return byteBuf;
    }

    public static HubMessage deserialize(ByteBuf b) {

        MessageType msgType = MessageType.find(b.readByte());
        int topicLength = b.readInt();
        String topic = b.readCharSequence(topicLength, StandardCharsets.US_ASCII).toString();
        int bufferLength = b.readInt();

        UnsafeBuffer buffer = new UnsafeBuffer(b.nioBuffer(), b.arrayOffset(), b.readableBytes());

        return new HubMessage(msgType, topic, buffer, bufferLength);
    }

    public static HubMessage deserializeHeader(ByteBuf b) {

        MessageType msgType = MessageType.find(b.readByte());
        int topicLength = b.readInt();
        String topic = b.readCharSequence(topicLength, StandardCharsets.US_ASCII).toString();

        return new HubMessage(msgType, topic);
    }
}
