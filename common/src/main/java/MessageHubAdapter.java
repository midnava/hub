import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledHeapByteBuf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MessageHubAdapter {

    public static ByteBuf serialize(HubMessage msg) {
        ByteBuf byteBuf = new UnpooledHeapByteBuf(ByteBufAllocator.DEFAULT, 256, 2048);

        return serialize(msg, byteBuf);
    }

    public static ByteBuf serialize(HubMessage msg, ByteBuf byteBuf) {
        byteBuf.writeByte(msg.getMsgType().getId());
        byteBuf.writeInt(msg.getTopic().length());
        byteBuf.writeCharSequence(msg.getTopic(), StandardCharsets.US_ASCII);

        ByteBuffer msgBytes = msg.getMsgBytes();
        int dif = msgBytes.capacity() - msgBytes.remaining();

        byteBuf.writeInt(dif);
        byteBuf.writeBytes(msgBytes.array(), 0, dif);

        return byteBuf;
    }

    public static HubMessage deserialize(ByteBuf b) {

        MessageType msgType = MessageType.find(b.readByte());
        int topicLength = b.readInt();
        String topic = b.readCharSequence(topicLength, StandardCharsets.US_ASCII).toString();
        int bufferLength = b.readInt();
        ByteBuffer buffer = ByteBuffer.allocate(bufferLength);
        b.readBytes(buffer);

        return new HubMessage(msgType, topic, buffer);
    }
}
