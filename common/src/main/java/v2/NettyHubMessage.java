package v2;

import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

// Message Model
public class NettyHubMessage {
    private static final byte[] EMPTY_BUFFER = new byte[0];

    private final MessageType messageType;
    private final String topic;
    private final long seqNo;
    private final UnsafeBuffer byteBuf;
    private final int offset;
    private final int buffLength;


    public NettyHubMessage(MessageType messageType, String topic, long seqNo, UnsafeBuffer byteBuf, int offset, int buffLength) {
        this.messageType = messageType;
        this.topic = topic;
        this.seqNo = seqNo;
        this.byteBuf = byteBuf;
        this.offset = offset;
        this.buffLength = buffLength;
    }

    public NettyHubMessage(MessageType messageType, String topic, long seqNo, String msg) {
        this.messageType = messageType;
        this.topic = topic;
        this.seqNo = seqNo;

        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(msg.length() + 4));
        int length = buffer.putStringAscii(0, msg);

        this.byteBuf = buffer;
        this.offset = 0;
        this.buffLength = length;
    }

    public void cleanupBuffer() {
        byteBuf.wrap(EMPTY_BUFFER);
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public String getTopic() {
        return topic;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public UnsafeBuffer getByteBuf() {
        return byteBuf;
    }

    public int getOffset() {
        return offset;
    }

    public int getBuffLength() {
        return buffLength;
    }

    @Override
    public String toString() {
        return "Message{" +
                "messageType=" + messageType +
                ", topic='" + topic + '\'' +
                ", seqNo=" + seqNo +
                ", offset=" + offset +
                ", length=" + buffLength +
                ", byteBuf=" + byteBuf +
                '}';
    }
}

