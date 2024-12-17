package v2;

import org.agrona.concurrent.UnsafeBuffer;

// Message Model
public class Message {
    private final MessageType messageType;
    private final String topic;
    private final long seqNo;
    private final UnsafeBuffer byteBuf;
    private final int offset;
    private final int buffLength;


    public Message(MessageType messageType, String topic, long seqNo, UnsafeBuffer byteBuf, int offset, int buffLength) {
        this.messageType = messageType;
        this.topic = topic;
        this.seqNo = seqNo;
        this.byteBuf = byteBuf;
        this.offset = offset;
        this.buffLength = buffLength;
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

