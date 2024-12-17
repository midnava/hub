package v2;

import org.agrona.concurrent.UnsafeBuffer;

// Message Model
public class Message {
    private final MessageType messageType;
    private final String topic;
    private final UnsafeBuffer byteBuf;
    private final int offset;
    private final int length;

    public Message(MessageType messageType, String topic, UnsafeBuffer byteBuf, int offset, int length) {
        this.messageType = messageType;
        this.topic = topic;
        this.byteBuf = byteBuf;
        this.offset = offset;
        this.length = length;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public String getTopic() {
        return topic;
    }

    public UnsafeBuffer getByteBuf() {
        return byteBuf;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    @Override
    public String toString() {
        return "Message{" +
                "messageType=" + messageType +
                ", topic='" + topic + '\'' +
                ", offset=" + offset +
                ", length=" + length +
                ", byteBuf=" + byteBuf +
                '}';
    }
}

