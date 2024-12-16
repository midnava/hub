import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

public class HubMessage {
    public static final UnsafeBuffer ZERO_BUFFER = new UnsafeBuffer(ByteBuffer.allocate(0));

    private final MessageType msgType;
    private final String topic;
    private final UnsafeBuffer msgBytes; //text or message bytes
    private final int msgBytesLength;

    public HubMessage(MessageType msgType, String topic, UnsafeBuffer msgBytes, int msgBytesLength) {
        this.msgType = msgType;
        this.topic = topic;
        this.msgBytes = msgBytes;
        this.msgBytesLength = msgBytesLength;
    }

    public HubMessage(MessageType msgType, String topic) {
        this.msgType = msgType;
        this.topic = topic;
        this.msgBytes = ZERO_BUFFER;
        this.msgBytesLength = 0;
    }

    public HubMessage(MessageType msgType, String topic, String msg) {
        this.msgType = msgType;
        this.topic = topic;
        this.msgBytes = new UnsafeBuffer(ByteBuffer.allocate(msg.length() + 4));
        this.msgBytesLength = msgBytes.putStringAscii(0, msg);
    }

    public MessageType getMsgType() {
        return msgType;
    }

    public String getTopic() {
        return topic;
    }

    public UnsafeBuffer getMsgBytes() {
        return msgBytes;
    }

    public int getMsgBytesLength() {
        return msgBytesLength;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder()
                .append("HubMessage{")
                .append("msgType=").append(msgType)
                .append(", topic=").append(topic)
                .append(", msgBytesLength=").append(msgBytesLength);

        if (msgType != MessageType.MESSAGE) {
            builder.append(", msgBytes=")
//                    .append(msgBytes.getStringAscii(0))
                    .append('}');
        }

        return builder.toString();
    }
}
