import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

public class OldHubMessage {
    public static final UnsafeBuffer ZERO_BUFFER = new UnsafeBuffer(ByteBuffer.allocate(0));

    private final MessageTypeOld msgType;
    private final String topic;
    private final UnsafeBuffer msgBytes; //text or message bytes
    private final int msgBytesLength;

    public OldHubMessage(MessageTypeOld msgType, String topic, UnsafeBuffer msgBytes, int msgBytesLength) {
        this.msgType = msgType;
        this.topic = topic;
        this.msgBytes = msgBytes;
        this.msgBytesLength = msgBytesLength;
    }

    public OldHubMessage(MessageTypeOld msgType, String topic) {
        this.msgType = msgType;
        this.topic = topic;
        this.msgBytes = ZERO_BUFFER;
        this.msgBytesLength = 0;
    }

    public OldHubMessage(MessageTypeOld msgType, String topic, String msg) {
        this.msgType = msgType;
        this.topic = topic;
        this.msgBytes = new UnsafeBuffer(ByteBuffer.allocate(msg.length() + 4));
        this.msgBytesLength = msgBytes.putStringAscii(0, msg);
    }

    public MessageTypeOld getMsgType() {
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

        if (msgType != MessageTypeOld.MESSAGE) {
            builder.append(", msgBytes=")
//                    .append(msgBytes.getStringAscii(0))
                    .append('}');
        }

        return builder.toString();
    }
}
