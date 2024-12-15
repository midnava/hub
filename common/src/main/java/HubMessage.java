import java.nio.ByteBuffer;

public class HubMessage {
    public static final ByteBuffer ZERO_BUFFER = ByteBuffer.allocate(1);
    private final MessageType msgType;
    private final String topic;
    private final ByteBuffer msgBytes;

    public HubMessage(MessageType msgType, String topic, ByteBuffer msgBytes) {
        this.msgType = msgType;
        this.topic = topic;
        this.msgBytes = msgBytes;
    }

    public HubMessage(MessageType msgType, String topic) {
        this.msgType = msgType;
        this.topic = topic;
        this.msgBytes = ZERO_BUFFER;
    }

    public MessageType getMsgType() {
        return msgType;
    }

    public String getTopic() {
        return topic;
    }

    public ByteBuffer getMsgBytes() {
        return msgBytes;
    }

    @Override
    public String toString() {
        return "HubMessage{" +
                "msgType=" + msgType +
                ", topic='" + topic + '\'' +
                ", msgBytes=" + msgBytes +
                '}';
    }
}
