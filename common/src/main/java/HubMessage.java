import java.nio.ByteBuffer;

public class HubMessage {
    private final MessageType msgType;
    private final String topic;
    private final ByteBuffer msgBytes;

    public HubMessage(MessageType msgType, String topic, ByteBuffer msgBytes) {
        this.msgType = msgType;
        this.topic = topic;
        this.msgBytes = msgBytes;
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
}
