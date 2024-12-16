import java.nio.ByteBuffer;

public class HubMessage {
    public static final ByteBuffer ZERO_BUFFER = ByteBuffer.allocate(0);
    private final MessageType msgType;
    private final String topic;
    private final ByteBuffer msgBytes; //text or message bytes

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

    public HubMessage(MessageType msgType, String topic, String msg) {
        this.msgType = msgType;
        this.topic = topic;
        this.msgBytes = ByteBuffer.allocate(msg.length());

        msgBytes.put(msg.getBytes(), 0, msg.length());
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
        StringBuilder builder = new StringBuilder()
                .append("HubMessage{")
                .append("msgType=").append(msgType)
                .append(", topic='").append(topic).append('\'');

        if (msgType != MessageType.MESSAGE) {
            builder.append(", msgBytes=")
                    .append(new String(msgBytes.array())).append('}');
        }

        return builder.toString();
    }
}
