package v2;

// Message Model
public class Message {
    private final MessageType messageType;
    private final String topic;
    private final long seqNo;
//    private final UnsafeBuffer byteBuf;
//    private final int offset;
//    private final int length;


    public Message(MessageType messageType, String topic, long seqNo) {
        this.messageType = messageType;
        this.topic = topic;
        this.seqNo = seqNo;
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

    @Override
    public String toString() {
        return "Message{" +
                "messageType=" + messageType +
                ", topic='" + topic + '\'' +
                ", seqNo=" + seqNo +
                '}';
    }
}

