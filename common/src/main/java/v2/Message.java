package v2;

// Message Model
public class Message {
    public final String topic;
    public final int seqNo;

    public Message(String topic, int seqNo) {
        this.topic = topic;
        this.seqNo = seqNo;
    }

    public String getTopic() {
        return topic;
    }

    public int getSeqNo() {
        return seqNo;
    }
}

