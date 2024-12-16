
public enum MessageType {
    PUB_INIT(0),
    SUBSCRIBE(1),
    SUBSCRIBE_RESPONSE(2),
    MESSAGE(3);

    private final byte id;

    private final static MessageType[] types = MessageType.values();


    MessageType(int id) {
        this.id = (byte) id;
    }

    public byte getId() {
        return id;
    }

    public static MessageType find(byte id) {
        return types[id];
    }

    @Override
    public String toString() {
        return name();
    }
}
