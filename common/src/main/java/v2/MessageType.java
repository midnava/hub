package v2;

public enum MessageType {
    MESSAGE(1),
    PUB_INIT(2),
    SUBSCRIBE(3),
    UNSUBSCRIBE(4),
    SUBSCRIBE_RESPONSE(5);

    private final byte id;

    private final static MessageType[] types = MessageType.values();


    MessageType(int id) {
        this.id = (byte) id;
    }

    public byte getId() {
        return id;
    }

    public static MessageType find(byte id) {
        for (int i = 0; i < types.length; i++) {
            MessageType type = types[i];
            if (type.id == id) {
                return type;
            }
        }

        throw new RuntimeException("no enum for id " + id);
    }

    @Override
    public String toString() {
        return name();
    }
}
