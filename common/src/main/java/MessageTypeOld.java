
public enum MessageTypeOld {
    MESSAGE(1),
    PUB_INIT(2),
    SUBSCRIBE(3),
    UNSUBSCRIBE(4),
    SUBSCRIBE_RESPONSE(5);

    private final byte id;

    private final static MessageTypeOld[] types = MessageTypeOld.values();


    MessageTypeOld(int id) {
        this.id = (byte) id;
    }

    public byte getId() {
        return id;
    }

    public static MessageTypeOld find(byte id) {
        for (int i = 0; i < types.length; i++) {
            MessageTypeOld type = types[i];
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
