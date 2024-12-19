package common;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StringPool {
    private final Map<String, byte[]> stringBytes = new ConcurrentHashMap<>();
    private final Map<byte[], String> bytesString = new ConcurrentHashMap<>();


    public static final StringPool INSTANCE = new StringPool();

    public String getString(byte[] bytes) {

        //TODO IMEMENT IT
        return "topic";
    }

    public byte[] getStringBytes(String str) {
        byte[] bytes = stringBytes.get(str);
        if (bytes != null) {
            return bytes;
        }

        bytes = str.getBytes(StandardCharsets.ISO_8859_1);

        stringBytes.put(str, bytes);

        return bytes;
    }
}
