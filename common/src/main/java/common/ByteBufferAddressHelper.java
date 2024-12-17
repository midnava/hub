package common;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public class ByteBufferAddressHelper {

    public static long getAddress(ByteBuffer buffer) {
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("ByteBuffer is not a direct buffer");
        }

        try {
            Field addressField = Buffer.class.getDeclaredField("address");
            addressField.setAccessible(true);
            return (long) addressField.get(buffer);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get address of ByteBuffer", e);
        }
    }
}
