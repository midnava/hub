import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledHeapByteBuf;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;


class MessageHubAdapterTest {

    @Test
    void serialize() {
        MessageTypeOld messageTypeOld = MessageTypeOld.MESSAGE;
        String topic = "MyTopic";
        ByteBuffer buffer = ByteBuffer.allocate(128);
        byte[] bytes = "Message".getBytes();
        buffer.put(bytes);

        OldHubMessage oldHubMessage = new OldHubMessage(messageTypeOld, topic, new UnsafeBuffer(buffer), bytes.length);

        ByteBuf serialized = MessageHubAdapter.serialize(oldHubMessage, new UnpooledHeapByteBuf(ByteBufAllocator.DEFAULT, 512, 1024));

        assertEquals(23, serialized.readableBytes());

        OldHubMessage newMsgHub = MessageHubAdapter.deserialize(serialized);

        assertEquals(MessageTypeOld.MESSAGE, newMsgHub.getMsgType());
        assertEquals(topic, newMsgHub.getTopic());

        UnsafeBuffer msgBytes = newMsgHub.getMsgBytes();
        for (int i = 0; i < bytes.length; i++) {
            assertEquals(bytes[i], msgBytes.getByte(i));
        }
    }
}