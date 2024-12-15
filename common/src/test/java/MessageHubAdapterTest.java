import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;


class MessageHubAdapterTest {

    @Test
    void serialize() {
        MessageType messageType = MessageType.MESSAGE;
        String topic = "MyTopic";
        ByteBuffer buffer = ByteBuffer.allocate(128);
        byte[] bytes = "Message".getBytes();
        buffer.put(bytes);

        HubMessage hubMessage = new HubMessage(messageType, topic, buffer);

        ByteBuf serialized = MessageHubAdapter.serialize(hubMessage);

        assertEquals(233, serialized.writableBytes());

        HubMessage newMsgHub = MessageHubAdapter.deserialize(serialized);

        assertEquals(MessageType.MESSAGE, newMsgHub.getMsgType());
        assertEquals(topic, newMsgHub.getTopic());

        ByteBuffer msgBytes = newMsgHub.getMsgBytes();
        for (int i = 0; i < bytes.length; i++) {
            assertEquals(bytes[i], msgBytes.get(i));
        }

    }

}