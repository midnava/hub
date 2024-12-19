import common.HubMessage;
import common.MessageType;
import connector.Connector;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class PublisherConnector100MsgTest {

    public static final String TOPIC = "topic";

    public static void main(String[] args) throws InterruptedException {
        Connector publisherConnector = new Connector(new Consumer<HubMessage>() {
            @Override
            public void accept(HubMessage message) {
                System.out.println("Pub IN: " + message);
            }
        });

        publisherConnector.start("localhost", 8080);

        Thread.sleep(1000);

        int capacity = 256;
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(capacity));
        for (int i = 0; i < capacity; i++) {
            buffer.putByte(i, (byte) i);
        }

        UnsafeBuffer msgBytes = new UnsafeBuffer(ByteBuffer.allocate(128 * 1024));
        int length = msgBytes.putStringAscii(0, "Hello Netty");

        int count = 100; //TODO FIX ME

        for (int i1 = 0; i1 < count; i1++) {
            publisherConnector.publish(new HubMessage(MessageType.MESSAGE, TOPIC, i1, msgBytes, 0, length));
        }

        Thread.sleep(30_000);
        publisherConnector.close();
    }
}
