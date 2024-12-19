import common.HubMessage;
import common.MessageType;
import connector.Connector;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class PublisherConnectorMaxLoadTest {

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

        int warmUpCount = 1000;
        for (int i = 0; i < warmUpCount; i++) { //warmup
            publisherConnector.publish(new HubMessage(MessageType.MESSAGE, TOPIC, i, msgBytes, 0, length));
        }

        long startNano = System.nanoTime();
        int count = 50_000_000; //TODO FIX ME

        for (int i1 = 0; i1 < count; i1++) {
            publisherConnector.publish(new HubMessage(MessageType.MESSAGE, TOPIC, i1 + warmUpCount, msgBytes, 0, length));

            if (i1 % 1_000_000 == 0) {
                System.out.println("Sent: " + i1);
            }
        }

        long endTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
        System.out.println("Done in " + endTimeMs + " ms");
        long rate = count / TimeUnit.MILLISECONDS.toSeconds(endTimeMs);
        System.out.println("msg rate is " + rate + " per second");


        publisherConnector.close();
    }
}
