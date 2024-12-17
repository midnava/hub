import org.agrona.concurrent.UnsafeBuffer;
import v2.Message;
import v2.MessageType;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class PublisherConnectorV2Test {

    public static void main(String[] args) throws InterruptedException {
        ConnectorV2 publisherConnector = new ConnectorV2(new Consumer<Message>() {
            @Override
            public void accept(Message message) {
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

        for (int i = 0; i < 100_000; i++) { //warmup
            publisherConnector.publish(new Message(MessageType.MESSAGE, "topic", buffer, 0, capacity));
        }

        long startNano = System.nanoTime();
        int count = 50_000_000; //TODO FIX ME

        for (int i = 0; i < count; i++) {
            publisherConnector.publish(new Message(MessageType.MESSAGE, "topic", buffer, 0, capacity));

            if (i % 1_000_000 == 0) {
                System.out.println("Sent: " + i);
            }
        }

        long endTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
        System.out.println("Done in " + endTimeMs + " ms");
        long rate = count / TimeUnit.MILLISECONDS.toSeconds(endTimeMs);
        System.out.println("msg rate is " + rate + " per second");


        publisherConnector.close();
    }
}
