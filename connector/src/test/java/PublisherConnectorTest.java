import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class PublisherConnectorTest {

    public static void main(String[] args) throws InterruptedException {
        Connector publisherConnector = new Connector(byteBuf -> {
            OldHubMessage oldHubMessage = MessageHubAdapter.deserializeHeader(byteBuf);

            if (oldHubMessage.getMsgType() == MessageTypeOld.MESSAGE) {

            } else {
                OldHubMessage fullOldHubMessage = MessageHubAdapter.deserialize(byteBuf);
                System.out.println("Received msg: " + fullOldHubMessage);
            }
        });

        publisherConnector.start("localhost", 8080);

        for (int i = 0; i < 100_000; i++) { //warmup
            String message = "car message " + (i + 1);
            byte[] bytes = message.getBytes();

            OldHubMessage oldHubMessage = new OldHubMessage(MessageTypeOld.MESSAGE, "topic", new UnsafeBuffer(ByteBuffer.wrap(bytes)), bytes.length);
            publisherConnector.publish(oldHubMessage);

            if (i % 1000 == 0) {
                System.out.println("Sent: " + message);
            }
        }

        long startNano = System.nanoTime();
        int count = 15_000_000; //TODO FIX ME

        for (int i = 0; i < count; i++) {
            String message = "car message " + (i + 1);
            byte[] bytes = message.getBytes();

            OldHubMessage oldHubMessage = new OldHubMessage(MessageTypeOld.MESSAGE, "topic", new UnsafeBuffer(ByteBuffer.wrap(bytes)), bytes.length);
            publisherConnector.publish(oldHubMessage);

            if (i % 50000 == 0) {
                System.out.println("Sent: " + message);
            }
        }

        long endTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
        System.out.println("Done in " + endTimeMs + " ms");
        long rate = count / TimeUnit.MILLISECONDS.toSeconds(endTimeMs);
        System.out.println("msg rate is " + rate + " per second");


        publisherConnector.close();
    }
}
