import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class PublisherConnectorTest {

    public static void main(String[] args) throws InterruptedException {
        Connector publisherConnector = new Connector(byteBuf -> {
            HubMessage hubMessage = MessageHubAdapter.deserializeHeader(byteBuf);

            if (hubMessage.getMsgType() == MessageTypeOld.MESSAGE) {

            } else {
                HubMessage fullHubMessage = MessageHubAdapter.deserialize(byteBuf);
                System.out.println("Received msg: " + fullHubMessage);
            }
        });

        publisherConnector.start("localhost", 8080);

        for (int i = 0; i < 100_000; i++) { //warmup
            String message = "car message " + (i + 1);
            byte[] bytes = message.getBytes();

            HubMessage hubMessage = new HubMessage(MessageTypeOld.MESSAGE, "topic", new UnsafeBuffer(ByteBuffer.wrap(bytes)), bytes.length);
            publisherConnector.publish(hubMessage);

            if (i % 1000 == 0) {
                System.out.println("Sent: " + message);
            }
        }

        long startNano = System.nanoTime();
        int count = 15_000_000; //TODO FIX ME

        for (int i = 0; i < count; i++) {
            String message = "car message " + (i + 1);
            byte[] bytes = message.getBytes();

            HubMessage hubMessage = new HubMessage(MessageTypeOld.MESSAGE, "topic", new UnsafeBuffer(ByteBuffer.wrap(bytes)), bytes.length);
            publisherConnector.publish(hubMessage);

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
