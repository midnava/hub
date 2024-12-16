import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class PublisherConnectorTest {

    public static void main(String[] args) throws InterruptedException {
        Connector publisherConnector = new Connector(byteBuf -> {
            HubMessage hubMessage = MessageHubAdapter.deserializeHeader(byteBuf);

            if (hubMessage.getMsgType() == MessageType.MESSAGE) {

            } else {
                HubMessage fullHubMessage = MessageHubAdapter.deserialize(byteBuf);
                System.out.println("Received msg: " + fullHubMessage);
            }
        });

        publisherConnector.start("localhost", 8080);

        for (int i = 0; i < 50_000; i++) { //warmup
            String message = "car message " + (i + 1);
            byte[] bytes = message.getBytes();

            HubMessage hubMessage = new HubMessage(MessageType.MESSAGE, "topic", ByteBuffer.wrap(bytes));
            publisherConnector.publish(hubMessage);

            if (i % 1000 == 0) {
                System.out.println("Sent: " + message);
            }
        }

        long startNano = System.nanoTime();
        int count = 1_000_000;

        for (int i = 0; i < count; i++) {
            String message = "car message " + (i + 1);
            byte[] bytes = message.getBytes();

            HubMessage hubMessage = new HubMessage(MessageType.MESSAGE, "topic", ByteBuffer.wrap(bytes));
            publisherConnector.publish(hubMessage);

            if (i % 25000 == 0) {
                System.out.println("Sent: " + message);
            }
        }

        long endTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
        System.out.println("Done in " + endTime + " ms");
        long rate = count / endTime * 1000;
        System.out.println("msg rate is " + rate + " per second");


        publisherConnector.close();
    }
}
