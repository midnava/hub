import v2.Message;

import java.util.concurrent.TimeUnit;

public class PublisherConnectorV2Test {

    public static void main(String[] args) throws InterruptedException {
        ConnectorV2 publisherConnector = new ConnectorV2(byteBuf -> {
            HubMessage hubMessage = MessageHubAdapter.deserializeHeader(byteBuf);

            if (hubMessage.getMsgType() == MessageType.MESSAGE) {

            } else {
                HubMessage fullHubMessage = MessageHubAdapter.deserialize(byteBuf);
                System.out.println("Received msg: " + fullHubMessage);
            }
        });

        publisherConnector.start("localhost", 8080);

        for (int i = 0; i < 100_000; i++) { //warmup

            publisherConnector.publish(new Message("topic", i));

            if (i % 1000 == 0) {
                System.out.println("Sent: " + i);
            }
        }

        long startNano = System.nanoTime();
        int count = 15_000_000; //TODO FIX ME

        for (int i = 0; i < count; i++) {
            publisherConnector.publish(new Message("topic", i));

            if (i % 1000 == 0) {
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
