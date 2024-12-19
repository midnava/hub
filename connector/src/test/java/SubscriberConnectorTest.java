import common.HubMessage;
import connector.Connector;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class SubscriberConnectorTest {

    public static void main(String[] args) throws InterruptedException {
        Connector connector = new Connector(new Consumer<HubMessage>() {
            @Override
            public void accept(HubMessage message) {
                if (message.getSeqNo() % 100_000 == 0) {
                    System.out.println("In Message: " + message.getSeqNo() + ", msg=" + message.getByteBuf().getStringAscii(0, 4));
                }
            }
        });

        connector.start("localhost", 8080);

        Thread.sleep(1000);

        connector.subscribe(PublisherConnectorTest.TOPIC, "appName");

        Thread.sleep(TimeUnit.SECONDS.toMillis(180));
    }
}
