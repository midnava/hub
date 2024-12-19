import common.HubMessage;
import connector.Connector;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class SubscriberConnectorWithLoggingTest {

    public static void main(String[] args) throws InterruptedException {
        Connector connector = new Connector(new Consumer<HubMessage>() {
            @Override
            public void accept(HubMessage message) {
                System.out.println("In Message: " + message.getSeqNo() + ", msg=" + message.getByteBuf().getStringAscii(0));
            }
        });

        connector.start("localhost", 8080);

        Thread.sleep(1000);

        connector.subscribe(PublisherConnectorTest.TOPIC, "appName");

        Thread.sleep(TimeUnit.SECONDS.toMillis(180));
    }
}
