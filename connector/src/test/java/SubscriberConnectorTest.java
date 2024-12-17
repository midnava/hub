import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SubscriberConnectorTest {

    public static void main(String[] args) throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        AtomicLong totalCount = new AtomicLong();

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            int rate = counter.getAndSet(0);
            totalCount.addAndGet(rate);
            if (rate > 0) {
                System.out.println("Msg rate is " + rate + " msg/sec, total=" + totalCount);
            }
        }, 0, 1, TimeUnit.SECONDS);

        Connector subscriberConnector = new Connector(byteBuf -> {
            OldHubMessage oldHubMessage = MessageHubAdapter.deserialize(byteBuf);

            if (oldHubMessage.getMsgType() == MessageTypeOld.MESSAGE) {
                counter.incrementAndGet();
            } else {
                System.out.println("Received msg: " + oldHubMessage);
            }
        });

        subscriberConnector.start("localhost", 8080);
        subscriberConnector.subscribe("topic");

    }
}
