import common.HubMessage;
import common.MessageType;
import connector.Connector;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class PublisherConnector4PublishersTest {

    public static final String TOPIC = "topic";

    public static void main(String[] args) {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        for (int j = 0; j < 4; j++) {
            int finalJ = j;

            Executors.newSingleThreadScheduledExecutor().execute(() -> {
                try {
                    AtomicInteger currentMsgRate = new AtomicInteger();
                    executorService.scheduleAtFixedRate(() -> currentMsgRate.set(0), 0, 1, TimeUnit.SECONDS);

                    Connector publisherConnector = new Connector("Connector-" + finalJ, message -> System.out.println("Pub IN: " + message));

                    publisherConnector.start("localhost", 8080);

                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));

                    UnsafeBuffer msgBytes = new UnsafeBuffer(ByteBuffer.allocate(128 * 1024));
                    int msgLength = 1024 * 2;
                    StringBuilder builder = new StringBuilder(msgLength);
                    builder.append("Hello Netty");
                    for (int i = 0; i < msgLength; i++) {
                        builder.append(i);
                    }
                    String msg = builder.toString();
                    int length = msgBytes.putStringAscii(0, msg);

                    int warmUpCount = 50_0000;
                    for (int i1 = 0; i1 < warmUpCount; i1++) { //warmup
                        publisherConnector.publish(new HubMessage(MessageType.MESSAGE, TOPIC, i1, msgBytes, 0, length));
                    }

                    long startNano = System.nanoTime();
                    int count = 500_000_000; //TODO FIX ME
                    int msgRate = 25_000;

                    for (int i1 = 0; i1 < count; i1++) {

                        int c = currentMsgRate.incrementAndGet();
                        if (c < msgRate) {
                            publisherConnector.publish(new HubMessage(MessageType.MESSAGE, TOPIC, i1 + warmUpCount, msgBytes, 0, length));
                        } else {
                            while (currentMsgRate.get() > 0) {
                                Thread.yield();
                            }
                        }

                        if (i1 % 1_000_000 == 0) {
                            System.out.println("Sent and sleep: " + i1);
                            Thread.sleep(5000);
                        }
                    }

                    long endTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
                    System.out.println("Done in " + endTimeMs + " ms");
                    long rate = count / TimeUnit.MILLISECONDS.toSeconds(endTimeMs);
                    System.out.println("msg rate is " + rate + " per second");


                    publisherConnector.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
