package common;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageRate {
    private final AtomicInteger pubMsgRate = new AtomicInteger();
    private final AtomicInteger subMsgRate = new AtomicInteger();
    private final AtomicInteger serverPubMsgRate = new AtomicInteger();
    private final AtomicInteger serverSubMsgRate = new AtomicInteger();

    public static final MessageRate instance = new MessageRate();

    public MessageRate() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Date date = new Date();

                int pubRate = pubMsgRate.getAndSet(0);
                if (pubRate > 0) {
                    System.out.println(date + ": pubMsgRate is " + pubRate);
                }

                int subMsg = subMsgRate.getAndSet(0);
                if (subMsg > 0) {
                    System.out.println(date + ": subMsgRate is " + subMsg);
                }

                int serverPubMsg = serverPubMsgRate.getAndSet(0);
                if (serverPubMsg > 0) {
                    System.out.println(date + ": serverPubMsgRate is " + serverPubMsg);
                }

                int serverSubMsg = serverSubMsgRate.getAndSet(0);
                if (serverSubMsg > 0) {
                    System.out.println(date + ": serverSubMsgRate is " + serverSubMsg);
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    public void incrementPubMsgRate() {
        pubMsgRate.incrementAndGet();
    }

    public void incrementSubMsgRate() {
        subMsgRate.incrementAndGet();
    }

    public void incrementServerPubMsgRate() {
        serverPubMsgRate.incrementAndGet();
    }

    public void incrementServerSubMsgRate() {
        serverSubMsgRate.incrementAndGet();
    }
}
