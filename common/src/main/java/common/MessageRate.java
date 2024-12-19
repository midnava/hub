package common;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageRate {
    private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor();

    private final AtomicInteger connectorPubMsgRate = new AtomicInteger();
    private final AtomicInteger connectorSubMsgRate = new AtomicInteger();
    private final AtomicInteger hubPubMsgRate = new AtomicInteger();
    private final AtomicInteger hubSubMsgRate = new AtomicInteger();


    public MessageRate(String name) {
        SCHEDULED_EXECUTOR_SERVICE.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Date date = new Date();
                String prefix = date + " : " + name + ": ";

                int pubRate = connectorPubMsgRate.getAndSet(0);
                if (pubRate > 0) {
                    System.out.println(prefix + "pubMsgRate is " + pubRate);
                }

                int subMsg = connectorSubMsgRate.getAndSet(0);
                if (subMsg > 0) {
                    System.out.println(prefix + "subMsgRate is " + subMsg);
                }

                int serverPubMsg = hubPubMsgRate.getAndSet(0);
                if (serverPubMsg > 0) {
                    System.out.println(prefix + "hubPubMsgRate is " + serverPubMsg);
                }

                int serverSubMsg = hubSubMsgRate.getAndSet(0);
                if (serverSubMsg > 0) {
                    System.out.println(prefix + "hubSubMsgRate is " + serverSubMsg);
                }

                int total = pubRate + subMsg + +serverSubMsg + serverPubMsg;
                if (total == 0) {
                    System.out.println(prefix + "total is " + total);
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    public void incrementConnectorPubMsgRate() {
        connectorPubMsgRate.incrementAndGet();
    }

    public void incrementConnectorSubMsgRate() {
        connectorSubMsgRate.incrementAndGet();
    }

    public void incrementServerPubMsgRate() {
        hubPubMsgRate.incrementAndGet();
    }

    public void incrementServerSubMsgRate() {
        hubSubMsgRate.incrementAndGet();
    }
}
