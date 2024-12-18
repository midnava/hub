package hub;

import common.HubMessage;
import common.MessageRate;
import io.netty.channel.Channel;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class SubscriberQueue {
    private final Channel ch;
    private final Queue<HubMessage> queue = new ConcurrentLinkedQueue<>();

    public SubscriberQueue(Channel ch, ExecutorService executor) {
        this.ch = ch;
        ch.eventLoop().scheduleAtFixedRate(ch::flush, 1, 1, TimeUnit.MILLISECONDS);
        executor.submit(this::handleMessages);
    }

    public boolean isActive() {
        return ch.isActive();
    }

    public void addMessage(HubMessage msg) {
        queue.add(msg);
    }

    private void handleMessages() {
        try {
            while (ch.isActive()) {
                HubMessage message = queue.poll();
                if (message != null) {
                    MessageRate.instance.incrementServerPubMsgRate();
                    ch.write(message);
                } else {
                    LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(250));
                }
            }
        } catch (Exception e) {
            System.err.println("Error processing queue for channel: " + e.getMessage());
        }
    }

    public void close() throws InterruptedException {
        ch.close().sync();
    }
}
