package hub;

import common.HubMessage;
import common.MessageRate;
import io.netty.channel.Channel;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class SubscriberQueue {
    public static final int MAX_CLIENT_QUEUE_CAPACITY = 1024 * 1024 * 10;
    private final Channel channel;
    private final Queue<HubMessage> queue = new ArrayDeque<>(MAX_CLIENT_QUEUE_CAPACITY); //10M
    private final AtomicLong queueSize = new AtomicLong();

    public SubscriberQueue(Channel channel) {
        this.channel = channel;
        channel.eventLoop().scheduleAtFixedRate(channel::flush, 1, 1, TimeUnit.MILLISECONDS);
        Executors.newSingleThreadScheduledExecutor().submit(this::handleMessages);
    }

    public boolean isActive() {
        return channel.isActive();
    }

    public void addMessage(HubMessage msg) {
        channel.write(msg);
        boolean add = queue.add(msg);
        if (!add) {
            System.err.println("Huge queue on client side");
            close();
        }
    }

    private void handleMessages() {
        try {
            while (channel.isActive()) {
                HubMessage message = queue.poll();
                if (message != null) {
                    if (channel.isActive()) {
                        while (!channel.isWritable()) {
                            LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(250));
                        }

                        channel.write(message);
                        MessageRate.instance.incrementServerPubMsgRate();
                        queueSize.decrementAndGet();
                    } else {
                        throw new IllegalArgumentException("Netty Connector is not ready");
                    }
                } else {
                    LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(250));
                }
            }
        } catch (Exception e) {
            System.err.println("Error processing queue for channel: " + e.getMessage());
        }
    }

    public void close() {
        try {
            channel.close().sync();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
