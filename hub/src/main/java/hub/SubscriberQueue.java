package hub;

import common.HubMessage;
import common.MessageRate;
import io.netty.channel.Channel;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class SubscriberQueue {
    public static final int MAX_CLIENT_QUEUE_CAPACITY = 1024 * 1024 * 5; //1M
    private final Channel channel;
    private final MessageRate messageRate;
    private final Queue<HubMessage> queue = new ManyToOneConcurrentArrayQueue<>(MAX_CLIENT_QUEUE_CAPACITY); //10M
    private final AtomicLong queueSize = new AtomicLong();

    public SubscriberQueue(Channel channel, MessageRate messageRate) {
        this.channel = channel;
        this.messageRate = messageRate;

        channel.eventLoop().scheduleAtFixedRate(channel::flush, 1, 1, TimeUnit.MILLISECONDS);
        Executors.newSingleThreadScheduledExecutor().submit(this::handleMessages);
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                System.out.println("-----------------------------------");
                System.out.println("Queue is for chanel [" + channel.remoteAddress() + "]: " + queueSize);
                System.out.println("-----------------------------------");
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    public boolean isActive() {
        return channel.isActive();
    }

    public void addMessage(HubMessage msg) {
        boolean add = queue.add(msg);
        queueSize.incrementAndGet();

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
                        messageRate.incrementServerPubMsgRate();
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

    public Channel getChannel() {
        return channel;
    }

    public void close() {
        try {
            channel.close().sync();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
