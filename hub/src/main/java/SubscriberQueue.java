import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;

public class SubscriberQueue {
    private final Channel ch;
    private final BlockingQueue<ByteBuf> queue;

    public SubscriberQueue(Channel ch) {
        this.ch = ch;
        this.queue = new ArrayBlockingQueue<>(2048);

        Executors.newVirtualThreadPerTaskExecutor().submit(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    ByteBuf message = queue.take();
                    handleMessage(message);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    public boolean isActive() {
        return ch.isActive();
    }

    public void addMessage(ByteBuf msg) {
        try {
            msg.retain();
            queue.put(msg);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void handleMessage(ByteBuf message) {
        ch.writeAndFlush(message).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }
}
