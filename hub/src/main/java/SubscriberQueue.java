import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class SubscriberQueue {
    private final Channel ch;
    private final BlockingQueue<ByteBuf> queue;
    private final Thread thread;

    public SubscriberQueue(Channel ch) {
        this.ch = ch;
        this.queue = new ArrayBlockingQueue<>(1024 * 500); //500K msg capacity

        this.thread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    ByteBuf message = queue.take();
                    handleMessage(message);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        thread.start();
    }

    public boolean isActive() {
        return ch.isActive();
    }

    public void addMessage(ByteBuf msg) {
        msg.retain();
        handleMessage(msg);
//        try {
//            queue.put(msg);
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//        }
    }

    private void handleMessage(ByteBuf message) {
        ch.writeAndFlush(message).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    public void close() {
        thread.interrupt();
    }
}
