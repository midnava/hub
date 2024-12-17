package hub;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.concurrent.TimeUnit;

public class SubscriberQueue {
    private final Channel ch;


    public SubscriberQueue(Channel ch) {
        this.ch = ch;
        ch.eventLoop().scheduleAtFixedRate(() -> ch.flush(), 1, 2, TimeUnit.MILLISECONDS);
    }

    public boolean isActive() {
        return ch.isActive();
    }

    public void addMessage(ByteBuf msg) {
        msg.retain();
        handleMessage(msg);
    }

    private void handleMessage(ByteBuf message) {
        ch.write(message);
    }

    public void close() throws InterruptedException {
        ch.close().sync();
    }
}
