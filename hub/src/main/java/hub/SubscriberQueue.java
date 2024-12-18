package hub;

import common.HubMessage;
import io.netty.channel.Channel;

import java.util.concurrent.TimeUnit;

public class SubscriberQueue {
    private final Channel ch;


    public SubscriberQueue(Channel ch) {
        this.ch = ch;
        ch.eventLoop().scheduleAtFixedRate(ch::flush, 1, 1, TimeUnit.MILLISECONDS);
    }

    public boolean isActive() {
        return ch.isActive();
    }

    public void addMessage(HubMessage msg) {
        handleMessage(msg);
    }

    private void handleMessage(HubMessage message) {
        ch.write(message);
    }

    public void close() throws InterruptedException {
        ch.close().sync();
    }
}
