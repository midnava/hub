package connector;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;

import java.util.concurrent.TimeUnit;

public class ReconnectClientHandler extends ChannelInboundHandlerAdapter {
    private final Bootstrap bootstrap;
    private final String host;
    private final int port;

    public ReconnectClientHandler(Bootstrap bootstrap, String host, int port) {
        this.bootstrap = bootstrap;
        this.host = host;
        this.port = port;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        System.out.println("Channel inactive. Attempting to reconnect...");
        reconnect();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.err.println("Exception occurred: " + cause.getMessage());
        ctx.close();
    }

    private void reconnect() {
        EventLoop loop = bootstrap.config().group().next();
        loop.schedule(() -> {
            System.out.println("Reconnecting...");
            bootstrap.connect(host, port).addListener((ChannelFuture future) -> {
                if (future.isSuccess()) {
                    System.out.println("Reconnected successfully!");
                } else {
                    System.err.println("Reconnection failed. Retrying...");
                    reconnect();
                }
            });
        }, 5, TimeUnit.SECONDS);
    }
}
