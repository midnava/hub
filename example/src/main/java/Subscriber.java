// Subscriber.java

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

public class Subscriber {
    private static final String HOST = "localhost";
    private static final int PORT = 8080;

    public static void main(String[] args) throws InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new LengthFieldBasedFrameDecoder(65535, 0, 2, 0, 2));
                            pipeline.addLast(new LengthFieldPrepender(2));
                            pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8));
                            pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8));
                            pipeline.addLast(new SimpleChannelInboundHandler<String>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, String msg) {
                                    System.out.println("Received message: " + msg);
                                }

                                @Override
                                public void channelActive(ChannelHandlerContext ctx) {
                                    System.out.println("Connected to server");
                                    ctx.writeAndFlush("SUBSCRIBE car")
                                            .addListener(future -> {
                                                if (future.isSuccess()) {
                                                    System.out.println("Subscription message sent");
                                                } else {
                                                    System.err.println("Failed to send subscription message: " + future.cause());
                                                }
                                            });
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) {
                                    System.out.println("Disconnected from server");
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    cause.printStackTrace();
                                    ctx.close();
                                }
                            });
                        }
                    });

            System.out.println("Subscriber is starting...");
            Channel ch = b.connect(HOST, PORT).sync().channel();
            System.out.println("Connected to server");

            // Keep the subscriber running
            ch.closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }
}
