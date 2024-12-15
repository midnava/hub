// Publisher.java

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

public class Publisher {
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
                            pipeline.addLast(new SimpleChannelInboundHandler<Object>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                                    System.out.println("Received: " + msg);
                                }
                            });
                        }
                    });

            System.out.println("Publisher is starting...");
            Channel ch = b.connect(HOST, PORT).sync().channel();
            System.out.println("Connected to server");

            for (int i = 0; i < 10; i++) {
                String message = "car message " + (i + 1);
                ch.writeAndFlush(message).sync();
                System.out.println("Sent: " + message);
                Thread.sleep(1000); // Slight delay between messages
            }

            ch.closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }
}
