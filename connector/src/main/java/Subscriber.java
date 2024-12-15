// Subscriber.java

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

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
                            pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                    HubMessage hubMessage = MessageHubAdapter.deserialize(msg);
                                    System.out.println("Received message: " + hubMessage);
                                }

                                @Override
                                public void channelActive(ChannelHandlerContext ctx) {
                                    System.out.println("Connected to server");

                                    HubMessage hubMessage = new HubMessage(MessageType.SUBSCRIBE, "topic");
                                    ByteBuf byteBuf = MessageHubAdapter.serialize(hubMessage);

                                    ctx.writeAndFlush(byteBuf)
                                            .addListener(future -> {
                                                if (future.isSuccess()) {
                                                    System.out.println("Subscription message sent");
                                                } else {
                                                    System.err.println("Failed to send subscription message: " + future.cause());
                                                    future.cause().printStackTrace();
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
