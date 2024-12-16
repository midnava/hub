// Publisher.java

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.nio.ByteBuffer;

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
                            pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                    HubMessage hubMessage = MessageHubAdapter.deserialize(msg);
                                    System.out.println("Received message: " + hubMessage);
                                }
                            });
                        }
                    });

            System.out.println("Publisher is starting...");
            Channel ch = b.connect(HOST, PORT).sync().channel();
            System.out.println("Connected to server");

            for (int i = 0; i < 10; i++) {
                String message = "car message " + (i + 1);
                byte[] bytes = message.getBytes();

                HubMessage hubMessage = new HubMessage(MessageType.MESSAGE, "topic", ByteBuffer.wrap(bytes));
                ByteBuf byteBuf = MessageHubAdapter.serialize(hubMessage);

                ch.writeAndFlush(byteBuf).sync();
                System.out.println("Sent: " + message);
                Thread.sleep(1000); // Slight delay between messages
            }

            ch.closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }
}