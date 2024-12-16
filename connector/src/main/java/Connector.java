import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class Connector {
    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private Channel ch;

    public void start(String host, int port) throws InterruptedException {
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
                })
                .validate();

        this.ch = b.connect(host, port).sync().channel();

        System.out.println("Publisher is starting...");
    }

    public void publish(HubMessage hubMessage) throws InterruptedException {
        ByteBuf byteBuf = MessageHubAdapter.serialize(hubMessage);

        ch.writeAndFlush(byteBuf);
    }

    public void close() throws InterruptedException {
        ch.closeFuture().sync();
        group.shutdownGracefully();
    }


    public static void main(String[] args) throws InterruptedException {
        Connector connector = new Connector();
        connector.start("localhost", 8080);

        for (int i = 0; i < 50_000; i++) { //warmup
            String message = "car message " + (i + 1);
            byte[] bytes = message.getBytes();

            HubMessage hubMessage = new HubMessage(MessageType.MESSAGE, "topic", ByteBuffer.wrap(bytes));
            connector.publish(hubMessage);

            if (i % 1000 == 0) {
                System.out.println("Sent: " + message);
            }
        }

        long startNano = System.nanoTime();
        int count = 1_000_000;

        for (int i = 0; i < count; i++) {
            String message = "car message " + (i + 1);
            byte[] bytes = message.getBytes();

            HubMessage hubMessage = new HubMessage(MessageType.MESSAGE, "topic", ByteBuffer.wrap(bytes));
            connector.publish(hubMessage);

            if (i % 25000 == 0) {
                System.out.println("Sent: " + message);
            }
        }

        long endTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
        System.out.println("Done in " + endTime + " ms");
        long rate = count / endTime * 1000;
        System.out.println("msg rate is " + rate + " per second");


        connector.close();
    }
}
