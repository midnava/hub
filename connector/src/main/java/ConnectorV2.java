import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import v2.Message;
import v2.MessageDecoder;
import v2.MessageEncoder;
import v2.MessageRate;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

public class ConnectorV2 {
    private final EventLoopGroup group = new NioEventLoopGroup(0);
    private Channel channel;
    private final Consumer<ByteBuf> messageConsumer;

    public ConnectorV2(Consumer<ByteBuf> messageConsumer) {
        this.messageConsumer = messageConsumer;

//        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    public void start(String host, int port) throws InterruptedException {
        Bootstrap bootstrap = new Bootstrap();
        ByteBufAllocator allocator = new UnpooledByteBufAllocator(true);

        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_RCVBUF, 2 * 1024 * 1024)
                .option(ChannelOption.SO_SNDBUF, 2 * 1024 * 1024)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(256 * 1024, 512 * 1024))
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.AUTO_CLOSE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new MessageDecoder(), new ClientHandler());
                        ch.pipeline().addLast(new MessageEncoder());
                        ch.config().setAllocator(allocator);
                    }
                })
                .validate();

        ChannelFuture future = bootstrap.connect(host, port).sync();
        channel = future.channel();
        channel.eventLoop().scheduleAtFixedRate(() -> channel.flush(), 1, 1, TimeUnit.MILLISECONDS);

        System.out.println("Publisher is starting...");
    }

    public void publish(Message message) {
        if (channel != null && channel.isActive()) {
            while (!channel.isWritable()) {
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(500));
            }
            channel.write(message);
            MessageRate.instance.incrementPubMsgRate();
        }
    }

    public void subscribe(String topic) {
//        HubMessage hubMessage = new HubMessage(MessageType.SUBSCRIBE, topic);
//        publish(hubMessage);
    }

    public void unsubscribe(String topic) {
//        HubMessage hubMessage = new HubMessage(MessageType.UNSUBSCRIBE, topic);
//        publish(hubMessage);
    }

    public void close() {
        if (channel != null) {
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
    }

    private static class ClientHandler extends SimpleChannelInboundHandler<Message> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
            // Simulate processing
            MessageRate.instance.incrementSubMsgRate();
            if (msg.seqNo % 1_000_000 == 0) {
                System.out.println("Received message: " + msg.seqNo);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
}
