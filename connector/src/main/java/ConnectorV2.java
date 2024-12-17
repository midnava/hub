import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import v2.MessageDecoder;
import v2.MessageEncoder;
import v2.MessageRate;
import v2.NettyHubMessage;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

public class ConnectorV2 {
    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private Channel channel;
    private final Consumer<NettyHubMessage> messageConsumer;

    public ConnectorV2(Consumer<NettyHubMessage> messageConsumer) {
        this.messageConsumer = messageConsumer;

//        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    public void start(String host, int port) throws InterruptedException {
        Bootstrap bootstrap = new Bootstrap();
        ByteBufAllocator allocator = new UnpooledByteBufAllocator(true);

        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_RCVBUF, 4 * 1024 * 1024)
                .option(ChannelOption.SO_SNDBUF, 4 * 1024 * 1024)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(1024 * 1024, 4 * 1024 * 1024))
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.AUTO_CLOSE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(
                                64 * 1024, // Максимальная длина сообщения (защитный лимит)
                                0,               // Смещение длины в сообщении (начало буфера)
                                4,               // Размер поля длины (int)
                                0,               // Смещение добавленной длины (нет дополнительных байт)
                                4                // Байты длины включаются в итоговое сообщение)
                        ));
                        ch.pipeline().addLast(new ReconnectClientHandler(bootstrap, host, port));
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

    public void publish(NettyHubMessage nettyHubMessage) {
        if (channel != null && channel.isActive()) {
            while (!channel.isWritable()) {
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(500));
            }
            channel.write(nettyHubMessage);
            MessageRate.instance.incrementPubMsgRate();
        } else {
            //throw new IllegalArgumentException("Transport is not ready");
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

    private class ClientHandler extends SimpleChannelInboundHandler<NettyHubMessage> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, NettyHubMessage msg) {
            // Simulate processing
            MessageRate.instance.incrementSubMsgRate();
            messageConsumer.accept(msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
}
