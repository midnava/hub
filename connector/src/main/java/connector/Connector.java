package connector;

import common.HubMessage;
import common.MessageRate;
import common.MessageType;
import connector.adapters.MessageConnectorDecoder;
import connector.adapters.MessageConnectorEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

public class Connector {
    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private Channel channel;
    private final Consumer<HubMessage> messageConsumer;
    private final MessageRate messageRate = new MessageRate("Connector");

    public Connector(Consumer<HubMessage> messageConsumer) {
        this.messageConsumer = messageConsumer;

//        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    public void start(String host, int port) throws InterruptedException {
        Bootstrap bootstrap = new Bootstrap();

        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_RCVBUF, 64 * 1024 * 1024)
                .option(ChannelOption.SO_SNDBUF, 64 * 1024 * 1024)
//                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(1024 * 1024 * 2, 64 * 1024 * 1024))
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.AUTO_CLOSE, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator())
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
                        ch.pipeline().addLast(new MessageConnectorDecoder(), new ClientHandler());
                        ch.pipeline().addLast(new MessageConnectorEncoder());
                    }
                })
                .validate();

        ChannelFuture future = bootstrap.connect(host, port).sync();
        channel = future.channel();
        channel.eventLoop().scheduleAtFixedRate(() -> channel.flush(), 1, 1, TimeUnit.MILLISECONDS);

        System.out.println("Netty Connector is starting...");
    }

    public void publish(HubMessage hubMessage) { //TODO FIX ME SeqNO - publish only UnsafeBuffer
        if (channel != null && channel.isActive()) {
            while (!channel.isWritable()) {
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(500));
            }
            channel.write(hubMessage);
            messageRate.incrementConnectorPubMsgRate();
        } else {
            throw new IllegalArgumentException("Netty Connector is not ready");
        }
    }

    public void subscribe(String topic, String appName) {
        HubMessage hubMessage = new HubMessage(MessageType.SUBSCRIBE, topic, 0, appName); //TODO FIX ME SeqNO
        publish(hubMessage);
    }

    public void unsubscribe(String topic, String appName) {
        HubMessage hubMessage = new HubMessage(MessageType.UNSUBSCRIBE, topic, 0, appName); //TODO FIX ME SeqNO
        publish(hubMessage);
    }

    public void close() {
        if (channel != null) {
            channel.close();
        }

        group.shutdownGracefully();
    }

    private class ClientHandler extends SimpleChannelInboundHandler<HubMessage> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, HubMessage msg) {
            messageRate.incrementConnectorSubMsgRate();
            messageConsumer.accept(msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
}
