import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.function.Consumer;

public class Connector {
    private final EventLoopGroup group = new NioEventLoopGroup(0);
    private Channel ch;
    private final Consumer<ByteBuf> messageConsumer;

    public Connector(Consumer<ByteBuf> messageConsumer) {
        this.messageConsumer = messageConsumer;

//        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    public void start(String host, int port) throws InterruptedException {
        Bootstrap b = new Bootstrap();

        b.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.SO_BACKLOG, 1024) // Серверная очередь соединений
                .handler(new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ch.pipeline()
                                .addLast(new LengthFieldBasedFrameDecoder(2048, 0, 2, 0, 2))
                                .addLast(new LengthFieldPrepender(2))
                                .addLast(new ReconnectClientHandler(b, host, port))
                                .addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                        messageConsumer.accept(msg);
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
                })
                .validate();

        this.ch = b.connect(host, port).sync().channel();
//        ch.eventLoop().scheduleAtFixedRate(() -> ch.flush(), 0, 1, TimeUnit.MILLISECONDS);

        System.out.println("Publisher is starting...");
    }

    public void publish(HubMessage hubMessage) throws InterruptedException {
        ByteBuf byteBuf = MessageHubAdapter.serialize(hubMessage, ch.alloc().buffer(hubMessage.getMsgBytesLength() + 128));

        ch.writeAndFlush(byteBuf);
    }

    public void subscribe(String topic) throws InterruptedException {
        HubMessage hubMessage = new HubMessage(MessageType.SUBSCRIBE, topic);
        publish(hubMessage);
    }

    public void unsubscribe(String topic) throws InterruptedException {
        HubMessage hubMessage = new HubMessage(MessageType.UNSUBSCRIBE, topic);
        publish(hubMessage);
    }

    public void close() throws InterruptedException {
        ch.closeFuture().sync();
        group.shutdownGracefully();
    }
}
