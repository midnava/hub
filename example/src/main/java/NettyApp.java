import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// Server Implementation
class NettyServer {
    private final int port;

    public NettyServer(int port) {
        this.port = port;
    }

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
//                    .option(ChannelOption.SO_LINGER, 5)
//                    .option(ChannelOption.SO_BACKLOG, 1024)
//                    .option(ChannelOption.SO_RCVBUF, 1048576)
//                    .option(ChannelOption.SO_SNDBUF, 1048576)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new MessageDecoder(), new ServerHandler());
                        }
                    })
                    .validate();

            ChannelFuture future = bootstrap.bind(port).sync();
            System.out.println("Server started on port: " + port);
            future.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private static class ServerHandler extends SimpleChannelInboundHandler<Message> {
        private static final AtomicInteger counter = new AtomicInteger();

        static {
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
                int c = counter.getAndSet(0);
                if (c > 0) {
                    System.out.println("Msg rate is " + c);
                }
            }, 0, 1, TimeUnit.SECONDS);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
            counter.incrementAndGet();
//            System.out.println("Received message: topic=" + msg.topic + ", seqNo=" + msg.seqNo);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
}

// Client Implementation
class NettyClient {
    private final String host;
    private final int port;
    private Channel channel;
    private EventLoopGroup group;

    public NettyClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws InterruptedException {
        group = new NioEventLoopGroup(1);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
//                .option(ChannelOption.SO_BACKLOG, 1024)
//                .option(ChannelOption.SO_LINGER, 5)
//                .option(ChannelOption.SO_RCVBUF, 1048576)
//                .option(ChannelOption.SO_SNDBUF, 1048576)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new MessageEncoder());
                    }
                })
                .validate();

        ChannelFuture future = bootstrap.connect(host, port).sync();
        channel = future.channel();
        channel.eventLoop().scheduleAtFixedRate(() -> channel.flush(), 1, 1, TimeUnit.MILLISECONDS);
    }

    public void publish(String topic, int seqNo) {
        if (channel != null && channel.isActive()) {
            channel.write(new Message(topic, seqNo));
        }
    }

    public void disconnect() {
        if (channel != null) {
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
    }
}

// Message Model
class Message {
    public final String topic;
    public final int seqNo;

    public Message(String topic, int seqNo) {
        this.topic = topic;
        this.seqNo = seqNo;
    }
}

// Message Encoder
class MessageEncoder extends MessageToByteEncoder<Message> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) {
        byte[] topicBytes = msg.topic.getBytes(StandardCharsets.UTF_8);
        out.writeInt(topicBytes.length);
        out.writeBytes(topicBytes);
        out.writeInt(msg.seqNo);
    }
}

// Message Decoder
class MessageDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < 8) {
            return;
        }

        in.markReaderIndex();
        int topicLength = in.readInt();

        if (in.readableBytes() < topicLength + 4) {
            in.resetReaderIndex();
            return;
        }

        byte[] topicBytes = new byte[topicLength];
        in.readBytes(topicBytes);
        int seqNo = in.readInt();

        out.add(new Message(new String(topicBytes, StandardCharsets.UTF_8), seqNo));
    }
}

// Main Class to Test
public class NettyApp {
    public static void main(String[] args) throws InterruptedException {
        int port = 8080;

        // Start Server in a Separate Thread
        Thread thread = new Thread(() -> {
            try {
                NettyServer nettyServer = new NettyServer(port);
                nettyServer.start();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        thread.setDaemon(true);
        thread.start();

        // Give the Server Some Time to Start
        System.out.println("WAITING");
        Thread.sleep(5_000);
        System.out.println("STARTED");


        // Start Client, Connect, Publish Multiple Messages, and Disconnect
        NettyClient client = new NettyClient("localhost", port);
        client.connect();

        int totalMessages = 50_000_000;

        for (int i = 0; i < totalMessages; i++) {
            client.publish("topic", i);
        }

        Thread.sleep(10_000);

        System.out.println("All messages sent successfully");
        client.disconnect();
        thread.interrupt();
        thread.stop();
    }
}
