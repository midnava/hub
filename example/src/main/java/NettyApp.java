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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

// Server Implementation
class NettyServer {
    private final int port;

    public NettyServer(int port) {
        this.port = port;
    }

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.SO_RCVBUF, 2 * 1024 * 1024)
                    .childOption(ChannelOption.SO_SNDBUF, 2 * 1024 * 1024)
                    .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(256 * 1024, 512 * 1024))
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new MessageDecoder(), new ServerHandler());
                        }
                    });

            ChannelFuture future = bootstrap.bind(port).sync();
            System.out.println("Server started on port: " + port);
            future.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private static class ServerHandler extends SimpleChannelInboundHandler<Message> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
            // Simulate processing
            MessageRate.instance.incrementServerSubMsgRate();
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

// Client Implementation
class NettyClient {
    private final String host;
    private final int port;
    private Channel channel;
    private EventLoopGroup group;
    private ScheduledExecutorService flushScheduler;

    public NettyClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws InterruptedException {
        group = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_RCVBUF, 2 * 1024 * 1024)
                .option(ChannelOption.SO_SNDBUF, 2 * 1024 * 1024)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(256 * 1024, 512 * 1024))
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new MessageEncoder());
                    }
                });

        ChannelFuture future = bootstrap.connect(host, port).sync();
        channel = future.channel();

        flushScheduler = Executors.newSingleThreadScheduledExecutor();
        flushScheduler.scheduleAtFixedRate(() -> {
            if (channel != null && channel.isActive()) {
                channel.flush();
            }
        }, 1, 1, TimeUnit.MILLISECONDS);
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

    public void disconnect() {
        if (channel != null) {
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
        if (flushScheduler != null) {
            flushScheduler.shutdown();
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
        int totalMessages = 100_000_000;

        // Start Server in a Separate Thread
        new Thread(() -> {
            try {
                new NettyServer(port).start();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        // Give the Server Some Time to Start
        Thread.sleep(1000);

        // Start Client, Connect, Publish Messages, and Disconnect
        NettyClient client = new NettyClient("localhost", port);
        client.connect();

        AtomicInteger counter = new AtomicInteger(1);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < totalMessages; i++) {
            client.publish(new Message("topic", counter.getAndIncrement()));
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Sent " + totalMessages + " messages in " + (endTime - startTime) + " ms");
        client.disconnect();
    }
}
