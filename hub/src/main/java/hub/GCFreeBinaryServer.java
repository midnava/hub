package hub;

import common.MessageRate;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GCFreeBinaryServer {
    private final int port;
    private final Map<Channel, Queue<ByteBuf>> clientQueues = new ConcurrentHashMap<>();
    private final ExecutorService virtualThreadExecutor;
    private final MessageRate messageRate = new MessageRate("GCFreeBinaryServer");

    public GCFreeBinaryServer(int port) {
        this.port = port;
        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.SO_RCVBUF, 2 * 1024 * 1024)
                    .childOption(ChannelOption.SO_SNDBUF, 2 * 1024 * 1024)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(
                                    64 * 1024, // Максимальная длина сообщения (защитный лимит)
                                    0,               // Смещение длины в сообщении (начало буфера)
                                    4,               // Размер поля длины (int)
                                    0,               // Смещение добавленной длины (нет дополнительных байт)
                                    4                // Байты длины включаются в итоговое сообщение)
                            ));
                            ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                    messageRate.incrementServerSubMsgRate();
                                    // Parse topic and prepare message for broadcast
                                    String topic = extractTopic(msg);
                                    if (topic != null) {
                                        ByteBuf copiedMessage = msg.retainedDuplicate();
                                        broadcastMessage(topic, copiedMessage);
                                    }
                                }

                                @Override
                                public void channelActive(ChannelHandlerContext ctx) {
                                    Channel channel = ctx.channel();
                                    clientQueues.put(channel, new ConcurrentLinkedQueue<>());
                                    virtualThreadExecutor.submit(() -> processClientQueue(channel));
                                    System.out.println("Client connected: " + channel.remoteAddress());
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) {
                                    Channel channel = ctx.channel();
                                    clientQueues.remove(channel);
                                    System.out.println("Client disconnected: " + channel.remoteAddress());
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    cause.printStackTrace();
                                    ctx.close();
                                }
                            });
                        }
                    });

            ChannelFuture future = bootstrap.bind(port).sync();
            System.out.println("Server started on port: " + port);
            future.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            virtualThreadExecutor.shutdown();
        }
    }

    private String extractTopic(ByteBuf msg) {
        try {
            int topicLength = msg.readInt(); // First 4 bytes are topic length
            if (topicLength > 0 && msg.readableBytes() >= topicLength) {
                byte[] topicBytes = new byte[topicLength];
                msg.readBytes(topicBytes);
                return new String(topicBytes); // GC may occur here, but minimal
            }
        } catch (Exception e) {
            System.err.println("Error parsing topic: " + e.getMessage());
        }
        return null;
    }

    private void broadcastMessage(String topic, ByteBuf message) {
        clientQueues.forEach((channel, queue) -> {
            ByteBuf queuedMessage = message.retainedDuplicate();
            queue.add(queuedMessage);
        });
    }

    private void processClientQueue(Channel channel) {
        Queue<ByteBuf> queue = clientQueues.get(channel);
        if (queue == null) return;

        try {
            while (channel.isActive()) {
                ByteBuf message = queue.poll();
                if (message != null) {
                    channel.writeAndFlush(message).sync(); // Ensure messages are sent in order
                } else {
                    Thread.sleep(1); // Avoid busy-waiting
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.println("Error processing queue for channel: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        int port = 8080;
        GCFreeBinaryServer server = new GCFreeBinaryServer(port);
        server.start();
    }
}
