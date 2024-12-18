package hub;

import common.MessageRate;
import hub.adapters.MessageHubEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.ReferenceCounted;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class HubV2 {
    private static final int port = 8080;
    private static final Map<String, List<SubscriberQueue>> subscribers = new HashMap<>();
    private static final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private static final Map<Channel, Queue<ByteBuf>> clientQueues = new ConcurrentHashMap<>();

    public static void main(String[] args) throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(2);
        EventLoopGroup workerGroup = new NioEventLoopGroup(2);
        ByteBufAllocator allocator = new UnpooledByteBufAllocator(true);


//        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)

                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.SO_RCVBUF, 4 * 1024 * 1024)
                    .childOption(ChannelOption.SO_SNDBUF, 4 * 1024 * 1024)
                    .childOption(ChannelOption.AUTO_CLOSE, true)
                    .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(1024 * 1024, 4 * 1024 * 1024))
                    .childOption(ChannelOption.TCP_NODELAY, true)
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
                            ch.pipeline().addLast(new LengthFieldPrepender(4));
                            ch.pipeline().addLast(new ServerHandler(ch));
                            ch.pipeline().addLast(new MessageHubEncoder());
                            ch.config().setAllocator(allocator);
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
    //

    private static class ServerHandler extends SimpleChannelInboundHandler<ByteBuf> {
        private volatile long currentIndex = -1;
        private final AtomicLong globalSeqNo = new AtomicLong();

        public ServerHandler(SocketChannel ch) {
            System.out.println("Created: " + ch.metadata().toString());
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf buff) {
            MessageRate.instance.incrementServerSubMsgRate();

            buff.markReaderIndex(); // Сохраняем позицию для сброса, если потребуется
            int readableBytes = buff.readableBytes();

            System.out.println("Server in [" + readableBytes + "]: " + buff.toString(StandardCharsets.UTF_8));

            int totalLength = readableBytes + 4; // +4 байта для длины
            ByteBuf copiedMessage = ctx.alloc().buffer(totalLength);


            copiedMessage.writeInt(totalLength); // Записываем длину
            copiedMessage.writeBytes(buff, 0, readableBytes); // Записываем данные
            buff.resetReaderIndex(); // Сбрасываем readerIndex для будущей обработки

            MessageRate.instance.incrementSubMsgRate();
            broadcastMessage("topic", copiedMessage);
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

        private void broadcastMessage(String topic, ByteBuf buff) {

            clientQueues.forEach((channel, queue) -> {
                ByteBuf queuedMessage = buff.retainedDuplicate();
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
                        MessageRate.instance.incrementServerPubMsgRate();
                        channel.writeAndFlush(message).sync(); // Ensure messages are sent in order
                        System.out.println("Sent message to " + channel.remoteAddress() + " [" + message.readableBytes() + "]:" + message.toString(StandardCharsets.UTF_8));
                    } else {
                        Thread.sleep(1); // Avoid busy-waiting
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("Error processing queue for channel: " + e.getMessage());
            } finally {
                queue.forEach(ReferenceCounted::release); //TODO CHECK IT
                queue.clear();
            }
        }
    }
}
