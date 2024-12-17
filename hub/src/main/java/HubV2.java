import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import v2.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class HubV2 {
    private static final int port = 8080;
    private static final Map<String, List<SubscriberQueue>> subscribers = new HashMap<>();


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
                            ch.pipeline().addLast(new MessageConnectorDecoder(), new ServerHandler(ch));
                            ch.pipeline().addLast(new MessageConnectorEncoder());
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

    private static class ServerHandler extends SimpleChannelInboundHandler<HubMessage> {
        private volatile long currentIndex = -1;
        private final AtomicLong globalSeqNo = new AtomicLong();

        public ServerHandler(SocketChannel ch) {
            System.out.println("Created: " + ch.metadata().toString());
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, HubMessage msg) {
            MessageRate.instance.incrementServerSubMsgRate();
            long seqNo = msg.getSeqNo();

            if (currentIndex > 0 && currentIndex + 1 != seqNo) {
                System.out.println("Error: " + currentIndex + " vs " + seqNo);
            }
            currentIndex = seqNo;

            String topic = msg.getTopic();
            MessageType messageType = msg.getMessageType();

            if (messageType == MessageType.SUBSCRIBE) {
                Channel channel = ctx.channel();
                subscribers.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(new SubscriberQueue(channel));

                HubMessage response = new HubMessage(MessageType.SUBSCRIBE, "topic", globalSeqNo.incrementAndGet(), "subscribed on " + topic);
                ctx.writeAndFlush(response);

                MessageRate.instance.incrementServerPubMsgRate();
                System.out.println("Subscriber added to topic: " + topic);
            } else if (messageType == MessageType.MESSAGE) {
                MessageRate.instance.incrementSubMsgRate();

                if (seqNo % 1_000_000 == 0) {
                    System.out.println("MSG " + seqNo + ": " + msg.getByteBuf().getStringAscii(0));
                }


//                List<SubscriberQueue> topicSubscribers = subscribers.get(topic);
//                if (topicSubscribers != null) {
//                    for (SubscriberQueue queue : topicSubscribers) {
//                        if (queue.isActive()) {
//                            queue.addMessage(msg);
//                        }
//                    }
////                                            System.out.println("Message sent to subscribers of topic: " + topic);
//                } else {
////                                            System.out.println("No subscribers for topic: " + topic);
//                }
//            } else {
//                throw new IllegalArgumentException(hubMessage.toString());
//            }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
}
