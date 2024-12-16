// Server.java

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Hub {
    private static final int PORT = 8080;
    private static final Map<String, List<SubscriberQueue>> subscribers = new HashMap<>();


    public static void main(String[] args) throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(4);
        EventLoopGroup workerGroup = new NioEventLoopGroup(4);

        AtomicInteger counter = new AtomicInteger();
        AtomicLong totalCount = new AtomicLong();

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            int rate = counter.getAndSet(0);
            totalCount.addAndGet(rate);
            if (rate > 0) {
                System.out.println("Msg rate is " + rate + " msg/sec, total=" + totalCount);
            }
        }, 0, 1, TimeUnit.SECONDS);

//        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new LengthFieldBasedFrameDecoder(65535, 0, 2, 0, 2));
                            pipeline.addLast(new LengthFieldPrepender(2));
                            pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                public void channelActive(ChannelHandlerContext ctx) {
                                    System.out.println("Subscriber connected: " + ctx.channel().remoteAddress());
                                }

                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws InterruptedException {
                                    HubMessage hubMessage = MessageHubAdapter.deserializeHeader(msg); //todo only header

                                    if (hubMessage.getMsgType() == MessageType.SUBSCRIBE) {
                                        String topic = hubMessage.getTopic();
                                        Channel channel = ctx.channel();
                                        subscribers.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(new SubscriberQueue(channel));

                                        HubMessage response = new HubMessage(MessageType.SUBSCRIBE_RESPONSE, "topic", "subscribed on " + topic);
                                        ByteBuf buffer = ch.alloc().buffer(512);

                                        ctx.writeAndFlush(MessageHubAdapter.serialize(response, buffer));
                                        System.out.println("Subscriber added to topic: " + topic);
                                    } else if (hubMessage.getMsgType() == MessageType.MESSAGE) {
                                        counter.incrementAndGet();
                                        String topic = hubMessage.getTopic();
                                        msg.resetReaderIndex();

                                        List<SubscriberQueue> topicSubscribers = subscribers.get(topic);
                                        if (topicSubscribers != null) {
                                            for (SubscriberQueue queue : topicSubscribers) {
                                                if (queue.isActive()) {
                                                    queue.addMessage(msg);
                                                }
                                            }
//                                            System.out.println("Message sent to subscribers of topic: " + topic);
                                        } else {
//                                            System.out.println("No subscribers for topic: " + topic);
                                        }
                                    } else {
                                        throw new IllegalArgumentException(hubMessage.toString());
                                    }
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) {
                                    System.out.println("Subscriber disconnected: " + ctx.channel().remoteAddress());
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    cause.printStackTrace();
                                    ctx.close();
                                }
                            });
                        }
                    });

            System.out.println("Server is starting...");
            ChannelFuture f = b.bind(PORT).sync();
            f.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
