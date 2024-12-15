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

public class Hub {
    private static final int PORT = 8080;
    private static final Map<String, List<Channel>> subscribers = new HashMap<>();

    public static void main(String[] args) throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

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
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                    HubMessage hubMessage = MessageHubAdapter.deserialize(msg);

                                    System.out.println("Received: " + hubMessage);

                                    if (hubMessage.getMsgType() == MessageType.SUBSCRIBE) {
                                        String topic = hubMessage.getTopic();
                                        subscribers.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(ctx.channel());
                                        HubMessage response = new HubMessage(MessageType.SUBSCRIBE_RESPONSE, "topic");
                                        ctx.writeAndFlush(MessageHubAdapter.serialize(response));
                                        System.out.println("Subscriber added to topic: " + topic);
                                    } else {
//                                        String[] parts = msg.split(" ", 2);
//                                        String topic = parts[0];
//                                        String content = parts[1];
//                                        List<Channel> topicSubscribers = subscribers.get(topic);
//                                        if (topicSubscribers != null) {
//                                            for (Channel ch : topicSubscribers) {
//                                                if (ch.isActive()) {
//                                                    ch.writeAndFlush(content)
//                                                            .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
//                                                }
//                                            }
//                                            System.out.println("Message sent to subscribers of topic: " + topic);
//                                        } else {
//                                            System.out.println("No subscribers for topic: " + topic);
//                                        }
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
