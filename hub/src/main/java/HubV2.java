import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import v2.Message;
import v2.MessageDecoder;
import v2.MessageEncoder;
import v2.MessageRate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                            ch.pipeline().addLast(new MessageDecoder(), new ServerHandler());
                            ch.pipeline().addLast(new MessageEncoder());
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

    private static class ServerHandler extends SimpleChannelInboundHandler<Message> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
            // Simulate processing
            MessageRate.instance.incrementServerSubMsgRate();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
}
