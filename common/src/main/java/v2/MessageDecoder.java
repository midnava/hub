package v2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

// Message Decoder
public class MessageDecoder extends ByteToMessageDecoder {
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
