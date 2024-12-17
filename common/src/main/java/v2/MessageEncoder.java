package v2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

public class MessageEncoder extends MessageToByteEncoder<Message> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) {
        byte[] topicBytes = msg.getTopic().getBytes(StandardCharsets.UTF_8);
        int topicLength = topicBytes.length;

        int messageLength = 1 + 8 + 4 + topicLength; // msgType + seqNo + topicLength + topicBytes

        out.writeInt(messageLength);
        out.writeByte(msg.getMessageType().getId());
        out.writeLong(msg.getSeqNo());

        out.writeInt(topicLength);
        out.writeBytes(topicBytes);
    }
}

