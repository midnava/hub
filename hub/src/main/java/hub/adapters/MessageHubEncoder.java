package hub.adapters;

import common.HubMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

public class MessageHubEncoder extends MessageToByteEncoder<HubMessage> {
    @Override
    protected void encode(ChannelHandlerContext ctx, HubMessage msg, ByteBuf out) {
        byte[] topicBytes = msg.getTopic().getBytes(StandardCharsets.US_ASCII);
        int topicLength = topicBytes.length;
        int offset = msg.getOffset();
        int buffLength = msg.getBuffLength();

        // msgType + seqNo + topicLength + topicBytes + offset + buffLength + buffer
        int messageLength = 1 + 8 + 4 + topicLength + 4 + 4 + buffLength;

        //total
        out.writeInt(messageLength);

        out.writeByte(msg.getMessageType().getId());
        out.writeLong(msg.getSeqNo());
        //string
        out.writeInt(topicLength);
        out.writeBytes(topicBytes);

        //buffer
        out.writeInt(offset);
        out.writeInt(buffLength);
        out.writeBytes(msg.getByteBuf().byteBuffer().array(), 0, buffLength);
    }
}

