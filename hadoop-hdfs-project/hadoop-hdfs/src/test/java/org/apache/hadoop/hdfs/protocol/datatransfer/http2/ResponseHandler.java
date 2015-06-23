package org.apache.hadoop.hdfs.protocol.datatransfer.http2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http2.Http2Headers;

import java.io.ByteArrayOutputStream;

public class ResponseHandler extends ChannelInboundHandlerAdapter {

  private boolean finished = false;

  private Http2Headers headers;

  private byte[] data;

  private final ByteArrayOutputStream bos = new ByteArrayOutputStream();

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
      throws Exception {
    boolean endOfStream =
        ((Http2StreamChannel) ctx.channel()).remoteSideClosed();
    synchronized (this) {
      if (msg instanceof Http2Headers) {
        headers = (Http2Headers) msg;
      } else if (msg instanceof ByteBuf) {
        ByteBuf buf = (ByteBuf) msg;
        buf.readBytes(bos, buf.readableBytes());
      }
      if (endOfStream) {
        finished = true;
        data = bos.toByteArray();
        notifyAll();
      }
    }
  }

  public synchronized Http2Headers getHeaders() throws InterruptedException {
    while (!finished) {
      wait();
    }
    return headers;
  }

  public synchronized byte[] getData() throws InterruptedException {
    while (!finished) {
      wait();
    }
    return data;
  }
}