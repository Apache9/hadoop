package com.xiaomi.infra.hadoop.io;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;

import com.google.common.annotations.VisibleForTesting;

public class SequenceFileWriter {
  private SequenceFile.Writer writer;
  private final BytesWritable nullKey = new BytesWritable(new byte[0]);
  private boolean isQuotaExceeded = false;

  public SequenceFileWriter(String filePath, String compressionType, String codecName)
      throws IOException {
    Path path = new Path(filePath);

    Configuration conf = new Configuration();

    CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
    CompressionCodec codec = codecFactory.getCodecByName(codecName);

    writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(path),
        SequenceFile.Writer.keyClass(BytesWritable.class),
        SequenceFile.Writer.valueClass(BytesWritable.class),
        SequenceFile.Writer.compression(
            SequenceFile.CompressionType.valueOf(compressionType), codec));
  }

  @VisibleForTesting
  public SequenceFileWriter(String filePath, String compressionType,
      String codecName, Configuration conf) throws IOException {
    Path path = new Path(filePath);

    CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
    CompressionCodec codec = codecFactory.getCodecByName(codecName);

    writer =
        SequenceFile.createWriter(
            conf,
            SequenceFile.Writer.file(path),
            SequenceFile.Writer.keyClass(BytesWritable.class),
            SequenceFile.Writer.valueClass(BytesWritable.class),
            SequenceFile.Writer.compression(
                SequenceFile.CompressionType.valueOf(compressionType), codec));
  }

  public void append(byte[] key, byte[] value) throws IOException {
    try {
      writer.append(new BytesWritable(key), new BytesWritable(value));
    } catch (QuotaExceededException qe) {
      isQuotaExceeded = true;
      throw qe;
    }
  }

  public void append(byte[] value) throws IOException {
    try {
      writer.append(nullKey, new BytesWritable(value));
    } catch (QuotaExceededException qe) {
      isQuotaExceeded = true;
      throw qe;
    }
  }

  public void sync() throws IOException {
    try {
      writer.sync();
    } catch (QuotaExceededException qe) {
      isQuotaExceeded = true;
      throw qe;
    }
  }

  public void hflush(boolean updateLength) throws IOException {
    try {
      if (updateLength) {
        FSDataOutputStream outputStream = writer.getStream();
        if (outputStream instanceof HdfsDataOutputStream) {
          ((HdfsDataOutputStream) outputStream).hflush(EnumSet
              .of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
          return;
        }
      }
      writer.hflush();
    } catch (QuotaExceededException qe) {
      isQuotaExceeded = true;
      throw qe;
    }
  }

  public void hsync(boolean updateLength) throws IOException {
    try {
      if (updateLength) {
        FSDataOutputStream outputStream = writer.getStream();
        if (outputStream instanceof HdfsDataOutputStream) {
          ((HdfsDataOutputStream) outputStream).hsync(EnumSet
              .of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
          return;
        }
      }
      writer.hsync();
    } catch (QuotaExceededException qe) {
      isQuotaExceeded = true;
      throw qe;
    }
  }

  public void close() throws IOException {
    try {
      writer.close();
    } catch (QuotaExceededException qe) {
      isQuotaExceeded = true;
      throw qe;
    }
  }

  public boolean isQuotaExceeded() {
    return isQuotaExceeded;
  }
}
