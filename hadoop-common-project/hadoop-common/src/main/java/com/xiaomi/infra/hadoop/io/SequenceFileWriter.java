package com.xiaomi.infra.hadoop.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.fs.Path;

public class SequenceFileWriter {
  private SequenceFile.Writer writer;
  private final BytesWritable nullKey = new BytesWritable(new byte[0]);

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

  public void append(byte[] key, byte[] value) throws IOException {
    writer.append(new BytesWritable(key), new BytesWritable(value));
  }

  public void append(byte[] value) throws IOException {
    writer.append(nullKey, new BytesWritable(value));
  }

  public void sync() throws IOException {
    writer.sync();
  }

  public void hflush() throws IOException {
    writer.hflush();
  }

  public void hsync() throws IOException {
    writer.hsync();
  }
  
  public void close() throws IOException {
    writer.close();
  }
}