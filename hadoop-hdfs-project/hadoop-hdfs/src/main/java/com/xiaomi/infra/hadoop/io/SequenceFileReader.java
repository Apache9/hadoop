package com.xiaomi.infra.hadoop.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileReader {
  public static void main(String[] args) throws IOException {
    String uri = args[0];
    Path path = new Path(uri);

    Configuration conf = new Configuration();
    SequenceFile.Reader reader = null;
    try {
      reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
      BytesWritable key = (BytesWritable) ReflectionUtils.newInstance(
          reader.getKeyClass(), conf);
      BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(
          reader.getValueClass(), conf);
      long position = reader.getPosition();

      while (reader.next(key, value)) {
        String syncSeen = reader.syncSeen() ? "*" : "";
        String keyString = new String(
            key.getBytes(), 0, key.getLength(), "UTF-8");
        String valueString = new String(
            value.getBytes(), 0, value.getLength(), "UTF-8");

        System.out.printf("[%s%s] [%s]%s [%s]%s\n",
            position, syncSeen, keyString.length(), keyString,
            valueString.length(), valueString);
        position = reader.getPosition();
      }
    } finally {
      IOUtils.closeStream(reader);
    }
  }
}