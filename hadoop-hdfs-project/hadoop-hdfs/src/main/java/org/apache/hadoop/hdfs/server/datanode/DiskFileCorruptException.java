package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

public class DiskFileCorruptException extends IOException {
  public DiskFileCorruptException() {
  }

  public DiskFileCorruptException(String msg) {
    super(msg);
  }
}
