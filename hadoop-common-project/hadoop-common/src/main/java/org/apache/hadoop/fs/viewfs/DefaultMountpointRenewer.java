package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class DefaultMountpointRenewer extends MountpointRenewer {
  @Override
  synchronized public void updateMptFromZk(Configuration conf) {
    // do nothing.
  }

  @Override
  public void updateMountPointConfig(Configuration conf,
      byte[] zkData) throws IOException {
    // do nothing
  }
}
