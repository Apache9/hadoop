package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CachingGetSpaceUsed;
import org.apache.hadoop.fs.GetSpaceUsed;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;

@InterfaceAudience.LimitedPrivate({ "HDFS", "MapReduce" })
@InterfaceStability.Evolving
public abstract class FSCachingGetSpaceUsed extends CachingGetSpaceUsed {
  public static final Log LOG = LogFactory.getLog(FSCachingGetSpaceUsed.class);

  public FSCachingGetSpaceUsed(Builder builder) throws IOException {
    super(builder);
  }

  public FSCachingGetSpaceUsed(CachingGetSpaceUsed.Builder builder)
      throws IOException {
    super(builder);
  }

  @Override
  public void init() {
    if (used.get() < 0) {
      used.set(0);
    }

    startRefreshThread();
  }

  public static class Builder extends GetSpaceUsed.Builder {
    private FsVolumeImpl volume;
    private String bpid;

    public FsVolumeImpl getVolume() {
      return volume;
    }

    public Builder setVolume(FsVolumeImpl volume) {
      this.volume = volume;
      return this;
    }

    public String getBpid() {
      return bpid;
    }

    public Builder setBpid(String bpid) {
      this.bpid = bpid;
      return this;
    }

    @Override
    public GetSpaceUsed build() throws IOException {
      Class clazz = getKlass();
      if (FSCachingGetSpaceUsed.class.isAssignableFrom(clazz)) {
        try {
          cons = clazz.getConstructor(Builder.class);
        } catch (NoSuchMethodException e) {
          LOG.error(e);
        }
      }
      return super.build();
    }
  }
}
