package org.apache.hadoop.hdfs;

public class FederationConfigKeys {
  public static final String FEDFS_ZK_MPT_NODE_KEY = "fs.fedfs.mpt.zknode";
  public static final String FEDFS_ZK_MPT_NODE_DEFAULT = "MountPointTable";

  public static final String FEDFS_MOUNT_TABLE_RENEW_INTERVAL =
      "fs.fedfs.mpt.renew.interval";
  public static final long FEDFS_MOUNT_TABLE_RENEW_INTERVAL_DEFAULT = 3600000;
  public static final String FEDFS_MOUNT_TABLE_RENEW_INTERVAL_RANDOMFACTOR =
      "fs.fedfs.mtp.renew.interval.randomfactor";
  public static final long FEDFS_MOUNT_TABLE_RENEW_INTERVAL_RANDOMFACTOR_DEFAULT =
      600000;
  public static final String FEDFS_MOUT_TABLE_RENEW_RETRY_INTERVAL =
      "fs.fedfs.mpt.retry.interval";
  public static final long FEDFS_MOUNT_TABLE_RENEW_RETRY_INTERVAL_DEFAULT =
      600000;
  public static final String FEDFS_SKIP_MOUNT_TABLE_RENEW =
      "fs.fedfs.skip.mpt.renew";
  public static final boolean FEDFS_SKIP_MOUNT_TABLE_RENEW_DEFAULT = false;
}
