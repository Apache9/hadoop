package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.ha.EditLogTailer;
import org.apache.hadoop.hdfs.server.namenode.ha.TestStandbyIsHot;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by xiegang1 on 17-12-8.
 */
public class TestFSImageBackup {
  protected static final Log LOG = LogFactory.getLog(
      TestStandbyIsHot.class);
  private static final Path TEST_DIR = new Path("/test");
  private static final Path TEST_FILE_PATH = new Path(TEST_DIR, "foo");
  private static final String TEST_FILE_STR = TEST_FILE_PATH.toUri().getPath();
  private static final String TEST_FILE_DATA =
      "Hello state transitioning world";
  private static final HAServiceProtocol.StateChangeRequestInfo REQ_INFO = new HAServiceProtocol.StateChangeRequestInfo(
      HAServiceProtocol.RequestSource.REQUEST_BY_USER_FORCED);

  static {
    ((Log4JLogger) EditLogTailer.LOG).getLogger().setLevel(Level.ALL);
  }

  @Test
  public void testFSImageBackupBasic() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_BACKUP_FSIMAGE_ENABLE_KEY, false);
    conf.set(DFS_NAMENODE_BACKUP_FSIMAGE_CLUSTER_KEY, "minidfs-ns");
    conf.set("dfs.client.failover.proxy.provider.minidfs-ns", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    String backupDir = conf.get(DFS_NAMENODE_BACKUP_FSIMAGE_DIR_KEY, DFS_NAMENODE_BACKUP_FSIMAGE_DIR_DEFAULT);

    conf.set("fs.defaultFS", "hdfs://minidfs-ns");
    //conf.set("dfs.namenode.rpc-address.minidfs-ns.host0", "localhost:123456");
    //conf.set("dfs.namenode.rpc-address.minidfs-ns.host1", "localhost:123457");

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(1)
        .clusterId("BackupCluster")
        .build();
    cluster.transitionToActive(0);
    cluster.waitActive();


    conf.set("dfs.namenode.rpc-address.minidfs-ns.host0", cluster.getNameNode(0).getHostAndPort());
    FSImageBackup fsImageBackup = new FSImageBackup(cluster.getNamesystem(0), conf);
    fsImageBackup.doBackup();

    Thread.sleep(10000);

    try {
      DistributedFileSystem fs = cluster.getFileSystem(0);
      String imageFileName = fsImageBackup.getLastBackupFSImageFileName();
      String mdfilename = fsImageBackup.getLastBackupFSImageMD5FileName();

      assertTrue (fs.exists(new Path(fs.getHomeDirectory(), imageFileName)));
      assertTrue (fs.exists(new Path(fs.getHomeDirectory(), mdfilename)));

    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testFSImageBackupFailoverWhenUploading() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_BACKUP_FSIMAGE_ENABLE_KEY, true);
    conf.set(DFS_NAMENODE_BACKUP_FSIMAGE_CLUSTER_KEY, "minidfs-ns");
    conf.set("dfs.client.failover.proxy.provider.minidfs-ns", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    conf.setLong(DFS_NAMENODE_BACKUP_FSIMAGE_BANDWIDTH_KEY, 1);


    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(1)
        .clusterId("BackupCluster")
        .build();
    cluster.transitionToActive(0);
    cluster.waitActive();

    conf.set("fs.defaultFS", "hdfs://minidfs-ns");
    //cluster.getNameNode(1).getNamesystem().getFSImageBackup().setBackupCluster("minidfs-ns");
    cluster.getNameNode(1).getNamesystem().getFSImageBackup().setConf(conf);

    Thread.sleep(10000);
    cluster.transitionToStandby(0);
    Thread.sleep(10000);
    cluster.transitionToActive(1);
    Thread.sleep(10000);

    assertTrue(cluster.getNameNode(0).getServiceState() == HAServiceProtocol.HAServiceState.STANDBY);
    assertTrue(cluster.getNameNode(1).getServiceState() == HAServiceProtocol.HAServiceState.ACTIVE);

    cluster.shutdown();
  }

  @Test
  public void testFSImageBackupPurgeOldestBackup() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_BACKUP_FSIMAGE_ENABLE_KEY, false);
    conf.set(DFS_NAMENODE_BACKUP_FSIMAGE_CLUSTER_KEY, "minidfs-ns");
    conf.set("dfs.client.failover.proxy.provider.minidfs-ns", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    conf.setInt(DFS_NAMENODE_BACKUP_FSIMAGE_MAX_BACKUP_KEY, 2);


    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(1)
        .clusterId("BackupCluster")
        .build();
    cluster.transitionToActive(0);
    cluster.waitActive();

    //create files in backup dir
    FileSystem fs = null;
    try {
      fs = cluster.getFileSystem(0);
      fs.create(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/fsimage_-2_1512961856722"));
      fs.create(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/fsimage_-1_1512961856723"));
      fs.create(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/fsimage_-2.md5_1512961856722"));
      fs.create(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/fsimage_-1.md5_1512961856723"));
      // some other files
      fs.create(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/fsimage"));
      fs.create(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/fsimage_"));
      fs.create(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/fsimage__"));
      fs.create(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/fsimage__1512961856722"));
      fs.create(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/fsimage_0000000000000000000_"));
      fs.create(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/fsimage_notnumber_1512961856723"));
      fs.create(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/fsimage_0000000000000000000_notnumber"));
      fs.create(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/fsimage_notnumber_notnumber"));
      fs.create(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/_fsimage"));
      fs.create(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/fsimages_0000000000000000000_1512961856723"));

      fs.close();
    } catch (Exception e) {
      LOG.error(e);
      assertTrue(false);
    }

    conf.set("fs.defaultFS", "hdfs://minidfs-ns");

    conf.set("dfs.namenode.rpc-address.minidfs-ns.host0", cluster.getNameNode(0).getHostAndPort());
    FSImageBackup fsImageBackup = new FSImageBackup(cluster.getNamesystem(0), conf);
    fsImageBackup.doBackup();

    Thread.sleep(10000);

    fs = cluster.getFileSystem(0);

    assertTrue(!fs.exists(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/fsimage_-2_1512961856722")));
    assertTrue(fs.exists(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/fsimage_-1_1512961856723")));
    assertTrue(!fs.exists(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/fsimage_-2.md5_1512961856722")));
    assertTrue(fs.exists(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/fsimage_-1.md5_1512961856723")));
    String imageFileName = fsImageBackup.getLastBackupFSImageFileName();
    String mdfilename = fsImageBackup.getLastBackupFSImageMD5FileName();
    assertTrue (fs.exists(new Path(fs.getHomeDirectory(), imageFileName)));
    assertTrue (fs.exists(new Path(fs.getHomeDirectory(), mdfilename)));
    assertTrue(fs.listStatus(new Path("./FSImageBackup/minidfs-ns_127.0.1.1/")).length == 14);

    cluster.shutdown();
  }

  @Test
  public void testFSImageBackupTheBackupClusterIsNotSet() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_BACKUP_FSIMAGE_ENABLE_KEY, true);
    conf.set("dfs.client.failover.proxy.provider.minidfs-ns", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");


    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(1)
        .clusterId("BackupCluster")
        .build();
    cluster.transitionToActive(0);
    cluster.waitActive();

    conf.set("fs.defaultFS", "hdfs://minidfs-ns");

    Thread.sleep(10000);

    assertTrue(cluster.getNameNode(1).getNamesystem().getFSImageBackup() == null);

    cluster.shutdown();
  }

  @Test
  public void testStanbyNNRestart() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_BACKUP_FSIMAGE_ENABLE_KEY, false);
    conf.set(DFS_NAMENODE_BACKUP_FSIMAGE_CLUSTER_KEY, "minidfs-ns");
    conf.set("dfs.client.failover.proxy.provider.minidfs-ns", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    conf.setInt(DFS_NAMENODE_BACKUP_FSIMAGE_MAX_BACKUP_KEY, 2);
    String backupDir = conf.get(DFS_NAMENODE_BACKUP_FSIMAGE_DIR_KEY, DFS_NAMENODE_BACKUP_FSIMAGE_DIR_DEFAULT);


    conf.set("fs.defaultFS", "hdfs://minidfs-ns");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(1)
        .clusterId("BackupCluster")
        .build();
    cluster.transitionToActive(0);
    cluster.waitActive();


    try {
      FSImageBackup fsImageBackup = new FSImageBackup(cluster.getNamesystem(0), cluster.getConfiguration(0));
      fsImageBackup.doBackup();

      Thread.sleep(5000);
      FileSystem fs = cluster.getFileSystem(0);
      String imageFileName = fsImageBackup.getLastBackupFSImageFileName();
      String mdfilename = fsImageBackup.getLastBackupFSImageMD5FileName();
      assertTrue (fs.exists(new Path(fs.getHomeDirectory(), imageFileName)));
      assertTrue (fs.exists(new Path(fs.getHomeDirectory(), mdfilename)));

      int i = 3;
      while (i > 0) {
        fsImageBackup = new FSImageBackup(cluster.getNamesystem(0), conf);
        fsImageBackup.doBackup();
        Thread.sleep(5000);
        i--;
      }

      assertTrue (fs.exists(new Path(fs.getHomeDirectory(), imageFileName)));
      assertTrue (fs.exists(new Path(fs.getHomeDirectory(), mdfilename)));

    } catch (Exception e) {
      LOG.error(e);
      assertTrue(false);
    } finally {
      cluster.shutdown();
    }
  }
}
