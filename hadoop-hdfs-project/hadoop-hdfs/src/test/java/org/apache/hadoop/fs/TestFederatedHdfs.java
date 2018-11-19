package org.apache.hadoop.fs;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.TestFederated;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.fs.viewfs.MountpointRenewer;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsMountpointRenewer;
import org.apache.hadoop.hdfs.TestFederatedDFSFileSystem;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.net.URI;
import java.security.PrivilegedAction;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFederatedHdfs extends TestFederated {
  private static final Log LOG =
      LogFactory.getLog(TestFederatedDFSFileSystem.class);

  /**
   * Test the whole process of updating mount point table:
   * 1. superuser uses DFSAdmin to update new mount point to zk;
   * 2. normal user uses FederatedDFSFilesystem to access new mount point dir;
   * */
  @Test
  public void testAddNewNameSpaceByRenewer() throws Exception {
    conf.set(DFSConfigKeys.DFS_CLIENT_ZOOKEEPER_OBSERVER, hostPort);
    // Add a new namespace
    Configuration tmpConf = new Configuration(conf);
    String clusterName = "test-cluster";
    String newNs = clusterName + "-3";
    addNSAccessConfig(tmpConf, newNs, 3);
    ConfigUtil.addLink(tmpConf, clusterName, "/new-mpt",
        new URI("hdfs://" + newNs + "/new-mpt"));
    cluster.transitionToActive(6);
    FileSystem fs4 = cluster.getFileSystem(6);
    fs4.mkdirs(new Path("/new-mpt"));

    // upload the config to zk with DFSAdmin
    HdfsMountpointRenewer hmpr = new HdfsMountpointRenewer();
    hmpr.initialize(clusterName, tmpConf,
        new MountpointRenewer.RenewMountpoint() {
          @Override
          public void doUpdateMountpoint() {
          }
        });
    String newMptConfString =
        HdfsMountpointRenewer.serializeMountpoint2String(tmpConf, clusterName);
    final DFSAdmin dfsAdmin = new DFSAdmin(tmpConf);
    final String[] argv = new String[]{"-updateMptOnZk"};
    UserGroupInformation ugi = UserGroupInformation
        .createUserForTesting("hdfs", new String[] { "hdfs" });
    ugi.doAs(new PrivilegedAction<Object>() {
      @Override
      public Object run() {
        int res = 0;
        try {
          res = ToolRunner.run(dfsAdmin, argv);
        } catch (Exception e) {
          assert false;
        }
        assertEquals(0, res);
        return null;
      }
    });
    byte[] zkData = hmpr.getMptConfFromZookeeper(tmpConf);
    assertTrue(Arrays.equals(zkData, newMptConfString.getBytes()));

    // use FederatedDFSFileSystem to access new mount point dir
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    conf.set("fs.AbstractFileSystem.hdfs.impl", FederatedHdfs.class.getName());
    FederatedHdfs fs = (FederatedHdfs) AbstractFileSystem
        .get(new URI("hdfs://" + clusterName + "/"), conf);
    Assert.assertEquals(fs.getChildFileSystems().length, 4);
    Path testPath = new Path("/new-mpt/test-dir");
    fs.mkdir(testPath, FsPermission.getDefault(),true);
    Assert.assertTrue(fs4.exists(testPath));
  }
}
