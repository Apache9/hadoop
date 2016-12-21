package org.apache.hadoop.tools;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.LinkedList;

public class SetReplicationRecursive implements Tool {
  private static final Log LOG =
      LogFactory.getLog(SetReplicationRecursive.class);

  private Configuration conf;

  @Override public Configuration getConf() {
    return conf;
  }

  @Override public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override public int run(String[] args) throws Exception {
    if (args.length < 2 || args.length > 3) {
      System.out.println("Usage: hadoop setrep [-check] <path> <replication factor>");
    }

    // parse arguments
    int argsIdx = 0;
    boolean doCheck = false;
    if (args.length == 3) {
      if (args[argsIdx].equals("-check")) {
        doCheck = true;
      } else {
        throw new IllegalArgumentException("Illegal option " + args[argsIdx]);
      }
      argsIdx++;
    }
    Path root = new Path(args[argsIdx++]);
    short repFactor = (short) Integer.parseInt(args[argsIdx++]);

    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
    LinkedList<FileStatus> queue = new LinkedList<FileStatus>();
    FileStatus rootStatus = dfs.getFileStatus(root);
    ContentSummary rootSummary = dfs.getContentSummary(root);
    int count = 0;
    int modified = 0;
    int notMatch = 0;
    try {
      queue.push(rootStatus);
      while (!queue.isEmpty()) {
        for (FileStatus child : dfs.listStatus(queue.pop().getPath())) {
          Path childPath = child.getPath();
          if (child.isDirectory()) {
            queue.push(child);
          } else {
            count++;
            if (child.getReplication() != repFactor) {
              notMatch++;
              if (doCheck) {
                LOG.info(childPath + ", replication factor:" + child
                    .getReplication());
                continue;
              }
              dfs.setReplication(childPath, repFactor);
              LOG.info("Modified replication factor of " + childPath + " from "
                  + child.getReplication() + " to " + repFactor);
              modified++;
            }
          }
        }
      }
    } catch (IOException e) {
      LOG.warn(e.getMessage());
    }
    LOG.info("Files in directory: " + root + ", " + rootSummary.getFileCount());
    LOG.info("Files traversed: " + count);
    LOG.info(notMatch + " files not match expected factor " + repFactor);
    if (!doCheck) {
      LOG.info("Modified replication factor for " + modified + " files");
    }
    return 0;
  }

  public static void main(String argv[]) {
    int exitcode = 0;
    try {
      SetReplicationRecursive setRep = new SetReplicationRecursive();
      exitcode = ToolRunner.run(new HdfsConfiguration(), setRep, argv);
    } catch (Exception e) {
      LOG.error("couldn't run FileArchiver, error: ", e);
    }
    System.exit(exitcode);
  }
}
