/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.contrib.raid;

import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A Mover is used to move blocks whose placement might deteriorate the data availability. The input
 * information is collected by the Collector{@link Collector}. It will use a MapReduce job to do the
 * work.
 */
public class Mover {

  private static final Log LOG = LogFactory.getLog(Mover.class);

  private final Path collectorResultFile;
  private final int mapTaskNum;
  private final Path outputPath;
  private final Configuration conf;
  private Job job;
  
  public enum CounterName {
    MovedBlocks, FailedBuildingMovingMap, FailedMoving
  }

  public Mover(Path collectorResultFile, int mapTaskNum, Path outputPath,
      Configuration inConf) {
    Preconditions.checkNotNull(collectorResultFile);
    Preconditions.checkArgument(mapTaskNum > 0);
    Preconditions.checkNotNull(inConf);
    this.collectorResultFile = collectorResultFile;
    this.mapTaskNum = mapTaskNum;
    this.outputPath = outputPath;
    this.conf = inConf;
    String queue = conf.get(HdfsRaidConfigKeys.HDFS_RAID_MOVER_JOB_QUEUE);
    if (queue != null) {
      conf.set("mapreduce.job.queuename", queue);
    }
  }
  
  public Counter getCounter(CounterName name) throws IOException {
    Preconditions.checkNotNull(job);
    return job.getCounters().findCounter(name);
  }

  public void run() throws IOException, ClassNotFoundException, InterruptedException {
    conf.setInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_MAP_TASK_NUM_KEY, mapTaskNum);
    conf.set(HdfsRaidConfigKeys.HDFS_RAIDNODE_COLLECTOR_RESULT_FILE_KEY,
      collectorResultFile.toString());

    job = Job.getInstance(conf, "RaidNode-Mover");
    job.setJarByClass(Mover.class);
    job.setMapperClass(MoverMapper.class);

    job.setInputFormatClass(MoverInfoInputFormat.class);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setSpeculativeExecution(false);
    job.setNumReduceTasks(0);
    job.submit();
    MRUtils.writeJobId(conf, getJobIdFilePath(), getJobId());

    if (!job.waitForCompletion(true)) {
      throw new IOException("Wait for job completion failed");
    }
  }

  public String getJobId() {
    Preconditions.checkNotNull(job);
    return job.getJobID().toString();
  }

  public static Path getJobIdFilePath() {
    Path raidRoot = BlockCodec.getRaidRoot();
    return new Path(raidRoot.toString() + "/" + "mover.jobid");
  }

  // UT code can use this method to set up testing environment
  @VisibleForTesting
  public static void moveOneBlock(int connectTimeout, int movTimeout, LocatedBlock loc,
      DatanodeInfo target) throws IOException {
    Socket sock = new Socket();
    DataOutputStream out = null;
    DataInputStream in = null;
    try {
      LOG.debug("Trying to move block " + loc + " to DN " + target);
      sock.connect(NetUtils.createSocketAddr(target.getXferAddr()), connectTimeout);
      sock.setSoTimeout(movTimeout);
      sock.setKeepAlive(true);

      OutputStream unbufOut = sock.getOutputStream();
      InputStream unbufIn = sock.getInputStream();

      out = new DataOutputStream(new BufferedOutputStream(unbufOut,
          HdfsConstants.IO_FILE_BUFFER_SIZE));
      in = new DataInputStream(new BufferedInputStream(unbufIn, HdfsConstants.IO_FILE_BUFFER_SIZE));

      new Sender(out).replaceBlock(loc.getBlock(), StorageType.DEFAULT, loc.getBlockToken(),
        loc.getLocations()[0].getDatanodeUuid(), loc.getLocations()[0]);

      BlockOpResponseProto response = BlockOpResponseProto.parseFrom(vintPrefixed(in));
      if (response.getStatus() != Status.SUCCESS && response.getStatus() != Status.IN_PROGRESS) {
        LOG.info("Fail to move block, status is " + response.getStatus());
        if (response.getStatus() == Status.ERROR_ACCESS_TOKEN) throw new IOException(
            "block move failed due to access token error");
        throw new IOException("block move is failed: " + response.getMessage());
      } else {
        LOG.debug("Successfully move one block");
      }
    } finally {
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
      IOUtils.closeSocket(sock);
    }
  }



  /**
   * Map task to do mover work.
   */
  public static class MoverMapper extends Mapper<Object, Text, Object, Object> {

    private Configuration conf;
    private DatanodeInfo[] liveNodes;
    private NetworkTopology topology;
    private FileSystem fs;
    private int dataBlocksNum;
    private int codingBlocksNum;
    private long requiredSize;
    private boolean shuffleBlksAmongRacks;
    
    private Counter movedBlocks;
    private Counter failedBuildingMovingMap;
    private Counter failedMoving;

    private NamenodeProtocol namenode;
    private BlockTokenSecretManager blockTokenSecretManager;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      this.conf = context.getConfiguration();
      UserGroupInformation.setConfiguration(conf);
      SecurityUtil.login(conf, HdfsRaidConfigKeys.HDFS_RAIDNODE_KEYTAB_FILE_KEY,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_KERBEROS_PRINCIPAL_KEY);
      fs = FileSystem.get(conf);
      topology = new NetworkTopology();
      if (!(fs instanceof DistributedFileSystem)) {
        throw new IOException("The file system is not a distributed file system");
      }
      liveNodes = ((DistributedFileSystem) fs).getClient().datanodeReport(DatanodeReportType.LIVE);
      for (DatanodeInfo di : liveNodes) {
        LOG.debug("Added live node " + di);
        topology.add(di);
      }

      dataBlocksNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_KEY,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_DATA_BLOCKS_NUM_DEFAULT);
      codingBlocksNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_KEY,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_CODING_BLOCKS_NUM_DEFAULT);
      int reservedBlockNumPerStorage = HdfsConstants.MIN_BLOCKS_FOR_WRITE;
      long blockSize = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
      requiredSize = blockSize * reservedBlockNumPerStorage;
      shuffleBlksAmongRacks = conf.getBoolean(HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_SHUFFLE_RACKS,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_SHUFFLE_RACKS_DEFAULT);
      StringBuilder sb = new StringBuilder();
      sb.append("dataBlocksNum ").append(dataBlocksNum).append(" codingBlocksNum ")
          .append(codingBlocksNum).append(" reserverdBlockNumPerStorage ")
          .append(reservedBlockNumPerStorage).append(" blockSize ").append(blockSize)
          .append(" requiredSize ").append(requiredSize).append(" shuffleBlksAmongRacks ")
          .append(shuffleBlksAmongRacks);
      LOG.info(sb.toString());

      this.movedBlocks = context.getCounter(CounterName.MovedBlocks);
      this.failedBuildingMovingMap = context.getCounter(CounterName.FailedBuildingMovingMap);
      this.failedMoving = context.getCounter(CounterName.FailedMoving);

      this.blockTokenSecretManager = MRUtils.getBlockTokenSecretManager(conf);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
      String info = value.toString();
      String[] tokens = info.split("\t");
      Path file = new Path(tokens[0].trim());
      int groupIdx = Integer.parseInt(tokens[1].trim());

      LOG.debug("Moving group " + groupIdx + " of file " + file.toString());
      BlockMover bm = new BlockMover(groupIdx, file);
      try {
        bm.doMove();
      } catch (IOException ioe) {
        // Log the error and go ahead with other entires
        StringBuilder sb = new StringBuilder();
        sb.append("Fail to move block in group ").append(groupIdx).append(" of file ")
            .append(file.toString());
        LOG.warn(sb.toString(), ioe);
      }
    }

    private class BlockMover {
      private int groupIdx;
      private Path file;
      private Map<LocatedBlock, DatanodeInfo> moveMapDN;
      private Map<LocatedBlock, DatanodeInfo> moveMapRack;
      private Set<DatanodeInfo> excludedDis;
      private Random rand;

      private BlockMover(int groupIdx, Path file) {
        this.groupIdx = groupIdx;
        this.file = file;
        moveMapDN = new HashMap<LocatedBlock, DatanodeInfo>();
        if (shuffleBlksAmongRacks) {
          moveMapRack = new HashMap<LocatedBlock, DatanodeInfo>();
        }
        excludedDis = new HashSet<DatanodeInfo>();
        rand = new Random();
      }

      private boolean checkMoveAmongRacks(LocatedBlock loc, Set<DatanodeInfo> excludedDNs) {
        if (shuffleBlksAmongRacks == false) {
          return false;
        }
        for (DatanodeInfo di : excludedDNs) {
          if (topology.isOnSameRack(loc.getLocations()[0], di)) {
            return true;
          }
        }
        return false;
      }

      private DatanodeInfo chooseTarget(LocatedBlock loc, boolean mustOnDiffRack)
          throws IOException {
        int totalDNs = liveNodes.length;
        List<DatanodeInfo> targetSet = new ArrayList<DatanodeInfo>();

        // Step1 : find all live DNs which is not in the same rack as any node in
        // the excluded list and randomly select one in them
        for (int i = 0; i < totalDNs; i++) {
          DatanodeInfo di = liveNodes[i];
          boolean newRack = true;
          if (!excludedDis.contains(di) && (di.getRemaining() > requiredSize)) {
            for (DatanodeInfo tmpDi : excludedDis) {
              if (topology.isOnSameRack(tmpDi, loc.getLocations()[0])) {
                newRack = false;
                break;
              }
            }
            if (newRack) {
              targetSet.add(di);
            }
          }
        }

        LOG.debug("Target chosen in the first round : ");
        for (DatanodeInfo di : targetSet) {
          LOG.debug(di.toString());
        }
        if (targetSet.size() > 0) {
          return targetSet.get(rand.nextInt(targetSet.size()));
        }

        if (mustOnDiffRack) {
          return null;
        }

        // Step 2 : go through all live DNs and find one which is not in the excluded list
        for (int i = 0; i < totalDNs; i++) {
          DatanodeInfo di = liveNodes[i];
          if (!excludedDis.contains(di) && (di.getRemaining() > requiredSize)) {
            targetSet.add(di);
          }
        }

        LOG.debug("Target chosen in the second round : ");
        for (DatanodeInfo di : targetSet) {
          LOG.debug(di.toString());
        }

        if (targetSet.size() > 0) {
          return targetSet.get(rand.nextInt(targetSet.size()));
        }

        return null;
      }

      private void buildMoveMap() throws IOException {
        Path codingFile = BlockCodec.getCodingFile(file);
        FileStatus fileStatus = fs.getFileStatus(file);

        if (!fs.exists(codingFile)) {
          throw new IOException("The file passed to Mover does not have a coding file");
        }

        long sOffset = fileStatus.getBlockSize() * dataBlocksNum * groupIdx;
        long sLen = fileStatus.getBlockSize() * dataBlocksNum;
        long cOffset = fileStatus.getBlockSize() * codingBlocksNum * groupIdx;
        long cLen = fileStatus.getBlockSize() * codingBlocksNum;

        LocatedBlocks sourceBlks = ((DistributedFileSystem) fs).getClient().getLocatedBlocks(
          file.toString(), sOffset, sLen);
        LocatedBlocks codingBlks = ((DistributedFileSystem) fs).getClient().getLocatedBlocks(
          codingFile.toString(), cOffset, cLen);

        LocatedBlocks[] blksArray = new LocatedBlocks[] { sourceBlks, codingBlks };

        for (LocatedBlocks blks : blksArray) {
          for (LocatedBlock loc : blks.getLocatedBlocks()) {
            if (loc.isCorrupt() || (loc.getLocations().length != 1)) {
              // Do not handle this group
              throw new IOException("Mover found a block which is not a valid candidate for moving");
            }
            if (excludedDis.contains(loc.getLocations()[0])) {
              moveMapDN.put(loc, null);
            } else {
              if (checkMoveAmongRacks(loc, excludedDis)) {
                moveMapRack.put(loc, null);
              }
              excludedDis.add(loc.getLocations()[0]);
            }
          }
        }

        LOG.debug("Group " + groupIdx + " info : ");
        LOG.debug("Src blks:");
        for (LocatedBlock loc : sourceBlks.getLocatedBlocks()) {
          LOG.debug(loc.toString());
        }
        for (LocatedBlock loc : codingBlks.getLocatedBlocks()) {
          LOG.debug(loc.toString());
        }
        LOG.debug("excludedDis:");
        for (DatanodeInfo di : excludedDis) {
          LOG.debug(di.toString());
        }
        LOG.debug("moveMapDN:");
        for (Map.Entry<LocatedBlock, DatanodeInfo> moveItem : moveMapDN.entrySet()) {
          LOG.debug(moveItem.getKey() + " : "
              + ((moveItem.getValue() == null) ? "null" : moveItem.getValue()));
        }
        LOG.debug("moveMapRack:");
        if (moveMapRack != null) {
          for (Map.Entry<LocatedBlock, DatanodeInfo> moveItem : moveMapRack.entrySet()) {
            LOG.debug(moveItem.getKey() + " : "
                + ((moveItem.getValue() == null) ? "null" : moveItem.getValue()));
          }
        }

        for (LocatedBlock loc : moveMapDN.keySet()) {
          DatanodeInfo di = chooseTarget(loc, false);
          if (di == null) {
            // It does not make sense to move blocks in this group
            throw new IOException("Mover cannot find a target DN to move blocks");
          }
          moveMapDN.put(loc, di);
          excludedDis.add(di);
        }

        if (moveMapRack != null) {
          for (LocatedBlock loc : moveMapRack.keySet()) {
            DatanodeInfo di = chooseTarget(loc, true);
            if (di == null) {
              // It does not make sense to move blocks in this group
              throw new IOException(
                  "Mover cannot find a target DN in different Rack to move blocks");
            }
            moveMapRack.put(loc, di);
            excludedDis.add(di);
          }
        }
        LOG.debug("moveMapDN after choosing target:");
        for (Map.Entry<LocatedBlock, DatanodeInfo> moveItem : moveMapDN.entrySet()) {
          LOG.debug(moveItem.getKey() + " : "
              + ((moveItem.getValue() == null) ? "null" : moveItem.getValue()));
        }
        LOG.debug("moveMapRack after choosing target:");
        if (moveMapRack != null) {
          for (Map.Entry<LocatedBlock, DatanodeInfo> moveItem : moveMapRack.entrySet()) {
            LOG.debug(moveItem.getKey() + " : "
                + ((moveItem.getValue() == null) ? "null" : moveItem.getValue()));
          }
        }
      }

      private void doMove() throws IOException {
        try {
          buildMoveMap();
        } catch (IOException ioe) {
          failedBuildingMovingMap.increment(1);
          throw ioe;
        }

        int connectTimeout = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_CONNECT_TIMEOUT,
          HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_CONNECT_TIMEOUT_DEFAULT);
        /*
         * As in balancer, we don't have a good way to know if DN is taking a really long time to
         * move a block or just sth went wrong. We simply set the timeout to (default) 20 mins as
         * the same as what the balancer does.
         */
        int moveTimeout = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_MOVEONEBLOCK_TIMEOUT,
          HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_MOVEONEBLOCK_TIMEOUT_DEFAULT);

        try {
          for (Map.Entry<LocatedBlock, DatanodeInfo> moveItem : moveMapDN.entrySet()) {
            LocatedBlock lb = moveItem.getKey();
            lb.setBlockToken(MRUtils.getAccessToken(lb.getBlock(), EnumSet.of(
              BlockTokenSecretManager.AccessMode.REPLACE, BlockTokenSecretManager.AccessMode.COPY),
              MoverMapper.this.blockTokenSecretManager));
            Mover.moveOneBlock(connectTimeout, moveTimeout, lb, moveItem.getValue());
            movedBlocks.increment(1);
          }

          if (moveMapRack != null) {
            for (Map.Entry<LocatedBlock, DatanodeInfo> moveItem : moveMapRack.entrySet()) {
              LocatedBlock lb = moveItem.getKey();
              lb.setBlockToken(MRUtils.getAccessToken(lb.getBlock(), EnumSet.of(
                BlockTokenSecretManager.AccessMode.REPLACE, BlockTokenSecretManager.AccessMode.COPY),
                MoverMapper.this.blockTokenSecretManager));
              Mover.moveOneBlock(connectTimeout, moveTimeout, lb, moveItem.getValue());
              movedBlocks.increment(1);
            }
          }
        } catch (IOException ioe) {
          failedMoving.increment(1);
          LOG.warn("Failed to moving blocks", ioe);
        }
      }
    }
  }

  /**
   * The mover information input format class.
   */
  private static class MoverInfoInputFormat extends InputFormat {
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      FileSystem fs = FileSystem.get(conf);

      String collectResultFile = conf
          .get(HdfsRaidConfigKeys.HDFS_RAIDNODE_COLLECTOR_RESULT_FILE_KEY);
      int mapTaskNum = conf.getInt(HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_MAP_TASK_NUM_KEY,
        HdfsRaidConfigKeys.HDFS_RAIDNODE_MOVER_MAP_TASK_NUM_DEFAULT);
      List<InputSplit> result = new ArrayList<InputSplit>(mapTaskNum);

      FSDataInputStream in = fs.open(new Path(collectResultFile));
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      String line;
      int count = 0;
      try {
        while ((line = reader.readLine()) != null) {
          MoverInfoSplit split;
          if (result.size() < mapTaskNum) {
            split = new MoverInfoSplit();
            result.add(split);
          } else {
            split = (MoverInfoSplit) result.get(count++ % mapTaskNum);
          }
          split.addMoverInfo(line.trim());
        }
      } finally {
        reader.close();
      }
      return result;
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      MoverInfoReader reader = new MoverInfoReader();
      reader.initialize(split, context);
      return reader;
    }
  }

  /**
   * The move information split class.
   */
  private static class MoverInfoSplit extends InputSplit implements Writable {

    private List<String> moverInfos;

    public MoverInfoSplit() {
      moverInfos = new LinkedList<String>();
    }

    public void addMoverInfo(String fileInfo) {
      moverInfos.add(fileInfo);
    }

    public List<String> getMoverInfos() {
      return moverInfos;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return moverInfos.size();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
      String[] infos = new String[moverInfos.size()];
      moverInfos.toArray(infos);
      WritableUtils.writeStringArray(out, infos);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      String[] infos = WritableUtils.readStringArray(in);
      moverInfos.addAll(Arrays.asList(infos));
    }
  }

  /**
   * The mover information record reader class.
   */
  private static class MoverInfoReader extends RecordReader<Object, Text> {

    private MoverInfoSplit split;
    private int nextIndex;
    private Text current;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
        InterruptedException {
      this.split = (MoverInfoSplit) split;
      this.nextIndex = 0;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      List<String> moverInfos = split.getMoverInfos();
      if (moverInfos == null || moverInfos.isEmpty()) {
        return false;
      }
      if (nextIndex < moverInfos.size()) {
        current = new Text(split.getMoverInfos().get(nextIndex));
        nextIndex++;
        return true;
      }
      return false;
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
      return null;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return current;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return nextIndex * 1.0f / split.getMoverInfos().size();
    }

    @Override
    public void close() throws IOException {
    }
  }
}
