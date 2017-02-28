/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.tools;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.FederatedDFSFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.FedMvOptions;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Random;

import com.google.common.annotations.VisibleForTesting;

public class FedMv extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(FedMv.class);

  // Exit code
  private static final int SUCCESS = 0;
  private static final int DISTCP_ERROR = 1;
  private static final int DIR_NOTEXIST_ONDEST = 2;
  private static final int UNKNOWN_ERROR = 3;


  public int run(String[] args) throws Exception {
    // Step 1 : get option and verify if they are legal
    FedMvOptions inputOption = FedMvOptions.parse(args);
    checkOperation(inputOption);

    // Step 2 : run distcp with FedMoveMapper (instead of CoppyMapper)
    String[] distCpArgs = inputOption.buildDistCpArgs();
    if (runDistCp(distCpArgs) == false) {
      return DISTCP_ERROR;
    }

    // Step 3 : make sure all source dirs are on dest and move source to trash
    boolean allRemoved = true;
    for (Path src : inputOption.getSourcePath()) {
      if (canCleanSource(src, inputOption.getTargetPath(), true)) {
        // Move to trash
        Trash.moveToAppropriateTrash(src.getFileSystem(getConf()), src,
            getConf());
      } else {
        allRemoved = false;
      }
    }
    return allRemoved ? SUCCESS : UNKNOWN_ERROR;
  }

  private void checkOperation(FedMvOptions inputOption)
      throws IllegalArgumentException, IOException {
    if (inputOption.getTargetPath() == null
        || inputOption.getSourcePath().isEmpty()) {
      throw new IllegalArgumentException(
          "No correct source paths or target path");
    }
    // Make sure all paths are FederatedDFSFileSystem
    FileSystem[] fses = new FileSystem[1 + inputOption.getSourcePath().size()];
    fses[0] = inputOption.getTargetPath().getFileSystem(getConf());
    for (int i = 1; i < fses.length; i++) {
      fses[i] = inputOption.getSourcePath().get(i - 1).getFileSystem(getConf());
    }
    for (int i = 0; i < fses.length; i++) {
      if (!(fses[i] instanceof FederatedDFSFileSystem)) {
        throw new IllegalArgumentException("Not in a federated file system");
      }
      if (!fses[i].getScheme().equals(fses[0].getScheme())
          || !fses[i].getUri().toString().equals(fses[0].getUri().toString())) {
        throw new IllegalArgumentException(
            "Not in the same federated file system");
      }
    }
  }
  
  private boolean runDistCp(String[] args) throws Exception {
    int exitCode = SUCCESS;
    try {
      DistCp dc = new DistCp();
      getConf().setBoolean(DistCpConstants.DISTCP_RENAME_FOR_COPY, true);
      exitCode = ToolRunner.run(getConf(), dc, args);
    } catch (Exception e) {
      LOG.error("Couldn't complete DistCp operation: ", e);
      exitCode = DISTCP_ERROR;
    }
    return exitCode == SUCCESS;
  }

  private boolean canCleanSource(Path source, Path dest, boolean mayBeFile)
      throws IOException {
    FileSystem sourceFs = source.getFileSystem(getConf());
    FileSystem destFs = source.getFileSystem(getConf());
    if (sourceFs.exists(source) && destFs.exists(dest)
        && (sourceFs.isDirectory(source) == destFs.isDirectory(dest))) {
      if (sourceFs.isDirectory(source)) {
        FileStatus[] sts = sourceFs.listStatus(source);
        for (FileStatus st : sts) {
          Path tmpSource = new Path(source, st.getPath().getName());
          Path tmpDest = new Path(dest, st.getPath().getName());
          if (!canCleanSource(tmpSource, tmpDest, false)) {
            return false;
          }
        }
      } else {
        if (mayBeFile) {
          return true;
        } else {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  public static void main(String[] argv) {
    int exitCode = SUCCESS;
    try {
      FedMv fedMv = new FedMv();
      exitCode = ToolRunner.run(new Configuration(), fedMv, argv);
    } catch (Exception e) {
      LOG.error("Couldn't complete FedMv operation: ", e);
      exitCode = UNKNOWN_ERROR;
    }
    System.exit(exitCode);
  }
}
