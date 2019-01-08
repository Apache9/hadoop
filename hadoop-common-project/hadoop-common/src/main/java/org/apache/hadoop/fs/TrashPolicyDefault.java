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
package org.apache.hadoop.fs;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.ATTR_TRASH_TTL_NAME;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Time;

/** Provides a <i>trash</i> feature.  Files are moved to a user's trash
 * directory, a subdirectory of their home directory named ".Trash".  Files are
 * initially moved to a <i>current</i> sub-directory of the trash directory.
 * Within that sub-directory their original path is preserved.  Periodically
 * one may checkpoint the current trash and remove older checkpoints.  (This
 * design permits trash management without enumeration of the full trash
 * content, without date support in the filesystem, and without clock
 * synchronization.)
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TrashPolicyDefault extends TrashPolicy {
  private static final Log LOG =
    LogFactory.getLog(TrashPolicyDefault.class);

  private static final Path CURRENT = new Path("Current");
  private static final Path TRASH = new Path(".Trash/");

  private static final String TRASH_TTL_CONF_FILE = "TrashTTL.conf";

  private static final FsPermission PERMISSION =
    new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

  private static final DateFormat CHECKPOINT = new SimpleDateFormat("yyMMddHHmmss");
  /** Format of checkpoint directories used prior to Hadoop 0.23. */
  private static final DateFormat OLD_CHECKPOINT =
      new SimpleDateFormat("yyMMddHHmm");
  private static final int MSECS_PER_MINUTE = 60*1000;

  private Path current;
  private Path homesParent;
  private long emptierInterval;

  public TrashPolicyDefault() { }

  private TrashPolicyDefault(FileSystem fs, Path home, Configuration conf)
      throws IOException {
    initialize(conf, fs, home);
  }

  @Override
  public void initialize(Configuration conf, FileSystem fs, Path home) {
    this.fs = fs;
    this.trash = new Path(home, TRASH);
    this.homesParent = home.getParent();
    this.current = new Path(trash, CURRENT);
    this.deletionInterval = (long)(conf.getFloat(
        FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT)
        * MSECS_PER_MINUTE);
    this.emptierInterval = (long)(conf.getFloat(
        FS_TRASH_CHECKPOINT_INTERVAL_KEY, FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT)
        * MSECS_PER_MINUTE);
    LOG.info("Namenode trash configuration: Deletion interval = " +
             (this.deletionInterval / MSECS_PER_MINUTE) + " minutes, Emptier interval = " +
             (this.emptierInterval / MSECS_PER_MINUTE) + " minutes.");
   }

  private Path makeTrashRelativePath(Path basePath, Path rmFilePath) {
    return Path.mergePaths(basePath, rmFilePath);
  }

  @Override
  public boolean isEnabled() {
    return deletionInterval != 0;
  }

  @Override
  public boolean moveToTrash(Path path) throws IOException {
    if (!isEnabled())
      return false;

    if (!path.isAbsolute())                       // make path absolute
      path = new Path(fs.getWorkingDirectory(), path);
    if (!fs.exists(path))                         // check that path exists
      throw new FileNotFoundException(path.toString());

    String qpath = fs.makeQualified(path).toString();
    if (qpath.startsWith(trash.toString())) {
      return false;                               // already in trash
    }

    if (trash.getParent().toString().startsWith(qpath)) {
      throw new IOException("Cannot move \"" + path +
                            "\" to the trash, as it contains the trash");
    }

    Path trashPath = makeTrashRelativePath(current, path);
    Path baseTrashPath = makeTrashRelativePath(current, path.getParent());

    IOException cause = null;

    // try twice, in case checkpoint between the mkdirs() & rename()
    for (int i = 0; i < 2; i++) {
      try {
        if (!fs.mkdirs(baseTrashPath, PERMISSION)) {      // create current
          LOG.warn("Can't create(mkdir) trash directory: "+baseTrashPath);
          return false;
        }
      } catch (FileAlreadyExistsException e) {
        // find the path which is not a directory, and modify baseTrashPath
        // & trashPath, then mkdirs
        Path existsFilePath = baseTrashPath;
        while (!fs.exists(existsFilePath)) {
          existsFilePath = existsFilePath.getParent();
        }

        // Case don't modify baseTrashPath when existsFilePath is deleted
        try {
          FileStatus fileStatus = fs.getFileStatus(existsFilePath);
          if (!fileStatus.isDirectory()) {
            baseTrashPath = new Path(baseTrashPath.toString().replace(
                    existsFilePath.toString(), existsFilePath.toString() + Time.now())
            );
            trashPath = new Path(baseTrashPath, trashPath.getName());
          }
        } catch (FileNotFoundException e1) {
          LOG.warn("The existFilePath is not found " + existsFilePath);
        }

        // retry, ignore current failure
        --i;
        continue;
      } catch (IOException e) {
        LOG.warn("Can't create trash directory: "+baseTrashPath);
        cause = e;
        break;
      }
      try {
        // if the target path in Trash already exists, then append with 
        // a current time in millisecs.
        String orig = trashPath.toString();
        
        while(fs.exists(trashPath)) {
          trashPath = new Path(orig + Time.now());
        }

        if (fs.rename(path, trashPath))           // move to current trash
          return true;
      } catch (IOException e) {
        cause = e;
      }
    }
    throw (IOException)
      new IOException("Failed to move to trash: "+path).initCause(cause);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void createCheckpoint() throws IOException {
    if (!fs.exists(current))                     // no trash, no checkpoint
      return;

    Path checkpointBase;
    synchronized (CHECKPOINT) {
      checkpointBase = new Path(trash, CHECKPOINT.format(new Date()));
    }
    Path checkpoint = checkpointBase;

    int attempt = 0;
    while (true) {
      try {
        fs.rename(current, checkpoint, Rename.NONE);
        break;
      } catch (FileAlreadyExistsException e) {
        if (++attempt > 1000) {
          throw new IOException("Failed to checkpoint trash: "+checkpoint);
        }
        checkpoint = checkpointBase.suffix("-" + attempt);
      }
    }

    LOG.info("Created trash checkpoint: "+checkpoint.toUri().getPath());
  }

  @Override
  public void deleteCheckpoint() throws IOException {
    deleteCheckpointInternal(deletionInterval);
  }

  @Override
  public Path getCurrentTrashDir() {
    return current;
  }

  @Override
  public Runnable getEmptier() throws IOException {
    return new Emptier(getConf(), emptierInterval);
  }

  private class Emptier implements Runnable {

    private Configuration conf;
    private long emptierInterval;
    private HashMap<String, Long> trashTTLConfig = null;

    Emptier(Configuration conf, long emptierInterval) throws IOException {
      this.conf = conf;
      this.emptierInterval = emptierInterval;
      if (emptierInterval > deletionInterval || emptierInterval == 0) {
        LOG.info("The configured checkpoint interval is " +
                 (emptierInterval / MSECS_PER_MINUTE) + " minutes." +
                 " Using an interval of " +
                 (deletionInterval / MSECS_PER_MINUTE) +
                 " minutes that is used for deletion instead");
        this.emptierInterval = deletionInterval;
      }
    }

    private void loadTrashConf() {

      // check if the rack config file is updated and OK
      File trashTTLConfFile = new File(TRASH_TTL_CONF_FILE);
      if (!trashTTLConfFile.exists()) {
        LOG.warn("the trash config file doesn't exist: " + trashTTLConfFile);
        return;
      }

      HashMap <String, Long> tempConfig = new HashMap<String, Long>();

      BufferedReader br = null;
      try {
        br = new BufferedReader(new InputStreamReader(new FileInputStream(trashTTLConfFile)));
        String line = null;
        while ((line = br.readLine()) != null) {
          String[] columns = line.split(",");
          if (columns == null || columns.length != 2) {
            continue;
          }
          String user = columns[0];
          try {
            Long interval = Long.parseLong(columns[1]) * MSECS_PER_MINUTE;
            tempConfig.put(user, interval);
          } catch (Throwable e) {
            LOG.error("error to parse trash config: " + line + " " + e);
          }
        }
      } catch (Exception e) {
        LOG.error("failed to load trash config " + e);
      } finally {
        if (br != null) {
          try {
            br.close();
          } catch (IOException e) {
            LOG.error("failed to close trash ttl conf file " + e);
          }
        }
      }

      trashTTLConfig = tempConfig;
      return;

    }

    private String getKBPathFromHome (FileStatus home) {
      String homePath = home.getPath().toUri().toString();
      String[] pathNodes = homePath.substring(homePath.indexOf("/user")).split("/");
      if (pathNodes == null || pathNodes.length < 3) {
        return null;
      }
      if (!pathNodes[1].equalsIgnoreCase("user")) {
        return null;
      }
      String kbPathStr = "/user/" + pathNodes[2];
      LOG.info("get kerberos path: " + kbPathStr);
      return kbPathStr;
    }

    @Override
    public void run() {
      if (emptierInterval == 0)
        return;                                   // trash disabled
      long now = Time.now();
      long end;
      while (true) {
        end = ceiling(now, emptierInterval);
        try {                                     // sleep for interval
          Thread.sleep(end - now);
        } catch (InterruptedException e) {
          break;                                  // exit on interrupt
        }

        try {
          now = Time.now();
          if (now >= end) {

            FileStatus[] homes = null;
            try {
              homes = fs.listStatus(homesParent);         // list all home dirs
            } catch (IOException e) {
              LOG.warn("Trash can't list homes: "+e+" Sleeping.");
              continue;
            }

            loadTrashConf();

            for (FileStatus home : homes) {         // dump each trash
              if (!home.isDirectory())
                continue;
              try {
                TrashPolicyDefault trash = new TrashPolicyDefault(
                    fs, home.getPath(), conf);
                long userSpecifiedDeletionInterval = deletionInterval;
                String kbPathStr = getKBPathFromHome(home);
                long trashttl = getTrashTTL(kbPathStr);
                if (trashttl > 0) {
                  userSpecifiedDeletionInterval = trashttl;
                }
                trash.deleteCheckpoint(userSpecifiedDeletionInterval);
                trash.createCheckpoint();
              } catch (IOException e) {
                LOG.warn("Trash caught: "+e+". Skipping "+home.getPath()+".");
              } 
            }
          }
        } catch (Exception e) {
          LOG.warn("RuntimeException during Trash.Emptier.run(): ", e); 
        }
      }
      try {
        fs.close();
      } catch(IOException e) {
        LOG.warn("Trash cannot close FileSystem: ", e);
      }
    }

    private long ceiling(long time, long interval) {
      return floor(time, interval) + interval;
    }
    private long floor(long time, long interval) {
      return (time / interval) * interval;
    }

    private long getTrashTTL(String kbPathStr) throws IOException {
      long trashttl = 0;
      Path kbPath = new Path(kbPathStr);
      if (fs.exists(kbPath)) {
        Map<String, byte[]> allXAttrs = fs.getXAttrs(kbPath);
        if (allXAttrs != null && allXAttrs.containsKey(ATTR_TRASH_TTL_NAME)) {
          byte[] value = allXAttrs.get(ATTR_TRASH_TTL_NAME);
          String s = StringUtils.newStringUtf8(value);
          try {
            trashttl = Long.parseLong(s) * MSECS_PER_MINUTE;
            LOG.info("kbPath: " + kbPathStr + " , " + "trashttl: " + trashttl);
          } catch (NumberFormatException e) {
            LOG.error("can not parse string to long");
          }
        }
      }
      return trashttl;
    }
  }

  private long getTimeFromCheckpoint(String name) throws ParseException {
    long time;

    try {
      synchronized (CHECKPOINT) {
        time = CHECKPOINT.parse(name).getTime();
      }
    } catch (ParseException pe) {
      // Check for old-style checkpoint directories left over
      // after an upgrade from Hadoop 1.x
      synchronized (OLD_CHECKPOINT) {
        time = OLD_CHECKPOINT.parse(name).getTime();
      }
    }

    return time;
  }

  /**
   * Delete old trash checkpoint(s) with user-specified deletion interval.
   */
  public void deleteCheckpoint(long userDeletionInterval) throws IOException {
    deleteCheckpointInternal(userDeletionInterval);
  }

  private void deleteCheckpointInternal(long userDeletionInterval) throws IOException {
    FileStatus[] dirs = null;

    try {
      dirs = fs.listStatus(trash);            // scan trash sub-directories
    } catch (FileNotFoundException fnfe) {
      return;
    }

    long now = Time.now();
    for (int i = 0; i < dirs.length; i++) {
      Path path = dirs[i].getPath();
      String dir = path.toUri().getPath();
      String name = path.getName();
      if (name.equals(CURRENT.getName()))         // skip current
        continue;

      long time;
      try {
        time = getTimeFromCheckpoint(name);
      } catch (ParseException e) {
        LOG.warn("Unexpected item in trash: "+dir+". Ignoring.");
        continue;
      }

      if ((now - userDeletionInterval) > time) {
        if (fs.delete(path, true)) {
          LOG.info("Deleted trash checkpoint: "+dir);
        } else {
          LOG.warn("Couldn't delete checkpoint: "+dir+" Ignoring.");
        }
      }
    }
  }
}
