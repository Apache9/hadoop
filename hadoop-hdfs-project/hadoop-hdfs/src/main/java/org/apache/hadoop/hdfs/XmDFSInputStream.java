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
package org.apache.hadoop.hdfs;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.util.Time;
import org.mortbay.log.Log;


/****************************************************************
 * XmDFSInputStream is to extend DFSInputStream. If the file is being written by
 * somebody else, XmDFSInputStream will update the file's length information if
 * the reading goes beyond the current EOF.
 ****************************************************************/
@InterfaceAudience.Private
public class XmDFSInputStream extends DFSInputStream {
  private String srcFile;
  private DFSClient dfsClient;
  private long sleepBeforeRetry;
  private boolean needRefreshLocatedBlocks = false;
  private Map<DatanodeInfo, ClientDatanodeProtocol> cachedCDP;

  // CacheStatus
  private long requestRefreshLocatedBlocks = 0;
  private long doRefreshLocatedBlocks = 0;
  private long requestNewCDP = 0;
  private long doNewCDP = 0;
  private long blockSize = 0;

  XmDFSInputStream(DFSClient dfsClient, String src, int buffersize,
      boolean verifyChecksum) throws IOException, UnresolvedLinkException {
    super(dfsClient, src, buffersize, verifyChecksum);
    this.srcFile = src;
    this.dfsClient = dfsClient;
    this.sleepBeforeRetry =
        dfsClient
            .getConfiguration()
            .getLong(
                DFSConfigKeys.DFS_CLIENT_XIAOMI_INPUT_SLEEP_BEFORE_RETRY_MS,
                DFSConfigKeys.DFS_CLIENT_XIAOMI_INPUT_SLEEP_BEFORE_RETRY_MS_DEFAULT);
    this.blockSize = dfsClient.getFileInfo(src).getBlockSize();
  }

  public String getCacheStatus(){
    return "RefreshLocatedBlocks(" + requestRefreshLocatedBlocks + "," + doRefreshLocatedBlocks + ");" +
           "NewCDP("+ requestNewCDP + "," + doNewCDP + ")";
  }

  @Override
  public int read() throws IOException {
    return super.read();
  }

  private int readInternal(final ByteBuffer bBuf, long position,
      final byte buf[], int off, int len) throws IOException {
    int readLen;
    if (bBuf != null) {
      readLen = super.read(bBuf);
    } else {
      if (position == -1) {
        readLen = super.read(buf, off, len);
      } else {
        readLen = super.read(position, buf, off, len);
      }
    }
    while (readLen == -1) {
      boolean fileClosed = dfsClient.isFileClosed(srcFile);
      long origLen = getFileLength();
      try {
        updateFileLength();
      } catch (IOException ioe) {
        if (isNeedToRetry(ioe)) {
          updateFileLength();
        } else {
          throw ioe;
        }
      }
      long newLen = getFileLength();
      if (origLen == newLen) {
        if (fileClosed) {
          // Nobody is writing the file then nothing to read
          break;
        } else {
          if (sleepBeforeRetry != 0) {
            try {
              Thread.sleep(sleepBeforeRetry);
            } catch (InterruptedException ie) {
              break;
            }
          } else {
            // Yield so that there is a higher possibility that some more data
            // is written to the file when calling following updateFileLength().
            Thread.yield();
          }
          continue;
        }
      } else {
        if (bBuf != null) {
          readLen = super.read(bBuf);
        } else {
          if (position == -1) {
            readLen = super.read(buf, off, len);
          } else {
            readLen = super.read(position, buf, off, len);
          }
        }
        break;
      }
    }
    return readLen;
  }

  @Override
  public int read(final byte buf[], int off, int len) throws IOException {
    return readInternal(null, -1, buf, off, len);
  }

  @Override
  public int read(final ByteBuffer buf) throws IOException {
    return readInternal(buf, -1, null, 0, 0);
  }

  /**
   * Read bytes starting from the specified position.
   * 
   * @param position start read from this position
   * @param buffer read buffer
   * @param offset offset into buffer
   * @param length number of bytes to read
   * 
   * @return actual number of bytes read
   */
  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    return readInternal(null, position, buffer, offset, length);
  }

  /**
   * Seek to a new arbitrary location
   */
  @Override
  public void seek(long targetPos) throws IOException {
    while (true) {
      IOException originalExp = null;
      long origLen = getFileLength();
      try {
        seekToBlockSource(targetPos);
        super.seek(targetPos);
        break;
      } catch (IOException ioe) {
        if (isDFSStreamClosed()) {
          throw ioe;
        } else {
          originalExp = ioe;
        }
      } catch (NullPointerException npe) {
        originalExp = new IOException("Fail to seek to " + targetPos);
      }

      boolean fileClosed = dfsClient.isFileClosed(srcFile);
      updateFileLength();
      long newLen = getFileLength();
      if (origLen == newLen) {
        if (fileClosed) {
          // Nobody is writing the file then nothing to read
          throw originalExp;
        } else {
          if (sleepBeforeRetry != 0) {
            try {
              Thread.sleep(sleepBeforeRetry);
            } catch (InterruptedException ie) {
              break;
            }
          } else {
            // Yield so that there is a higher possibility that some more data
            // is written to the file when calling following updateFileLength().
            Thread.yield();
          }
        }
      }
    }
  }

  @Override
  protected ClientDatanodeProtocol getClientDatanodeProtocol(DatanodeInfo datanodeInfo)
  {
    ++requestNewCDP;
    if (null != cachedCDP && cachedCDP.containsKey(datanodeInfo)) {
      return cachedCDP.get(datanodeInfo);
    }
    ++doNewCDP;
    if (requestNewCDP%100 == 0) {
      DFSClient.LOG.info("getClientDatanodeProtocol: cache status: " + getCacheStatus());
    }
    return null;
  }

  @Override
  protected void saveClientDatanodeProtocol(DatanodeInfo datanodeInfo, ClientDatanodeProtocol cdp)
  {
    if (null == cachedCDP) {
      cachedCDP = new TreeMap<DatanodeInfo, ClientDatanodeProtocol> ();
    }
    cachedCDP.put(datanodeInfo, cdp);
  }

  @Override
  protected void releaseClientDatanodeProtocol(ClientDatanodeProtocol cdp) {
  }

  @Override
  protected void clearClientDatanodeProtocol()
  {
    if (null != cachedCDP) {
      DFSClient.LOG.info("clearClientDatanodeProtocol: cache status: " + getCacheStatus());
      for (ClientDatanodeProtocol cdp : cachedCDP.values()) {
        if (null != cdp) {
          RPC.stopProxy(cdp);
        }
      }
      cachedCDP.clear();
    }
  }

  @Override
  protected boolean needRefreshLocatedBlocks() {
    ++requestRefreshLocatedBlocks;
    if (needRefreshLocatedBlocks) {
      ++doRefreshLocatedBlocks;
    }
    if (requestRefreshLocatedBlocks%100 == 0) {
      DFSClient.LOG.info("needRefreshLocatedBlocks: cache status: " + getCacheStatus());
    }
    return needRefreshLocatedBlocks;
  }

  @Override
  protected void setRefreshLocatedBlocks(boolean val) {
    needRefreshLocatedBlocks = val;
  }

  @Override
  protected long getBlockSize() {
    return blockSize;
  }

  protected boolean isNeedToRetry(IOException ioe) {
    if (ioe instanceof RemoteException
        && (((RemoteException) ioe).unwrapRemoteException() instanceof SecretManager.InvalidToken)) {
      return true;
    }
    return false;
  }
}
