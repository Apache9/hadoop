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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

// TBD: This will be fully implemented in the task of T3048
public class Policy {
  private Configuration conf;
  private List<PolicyEntry> peList;

  // Used for communication between RaidShell/RaidNode to
  // pass policy information.
  private String cookie;
  // TBD: To decide the policy should be auto reloaded at fixed frequency or should be only
  // refreshed per user's request from RaidShell.
  private long lastLoadTicks;
  private ReentrantReadWriteLock lock;

  public Policy(Configuration conf) {
    this(new LinkedList<PolicyEntry>(), conf);
  }

  public Policy(List<PolicyEntry> peList, Configuration conf) {
    this.conf = conf;
    this.peList = peList;
    cookie = null;
    lastLoadTicks = 0;
    lock = new ReentrantReadWriteLock();
  }

  // At this point of time, one policy is supposed to be a
  // scan configuration for a specific directory.
  // Be note: this is subject to change with the progress of
  // the project.
  static public class PolicyEntry {
    private Path appliedDir;
    // Scan interval in seconds
    private long interval;

    public PolicyEntry(Path dir, long interval) throws IllegalArgumentException {
      // TBD: if dir is not a directory, throws an IllegalArgumentException.
      this.appliedDir = dir;
      this.interval = interval;
    }

    private void showInfo() {
      System.out.println("Policy for " + appliedDir + ":");
      System.out.println("  scan every " + interval + " seconds.");
    }

    public String getPathStr() {
      return appliedDir.toString();
    }

    public Path getPath() {
      return appliedDir;
    }

    public long getInterval() {
      return interval;
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof PolicyEntry) {
        if (appliedDir.toString().equals(((PolicyEntry) anObject).getPathStr())
            && interval == ((PolicyEntry) anObject).getInterval()) {
          return true;
        }
      }
      return false;
    }
  }

  // TBD: Implement more elegant logic to parse policies.
  private void parsePolicy() throws IOException {
    String rawPolicyStr = conf.get(HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_POLICY_KEY);
    if (rawPolicyStr != null) {
      String[] policyStrs = rawPolicyStr.trim().split("\\s+");
      for (String policy : policyStrs) {
        String[] policyInfo = policy.split(":");
        if (policyInfo.length != 2) {
          throw new IOException("Incorrect policy format");
        }
        try {
          long interval = Long.parseLong(policyInfo[1]);
          addNewPolicy(policyInfo[0], interval);
        } catch (NumberFormatException e) {
          throw new IOException("Incorrect interval value, it should be an integer");
        }
      }
    }
  }

  public void loadPolicy(Configuration conf) throws IOException {
    long currTicks = System.currentTimeMillis();
    long reloadInterval = conf.getLong(
      HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_POLICY_RELOAD_INTERVAL,
      HdfsRaidConfigKeys.HDFS_RAIDNODE_RAID_POLICY_RELOAD_INTERVAL_DEFAULT);
    if ((currTicks - lastLoadTicks > reloadInterval) || (lastLoadTicks == 0)) {
      // Should use a java timer to do auto- reload?
      lock.writeLock().lock();
      try {
        this.conf = conf;
        this.peList = new LinkedList<PolicyEntry>();
        parsePolicy();
        this.lastLoadTicks = currTicks;
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  // TBD: Add logic to handle cookie in case of too large packet issue
  public Policy getPolicyInfos(String cookie) {
    Policy res = null;
    lock.readLock().lock();
    res = new Policy(this.peList, this.conf);
    lock.readLock().unlock();
    return res;
  }

  public List<PolicyEntry> getPolicyEntries() {
    return peList;
  }

  public List<Path> getCandidateDirs() {
    List<Path> res = new LinkedList<Path>();
    for (PolicyEntry pe : peList) {
      res.add(pe.getPath());
    }
    return res;
  }

  public boolean isRaidCandidate(String path) {
    for (PolicyEntry pe : peList) {
      if (path.startsWith(pe.getPathStr())) {
        return true;
      }
    }
    return false;
  }

  public String getCookie() {
    return cookie;
  }

  public void setCookie(String cookie) {
    this.cookie = cookie;
  }

  public void showPolicy() {
    for (PolicyEntry pe : peList) {
      pe.showInfo();
    }
  }

  public void addNewPolicy(String path, long interval) {
    PolicyEntry pe = new PolicyEntry(new Path(path), interval);
    peList.add(pe);
  }

  @Override
  public boolean equals(Object anObject) {
    if (this == anObject) {
      return true;
    }

    if (anObject instanceof Policy) {
      if (peList.size() != ((Policy) anObject).getPolicyEntries().size()) {
        return false;
      }
      if (peList.isEmpty()) {
        return true;
      }
      for (PolicyEntry pe : peList) {
        if (((Policy) anObject).getPolicyEntries().contains(pe) == false) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
}
