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

import java.lang.String;
import java.util.List;
import java.util.LinkedList;
import java.lang.IllegalArgumentException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

// TBD: This will be fully implemented in the task of T3048
public class Policy {
  private Configuration conf;
  private List<PolicyEntry> peList;

  // Used for communication between RaidShell/RaidNode to
  // pass policy information.
  private String cookie;

  public Policy(Configuration conf) {
    this.conf = conf;
    this.peList = new LinkedList<PolicyEntry>();
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

    public String getPath() {
      return appliedDir.toString();
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
        if (appliedDir.toString().equals(((PolicyEntry) anObject).getPath().toString())
            && interval == ((PolicyEntry) anObject).getInterval()) {
          return true;
        }
      }
      return false;
    }
  }

  // TBD: Add the logic to implement parsing policies.
  public void parsePolicy() throws IOException {
  }

  // TBD: Add logic to handle cookie in case of too large packet issue
  public Policy getPolicyInfos(String cookie) {
    return this;
  }

  public List<PolicyEntry> getPolicyEntries() {
    return peList;
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
