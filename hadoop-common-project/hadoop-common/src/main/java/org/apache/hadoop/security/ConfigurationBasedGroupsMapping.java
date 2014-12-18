package org.apache.hadoop.security;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.log4j.Logger;

public class ConfigurationBasedGroupsMapping implements
    GroupMappingServiceProvider, Configurable {

  private static final Logger LOG = Logger.getLogger(
      ConfigurationBasedGroupsMapping.class);

  private Configuration conf;
  private String groupsMappingFile;
  private Map<String, List<String>> userGroupsMapping;

  private static final String DEFAULT_GROUP = "hadoop";

  /**
   * Returns list of groups for a user
   *
   * @param user get groups for this user
   * @return list of groups for a given user
   */
  @Override
  public synchronized List<String> getGroups(String user) throws IOException {
    if (userGroupsMapping == null) {
      this.loadGroupsMappingFile();
    }

    List<String> groups = userGroupsMapping.get(user);
    if (groups == null) {
      groups = new LinkedList<String>();
    }

    if (groups.isEmpty()) {
      // If the user's group information isn't configured or misconfigured
      // to empty, we suppose that it belongs to the DEFAULT_GROUP and the
      // group named after its own name.
      groups.add(user);
      groups.add(DEFAULT_GROUP);
    }

    return groups;
  }

  /**
   * Refresh the groups by reloading the groups mapping file
   */
  @Override
  public synchronized void cacheGroupsRefresh() throws IOException {
    this.loadGroupsMappingFile();
  }

  @Override
  public void cacheGroupsAdd(List<String> groups) throws IOException {
    // All user groups mapping information will be loaded from the
    // configured file, so this method needs do nothing so far.
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  String getGroupsMappingFile() {
    if (this.groupsMappingFile == null) {
      this.groupsMappingFile = conf.get(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUP_MAPPING_FILE_KEY);
    }
    return groupsMappingFile;
  }

  void loadGroupsMappingFile() throws IOException {
    if (StringUtils.isBlank(getGroupsMappingFile())) {
      throw new IOException("The groups mapping config file name is blank");
    }

    if (this.userGroupsMapping != null) {
      this.userGroupsMapping.clear();
    } else {
      this.userGroupsMapping = new HashMap<String, List<String>>();
    }

    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(this.groupsMappingFile));
      String line = reader.readLine();

      while (line != null) {
        line = StringUtils.strip(line);
        if (line.length() != 0 && !line.startsWith("#")) {
          processMappingInfo(line);
        }
        line = reader.readLine();
      }
    } catch (Exception e) {
      throw new IOException("Read groups mapping file failed: " +
          this.groupsMappingFile, e);
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  void processMappingInfo(String line) {
    // The line is in the format of 'user1 group1,group2,...'
    int index = StringUtils.indexOf(line, ' ');
    if (index == -1) {
      LOG.error("Invalid groups mapping: " + line);
      return;
    }
    String user = StringUtils.substring(line, 0, index);

    line = StringUtils.substring(line, index + 1);
    String[] groupList = StringUtils.split(line, ", ");
    List<String> groups = new LinkedList<String>();
    for (String group: groupList) {
      if (StringUtils.isNotBlank(group)) {
        groups.add(StringUtils.strip(group));
      }
    }

    this.userGroupsMapping.put(user, groups);
  }
}
