package org.apache.hadoop.security;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestConfigurationBasedGroupsMapping {
  private ConfigurationBasedGroupsMapping groupsMapping;

  @Before
  public void setUp() {
    System.setProperty("hadoop.conf.dir", "/tmp");
    groupsMapping = new ConfigurationBasedGroupsMapping();
    Configuration conf = new Configuration();
    groupsMapping.setConf(conf);
  }

  @Test
  public void testUserGroupsMapping() throws IOException {
    Map<String, List<String>> userGroupsMapping = new HashMap<String, List<String>>();

    // one line case
    String mappingInfo = "user1 group1, group2\n";
    initGroupsMappingFile(mappingInfo);
    // ensure that groups mapping file is loaded
    groupsMapping.cacheGroupsRefresh();
    userGroupsMapping.put("user1", Arrays.asList(StringUtils.split("group1, group2", ", ")));
    checkUserGroupsMapping(userGroupsMapping);

    // multiple line case
    userGroupsMapping.clear();
    mappingInfo = "user1 group1,group2\nuser2 group1\n";
    initGroupsMappingFile(mappingInfo);
    groupsMapping.cacheGroupsRefresh();
    userGroupsMapping.put("user1", Arrays.asList(StringUtils.split("group1, group2", ", ")));
    userGroupsMapping.put("user2", Arrays.asList(StringUtils.split("group1", ", ")));
    checkUserGroupsMapping(userGroupsMapping);

    // misconfigured case
    userGroupsMapping.clear();
    mappingInfo = "user1 group1,group2\nuser2 \n";
     initGroupsMappingFile(mappingInfo);
    groupsMapping.cacheGroupsRefresh();
    userGroupsMapping.put("user1", Arrays.asList(StringUtils.split("group1, group2", ", ")));
    userGroupsMapping.put("user2", Arrays.asList(StringUtils.split("user2, hadoop", ", ")));
    checkUserGroupsMapping(userGroupsMapping);

    // misconfigured case
    userGroupsMapping.clear();
    mappingInfo = "user1 group1,group2\nuser2\n";
     initGroupsMappingFile(mappingInfo);
    groupsMapping.cacheGroupsRefresh();
    userGroupsMapping.put("user1", Arrays.asList(StringUtils.split("group1, group2", ", ")));
    userGroupsMapping.put("user2", Arrays.asList(StringUtils.split("user2, hadoop", ", ")));
    checkUserGroupsMapping(userGroupsMapping);
  }

  private void initGroupsMappingFile(String mappingInfo) throws IOException {
    // write the mapping file
    String mappingFile = groupsMapping.getGroupsMappingFile();
    FileWriter writer = null;
    try {
      writer = new FileWriter(mappingFile);
      writer.write(mappingInfo);
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  private void checkUserGroupsMapping(Map<String, List<String>> userGroupsMapping)
  {
    try {
      for (String user: userGroupsMapping.keySet()) {
        List<String> expectedGroups = userGroupsMapping.get(user);
        List<String> actualGroups = this.groupsMapping.getGroups(user);
        Assert.assertEquals(expectedGroups.size(), actualGroups.size());
        for (String group: expectedGroups) {
          Assert.assertTrue(actualGroups.contains(group));
        }
      }
    } catch (IOException e) {
      Assert.fail("getGroups() fail: " + e);
    }
  }
}
