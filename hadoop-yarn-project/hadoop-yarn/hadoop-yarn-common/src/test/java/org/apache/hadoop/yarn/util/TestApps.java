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

package org.apache.hadoop.yarn.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestApps {
  @Test
  public void testSingleEnv() {
    Map<String, String> envs = new HashMap<String, String>();
    String env = "A=B";
    Apps.setEnvFromInputString(envs, env, "/");
    Assert.assertEquals("map size is not 1", 1, envs.size());
  }

  @Test
  public void testMultipleEnvs() {
    Map<String, String> envs = new HashMap<String, String>();
    String env = "A=1,B=2";
    Apps.setEnvFromInputString(envs, env, "/");
    Assert.assertEquals("map size is not 2", 2, envs.size());
  }

  @Test
  public void testEnvWithDoubleQuotes() {
    Map<String, String> envs = new HashMap<String, String>();
    String env = "A=1,B=\"p1:1,p2:2\"";
    Apps.setEnvFromInputString(envs, env, "/");
    Assert.assertEquals("map size is not 2", 2, envs.size());
    Assert.assertEquals("element B of the map is not p1:1,p2:2", "p1:1,p2:2", envs.get("B"));
  }
}
