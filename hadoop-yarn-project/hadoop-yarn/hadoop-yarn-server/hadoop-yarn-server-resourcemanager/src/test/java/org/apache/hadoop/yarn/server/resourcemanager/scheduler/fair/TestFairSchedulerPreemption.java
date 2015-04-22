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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFairSchedulerPreemption extends FairSchedulerTestBase {
  private final static String ALLOC_FILE = new File(TEST_DIR,
      TestFairSchedulerPreemption.class.getName() + ".xml").getAbsolutePath();

  private MockClock clock;
  private RMNode node;

  private static class StubbedFairScheduler extends FairScheduler {
    Set<RMContainer> preemptedContainers = new HashSet<RMContainer>();
    @Override
    public void warnOrKillContainer(RMContainer container) {
      preemptedContainers.add(container);
      super.warnOrKillContainer(container);
    }

    public void resetLastPreemptResources() {
      preemptedContainers.clear();
    }
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, StubbedFairScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(FairSchedulerConfiguration.PREEMPTION, true);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    return conf;
  }

  @Before
  public void setup() throws IOException {
    conf = createConfiguration();
    clock = new MockClock();
  }

  @After
  public void teardown() {
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
    conf = null;
  }

  private void startResourceManager(float utilizationThreshold) {
    conf.setFloat(FairSchedulerConfiguration.PREEMPTION_THRESHOLD,
        utilizationThreshold);

    // Allow really kill container in 2nd calling of preemptTasksIfNecessary()
    conf.setInt(FairSchedulerConfiguration.PREEMPTION_INTERVAL, 1);
    conf.setInt(FairSchedulerConfiguration.WAIT_TIME_BEFORE_KILL, 0);

    resourceManager = new MockRM(conf);
    resourceManager.start();

    assertTrue(
        resourceManager.getResourceScheduler() instanceof StubbedFairScheduler);
    scheduler = (FairScheduler)resourceManager.getResourceScheduler();

    scheduler.setClock(clock);
    scheduler.updateInterval = 60 * 1000;
  }

  private void initClusterResource(int memory, int vcores) {
    node = MockNodes.newNodeInfo(
        1, Resources.createResource(memory, vcores), 1, "node1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node);
    scheduler.handle(nodeEvent1);

    assertEquals("Incorrect amount of resources in the cluster",
        memory, scheduler.rootMetrics.getAvailableMB());
    assertEquals("Incorrect amount of resources in the cluster",
        vcores, scheduler.rootMetrics.getAvailableVirtualCores());
  }

  private ApplicationAttemptId createSchedulingRequestAndSchedule(
      int memory, String queueId, String userId,
      int numContainers, int priority) {
    ApplicationAttemptId applicationAttemptId =
        createSchedulingRequest(memory, queueId, userId, numContainers,
            priority);
    scheduler.update();
    nodemanagerHearbeat();
    return applicationAttemptId;
  }

  private void nodemanagerHearbeat() {
    // Sufficient node check-ins to fully schedule containers
    for (int i = 0; i < node.getTotalCapability().getMemory() / 1024; i++) {
      NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node);
      scheduler.handle(nodeUpdate1);
    }
  }

  @Test
  public void testPreemptionForDepth1() throws Exception {
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<maxResources>0mb,0vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>1</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>1</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<fairSharePreemptionTimeout>5</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();

    startResourceManager(0.8f);
    // Create node with 4GB memory and 4 vcores
    initClusterResource(4 * 1024, 4);

    createSchedulingRequestAndSchedule(1024, "queueA", "user1", 3, 1);

    // Verify submitting another request doesn't trigger preemption
    createSchedulingRequest(1024, "queueB", "user1", 1, 1);
    scheduler.update();
    clock.tick(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    assertTrue("preemptResources() should not have been called",
        ((StubbedFairScheduler) scheduler).preemptedContainers.isEmpty());

    resourceManager.stop();

    startResourceManager(0.7f);
    // Create node with 4GB memory and 4 vcores
    initClusterResource(4 * 1024, 4);
    ApplicationAttemptId applicationAttemptId =
        createSchedulingRequestAndSchedule(1024, "queueA", "user1", 4, 1);

    createSchedulingRequest(1024, "queueB", "user1", 1, 1);
    scheduler.update();
    clock.tick(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    Set<RMContainer> preemptedContainers =
        ((StubbedFairScheduler) scheduler).preemptedContainers;
    assertEquals("Container(s) should have been preempted", 1,
        preemptedContainers.size());
    for (RMContainer preemptedContainer : preemptedContainers) {
      assertEquals(applicationAttemptId,
          preemptedContainer.getApplicationAttemptId());
    }
    clock.tick(2);
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    nodemanagerHearbeat();

    FSQueue queueB = scheduler.getQueueManager().getQueue("queueB");
    assertEquals(1024, queueB.getResourceUsage().getMemory());
  }

  @Test
  public void testPreemptionForDepth2Case1() throws Exception {
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<maxResources>0mb,0vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>1</weight>");
    out.println("  <queue name=\"1\">");
    out.println("  <weight>1</weight>");
    out.println("  </queue>");
    out.println("  <queue name=\"2\">");
    out.println("  <weight>1</weight>");
    out.println("  </queue>");
    out.println("</queue>");
    out.println("<fairSharePreemptionTimeout>5</fairSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionThreshold>0.8</defaultFairSharePreemptionThreshold>");
    out.println("</allocations>");
    out.close();

    startResourceManager(0.7f);
    // Create node with 4GB memory and 4 vcores
    initClusterResource(4 * 1024, 4);
    ApplicationAttemptId applicationAttemptId =
        createSchedulingRequestAndSchedule(1024, "queueA.1", "user1", 4, 1);

    createSchedulingRequest(1024, "queueA.2", "user1", 3, 1);
    scheduler.update();
    nodemanagerHearbeat();
    clock.tick(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    Set<RMContainer> preemptedContainers =
        ((StubbedFairScheduler) scheduler).preemptedContainers;
    assertEquals("Container(s) should have been preempted", 2,
        preemptedContainers.size());
    for (RMContainer preemptedContainer : preemptedContainers) {
      assertEquals(applicationAttemptId,
          preemptedContainer.getApplicationAttemptId());
    }
    clock.tick(2);
    scheduler.preemptTasksIfNecessary();

    scheduler.update();
    nodemanagerHearbeat();

    // verify final usage
    assertEquals(2048, scheduler.getQueueManager().getQueue("queueA.1")
        .getResourceUsage().getMemory());
    assertEquals(2048, scheduler.getQueueManager().getQueue("queueA.2")
        .getResourceUsage().getMemory());
  }

  @Test
  public void testPreemptionForDepth2Case2() throws Exception {
    // case: A2 should only preempt from sibling A1, when other queue B is
    // under fair share
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<maxResources>0mb,0vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>1</weight>");
    out.println("  <queue name=\"1\">");
    out.println("  <weight>1</weight>");
    out.println("  </queue>");
    out.println("  <queue name=\"2\">");
    out.println("  <weight>1</weight>");
    out.println("  </queue>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>1</weight>");
    out.println("</queue>");
    out.println("<fairSharePreemptionTimeout>5</fairSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionThreshold>0.8</defaultFairSharePreemptionThreshold>");
    out.println("</allocations>");
    out.close();

    startResourceManager(0.7f);
    // Create node with 4GB memory and 4 vcores
    initClusterResource(4 * 1024, 4);

    ApplicationAttemptId applicationAttemptId =
        createSchedulingRequestAndSchedule(1024, "queueA.1", "user1", 2, 1);
    createSchedulingRequestAndSchedule(1024, "queueB", "user1", 2, 1);

    createSchedulingRequest(1024, "queueA.2", "user1", 4, 1);
    scheduler.update();
    nodemanagerHearbeat();
    clock.tick(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    Set<RMContainer> preemptedContainers =
        ((StubbedFairScheduler) scheduler).preemptedContainers;
    assertEquals("Container(s) should have been preempted", 1,
        preemptedContainers.size());
    for (RMContainer preemptedContainer : preemptedContainers) {
      assertEquals("Should only preempt from sibling", applicationAttemptId,
          preemptedContainer.getApplicationAttemptId());
    }
    clock.tick(2);
    scheduler.preemptTasksIfNecessary();

    scheduler.update();
    nodemanagerHearbeat();

    // verify final usage
    assertEquals(1024, scheduler.getQueueManager().getQueue("queueA.1")
        .getResourceUsage().getMemory());
    assertEquals(1024, scheduler.getQueueManager().getQueue("queueA.2")
        .getResourceUsage().getMemory());
    assertEquals(2048, scheduler.getQueueManager().getQueue("queueB")
        .getResourceUsage().getMemory());
  }

  @Test
  public void testPreemptionForDepth2Case3() throws Exception {
    // case: A2 should preempt from A1 and B
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<maxResources>0mb,0vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>1</weight>");
    out.println("  <queue name=\"1\">");
    out.println("  <weight>1</weight>");
    out.println("  </queue>");
    out.println("  <queue name=\"2\">");
    out.println("  <weight>1</weight>");
    out.println("  </queue>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>1</weight>");
    out.println("</queue>");
    out.println("<fairSharePreemptionTimeout>5</fairSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionThreshold>0.8</defaultFairSharePreemptionThreshold>");
    out.println("</allocations>");
    out.close();

    startResourceManager(0.7f);
    initClusterResource(100 * 1024, 100);

    ApplicationAttemptId applicationAttemptIdA1 =
        createSchedulingRequestAndSchedule(1024, "queueA.1", "user1", 30, 1);
    ApplicationAttemptId applicationAttemptIdB =
        createSchedulingRequestAndSchedule(1024, "queueB", "user1", 70, 1);

    createSchedulingRequest(1024, "queueA.2", "user1", 50, 1);
    scheduler.update();
    nodemanagerHearbeat();
    clock.tick(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    Set<RMContainer> preemptedContainers =
        ((StubbedFairScheduler) scheduler).preemptedContainers;
    assertEquals("Container(s) should have been preempted", 25,
        preemptedContainers.size());
    int numPreemptFromQueueA1 = 0;
    int numPreemptFromQueueB = 0;
    for (RMContainer preemptedContainer : preemptedContainers) {
      if (applicationAttemptIdA1.equals(
          preemptedContainer.getApplicationAttemptId())) {
        ++numPreemptFromQueueA1;
      }
      if (applicationAttemptIdB.equals(
          preemptedContainer.getApplicationAttemptId())) {
        ++numPreemptFromQueueB;
      }
    }

    assertEquals("Should preempt from queueA.1", 5, numPreemptFromQueueA1);
    assertEquals("Should preempt from queueB", 20, numPreemptFromQueueB);
    clock.tick(2);
    scheduler.preemptTasksIfNecessary();

    scheduler.update();
    nodemanagerHearbeat();

    // verify final usage
    assertEquals(1024 * 25, scheduler.getQueueManager().getQueue("queueA.1")
        .getResourceUsage().getMemory());
    assertEquals(1024 * 25, scheduler.getQueueManager().getQueue("queueA.2")
        .getResourceUsage().getMemory());
    assertEquals(1024 * 50, scheduler.getQueueManager().getQueue("queueB")
        .getResourceUsage().getMemory());
  }

  @Test
  public void testPreemptionForDepth2Case4() throws Exception {
    // case: A2 should only preempt from A1 when parent queue A reached its max
    // limit, even when other siblings of queue A is over fair share
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<maxResources>0mb,0vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>1</weight>");
    out.println("<maxResources>20480mb,20vcores</maxResources>");
    out.println("  <queue name=\"1\">");
    out.println("  <weight>1</weight>");
    out.println("  </queue>");
    out.println("  <queue name=\"2\">");
    out.println("  <weight>1</weight>");
    out.println("  </queue>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>1</weight>");
    out.println("</queue>");
    out.println("<queue name=\"queueC\">");
    out.println("<weight>1</weight>");
    out.println("</queue>");
    out.println("<fairSharePreemptionTimeout>5</fairSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionThreshold>0.8</defaultFairSharePreemptionThreshold>");
    out.println("</allocations>");
    out.close();

    startResourceManager(0.7f);
    initClusterResource(100 * 1024, 100);

    ApplicationAttemptId applicationAttemptIdA1 =
        createSchedulingRequestAndSchedule(1024, "queueA.1", "user1", 20, 1);
    ApplicationAttemptId applicationAttemptIdB =
        createSchedulingRequestAndSchedule(1024, "queueB", "user1", 20, 1);
    ApplicationAttemptId applicationAttemptIdC =
        createSchedulingRequestAndSchedule(1024, "queueC", "user1", 60, 1);

    createSchedulingRequest(1024, "queueA.2", "user1", 50, 1);
    scheduler.update();
    nodemanagerHearbeat();
    clock.tick(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    Set<RMContainer> preemptedContainers =
        ((StubbedFairScheduler) scheduler).preemptedContainers;
    assertEquals("Container(s) should have been preempted", 10,
        preemptedContainers.size());
    for (RMContainer preemptedContainer : preemptedContainers) {
      assertEquals("Should only preempt from A1", applicationAttemptIdA1,
          preemptedContainer.getApplicationAttemptId());
    }

    clock.tick(2);
    scheduler.preemptTasksIfNecessary();

    scheduler.update();
    nodemanagerHearbeat();

    // verify final usage
    assertEquals(1024 * 10, scheduler.getQueueManager().getQueue("queueA.1")
        .getResourceUsage().getMemory());
    assertEquals(1024 * 10, scheduler.getQueueManager().getQueue("queueA.2")
        .getResourceUsage().getMemory());
    assertEquals(1024 * 20, scheduler.getQueueManager().getQueue("queueB")
        .getResourceUsage().getMemory());

    // queueC is over fair share, but will not be preempted
    assertEquals(1024 * 40, scheduler.getQueueManager().getQueue("queueC")
        .getFairShare().getMemory());
    assertEquals(1024 * 60, scheduler.getQueueManager().getQueue("queueC")
        .getResourceUsage().getMemory());
  }

  @Test
  public void testPreemptionForDepth2Case5() throws Exception {
    // case: starved A2 should only preempt from sibling A1 when parent A is
    // not starved
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<maxResources>0mb,0vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>1</weight>");
    out.println("  <queue name=\"1\">");
    out.println("  <weight>1</weight>");
    out.println("  </queue>");
    out.println("  <queue name=\"2\">");
    out.println("  <weight>1</weight>");
    out.println("  </queue>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>1</weight>");
    out.println("</queue>");
    out.println("<fairSharePreemptionTimeout>5</fairSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionThreshold>0.8</defaultFairSharePreemptionThreshold>");
    out.println("</allocations>");
    out.close();

    startResourceManager(0.8f);
    // Create node with 10GB memory and 10 vcores
    initClusterResource(10 * 1024, 10);

    ApplicationAttemptId applicationAttemptId =
        createSchedulingRequestAndSchedule(1024, "queueA.1", "user1", 4, 1);
    createSchedulingRequestAndSchedule(1024, "queueB", "user1", 6, 1);

    createSchedulingRequest(1024, "queueA.2", "user1", 1, 1);
    scheduler.update();
    nodemanagerHearbeat();
    clock.tick(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    Set<RMContainer> preemptedContainers =
        ((StubbedFairScheduler) scheduler).preemptedContainers;
    assertEquals("Container(s) should have been preempted", 1,
        preemptedContainers.size());
    for (RMContainer preemptedContainer : preemptedContainers) {
      assertEquals("Should only preempt from sibling", applicationAttemptId,
          preemptedContainer.getApplicationAttemptId());
    }
    clock.tick(2);
    scheduler.preemptTasksIfNecessary();

    scheduler.update();
    nodemanagerHearbeat();

    // verify final usage
    assertEquals(1024 * 3, scheduler.getQueueManager().getQueue("queueA.1")
        .getResourceUsage().getMemory());
    assertEquals(1024 * 1, scheduler.getQueueManager().getQueue("queueA.2")
        .getResourceUsage().getMemory());
    assertEquals(1024 * 6, scheduler.getQueueManager().getQueue("queueB")
        .getResourceUsage().getMemory());
  }
}
