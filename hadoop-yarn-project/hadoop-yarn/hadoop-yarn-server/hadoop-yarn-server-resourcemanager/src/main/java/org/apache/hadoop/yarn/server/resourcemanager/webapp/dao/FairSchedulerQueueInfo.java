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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;


import java.util.ArrayList;
import java.util.Collection;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.util.resource.Resources;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlSeeAlso({FairSchedulerLeafQueueInfo.class})
public class FairSchedulerQueueInfo {
  private int maxApps;
  private ResourceType dominantResourceType;
  
  @XmlTransient
  private float fractionResourcesUsed;
  @XmlTransient
  private float fractionResourcesSteadyFairShare;
  @XmlTransient
  private float fractionResourcesFairShare;
  @XmlTransient
  private float fractionResourcesMinShare;
  @XmlTransient
  private float fractionResourcesMaxShare;
  
  private ResourceInfo minResources;
  private ResourceInfo maxResources;
  private ResourceInfo usedResources;
  private ResourceInfo steadyFairResources;
  private ResourceInfo fairResources;
  private ResourceInfo clusterResources;
  private ResourceInfo expectedFairShare;

  private String queueName;
  private String schedulingPolicy;
  
  private Collection<FairSchedulerQueueInfo> childQueues;

  public FairSchedulerQueueInfo() {
  }
  
  public FairSchedulerQueueInfo(FSQueue queue, FairScheduler scheduler) {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    
    queueName = queue.getName();
    schedulingPolicy = queue.getPolicy().getName();
    
    clusterResources = new ResourceInfo(scheduler.getClusterResource());
    
    usedResources = new ResourceInfo(queue.getResourceUsage());

    updateDominantResourceType();

    fractionResourcesUsed = resourceInfoRatio(usedResources, clusterResources);

    steadyFairResources = new ResourceInfo(queue.getSteadyFairShare());
    fairResources = new ResourceInfo(queue.getFairShare());
    minResources = new ResourceInfo(queue.getMinShare());
    maxResources = new ResourceInfo(queue.getMaxShare());
    maxResources = new ResourceInfo(
        Resources.componentwiseMin(queue.getMaxShare(),
            scheduler.getClusterResource()));
    expectedFairShare = new ResourceInfo();
    expectedFairShare.setMemory((int) queue.getWeights().getWeight(ResourceType.MEMORY));
    expectedFairShare.setvCores((int) queue.getWeights().getWeight(ResourceType.CPU));

    fractionResourcesSteadyFairShare = resourceInfoRatio(steadyFairResources, clusterResources);
    fractionResourcesFairShare = resourceInfoRatio(fairResources, clusterResources);
    fractionResourcesMinShare = resourceInfoRatio(minResources, clusterResources);
    fractionResourcesMaxShare = resourceInfoRatio(maxResources, clusterResources);
    
    maxApps = allocConf.getQueueMaxApps(queueName);
    
    Collection<FSQueue> children = queue.getChildQueues();
    childQueues = new ArrayList<FairSchedulerQueueInfo>();
    for (FSQueue child : children) {
      if (child instanceof FSLeafQueue) {
        childQueues.add(new FairSchedulerLeafQueueInfo((FSLeafQueue)child, scheduler));
      } else {
        childQueues.add(new FairSchedulerQueueInfo(child, scheduler));
      }
    }
  }

  private void updateDominantResourceType() {
    if (schedulingPolicy.equals(DominantResourceFairnessPolicy.NAME)
        && ((float)usedResources.getvCores() / clusterResources.getvCores()
            > (float)usedResources.getMemory() / clusterResources.getMemory())) {
      dominantResourceType = ResourceType.CPU;
    } else {
      dominantResourceType = ResourceType.MEMORY;
    }
  }

  private float resourceInfoRatio(ResourceInfo a, ResourceInfo b) {
    float ratio;
    if (dominantResourceType == ResourceType.CPU) {
      ratio = b.getvCores() == 0 ? 0 : (float)a.getvCores() / b.getvCores();
    } else {
      ratio = b.getMemory() == 0 ? 0 : (float)a.getMemory() / b.getMemory();
    }
    return Math.min(1, ratio);
  }

  /**
   * Returns the dominant resource type of used resources
   */
  public ResourceType getDominantResourceType() {
    return dominantResourceType;
  }

  /**
   * Returns the steady fair share as a fraction of the entire cluster capacity.
   */
  public float getSteadyFairShareResourcesFraction() {
    return fractionResourcesSteadyFairShare;
  }

  /**
   * Returns the fair share as a fraction of the entire cluster capacity.
   */
  public float getFairShareResourcesFraction() {
    return fractionResourcesFairShare;
  }

  /**
   * Returns the steady fair share of this queue in megabytes.
   */
  public ResourceInfo getSteadyFairShare() {
    return steadyFairResources;
  }

  /**
   * Returns the fair share of this queue in megabytes
   */
  public ResourceInfo getFairShare() {
    return fairResources;
  }

  public ResourceInfo getMinResources() {
    return minResources;
  }
  
  public ResourceInfo getMaxResources() {
    return maxResources;
  }

  public ResourceInfo getExpectedFairShare() {
    return expectedFairShare;
  }

  public int getMaxApplications() {
    return maxApps;
  }
  
  public String getQueueName() {
    return queueName;
  }
  
  public ResourceInfo getUsedResources() {
    return usedResources;
  }
  
  /**
   * Returns the queue's min share in as a fraction of the entire
   * cluster capacity.
   */
  public float getMinShareResourcesFraction() {
    return fractionResourcesMinShare;
  }
  
  /**
   * Returns the memory used by this queue as a fraction of the entire 
   * cluster capacity.
   */
  public float getUsedResourcesFraction() {
    return fractionResourcesUsed;
  }
  
  /**
   * Returns the capacity of this queue as a fraction of the entire cluster 
   * capacity.
   */
  public float getMaxResourcesFraction() {
    return fractionResourcesMaxShare;
  }
  
  /**
   * Returns the name of the scheduling policy used by this queue.
   */
  public String getSchedulingPolicy() {
    return schedulingPolicy;
  }
  
  public Collection<FairSchedulerQueueInfo> getChildQueues() {
    return childQueues;
  }
}
