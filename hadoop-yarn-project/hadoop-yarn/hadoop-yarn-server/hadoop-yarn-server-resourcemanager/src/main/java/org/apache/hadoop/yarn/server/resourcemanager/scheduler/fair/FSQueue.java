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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.util.resource.Resources;

@Private
@Unstable
public abstract class FSQueue implements Queue, Schedulable {
  private static final Log LOG = LogFactory.getLog(
      FSQueue.class.getName());
  private Resource fairShare = Resources.createResource(0, 0);
  private Resource steadyFairShare = Resources.createResource(0, 0);
  protected ResourceWeights weight = ResourceWeights.NEUTRAL;

  private final String name;
  protected final FairScheduler scheduler;
  private final FSQueueMetrics metrics;
  
  protected final FSParentQueue parent;
  protected final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  
  protected SchedulingPolicy policy = SchedulingPolicy.DEFAULT_POLICY;

  private long fairSharePreemptionTimeout = Long.MAX_VALUE;
  private long minSharePreemptionTimeout = Long.MAX_VALUE;
  private float fairSharePreemptionThreshold = 0.5f;

  protected Resource preemptionRequestFromChildren = Resources.createResource(0, 0);
  protected Resource resourceToPreemptBetweenChildren = Resources.createResource(0, 0);

  protected List<RMContainer> warnedContainers = new ArrayList<RMContainer>();

  protected volatile Resource usage = Resources.createResource(0);
  protected volatile ReentrantLock usageUpdateLock = new ReentrantLock();

  public FSQueue(String name, FairScheduler scheduler, FSParentQueue parent) {
    this.name = name;
    this.scheduler = scheduler;
    this.metrics = FSQueueMetrics.forQueue(getName(), parent, true, scheduler.getConf());
    metrics.setMinShare(getMinShare());
    metrics.setMaxShare(getMaxShare());
    this.parent = parent;
  }
  public String getName() {
    return name;
  }
  
  @Override
  public String getQueueName() {
    return name;
  }
  
  public SchedulingPolicy getPolicy() {
    return policy;
  }
  
  public FSParentQueue getParent() {
    return parent;
  }

  protected void throwPolicyDoesnotApplyException(SchedulingPolicy policy)
      throws AllocationConfigurationException {
    throw new AllocationConfigurationException("SchedulingPolicy " + policy
        + " does not apply to queue " + getName());
  }

  public abstract void setPolicy(SchedulingPolicy policy)
      throws AllocationConfigurationException;

  /**
   * Preempt resource downside
   */
  public abstract void preemptResource();

  @Override
  public Resource getMinShare() {
    return scheduler.getAllocationConfiguration().getMinResources(getName());
  }
  
  @Override
  public Resource getMaxShare() {
    return scheduler.getAllocationConfiguration().getMaxResources(getName());
  }

  @Override
  public long getStartTime() {
    return 0;
  }

  @Override
  public Priority getPriority() {
    Priority p = recordFactory.newRecordInstance(Priority.class);
    p.setPriority(1);
    return p;
  }
  
  @Override
  public QueueInfo getQueueInfo(boolean includeChildQueues, boolean recursive) {
    QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    queueInfo.setQueueName(getQueueName());

    if (scheduler.getClusterResource().getMemory() == 0) {
      queueInfo.setCapacity(0.0f);
    } else {
      queueInfo.setCapacity((float) getFairShare().getMemory() /
          scheduler.getClusterResource().getMemory());
    }

    if (getFairShare().getMemory() == 0) {
      queueInfo.setCurrentCapacity(0.0f);
    } else {
      queueInfo.setCurrentCapacity((float) getResourceUsage().getMemory() /
          getFairShare().getMemory());
    }

    ArrayList<QueueInfo> childQueueInfos = new ArrayList<QueueInfo>();
    if (includeChildQueues) {
      Collection<FSQueue> childQueues = getChildQueues();
      for (FSQueue child : childQueues) {
        childQueueInfos.add(child.getQueueInfo(recursive, recursive));
      }
    }
    queueInfo.setChildQueues(childQueueInfos);
    queueInfo.setQueueState(QueueState.RUNNING);

    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    AccessControlList submitAcls = allocConf.getQueueAcl(getQueueName(), QueueACL.SUBMIT_APPLICATIONS);
    queueInfo.setSubmitAcls(submitAcls.getAclString());

    AccessControlList adminAcls = allocConf.getQueueAcl(getQueueName(), QueueACL.ADMINISTER_QUEUE);
    queueInfo.setAdminAcls(adminAcls.getAclString());
    return queueInfo;
  }

  @Override
  public FSQueueMetrics getMetrics() {
    return metrics;
  }

  /** Get the fair share assigned to this Schedulable. */
  public Resource getFairShare() {
    return fairShare;
  }

  @Override
  public void setFairShare(Resource fairShare) {
    this.fairShare = fairShare;
    metrics.setFairShare(fairShare);
  }

  /** Get the steady fair share assigned to this Schedulable. */
  public Resource getSteadyFairShare() {
    return steadyFairShare;
  }

  public void setSteadyFairShare(Resource steadyFairShare) {
    this.steadyFairShare = steadyFairShare;
    metrics.setSteadyFairShare(steadyFairShare);
  }

  public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
    return scheduler.getAllocationConfiguration().hasAccess(name, acl, user);
  }

  public long getFairSharePreemptionTimeout() {
    return fairSharePreemptionTimeout;
  }

  public void setFairSharePreemptionTimeout(long fairSharePreemptionTimeout) {
    this.fairSharePreemptionTimeout = fairSharePreemptionTimeout;
  }

  public long getMinSharePreemptionTimeout() {
    return minSharePreemptionTimeout;
  }

  public void setMinSharePreemptionTimeout(long minSharePreemptionTimeout) {
    this.minSharePreemptionTimeout = minSharePreemptionTimeout;
  }

  public float getFairSharePreemptionThreshold() {
    return fairSharePreemptionThreshold;
  }

  public void setFairSharePreemptionThreshold(float fairSharePreemptionThreshold) {
    this.fairSharePreemptionThreshold = fairSharePreemptionThreshold;
  }

  /**
   * Update the expected shares for all child queues
   */
  public abstract void updateExpectedFairShares();

  @Override
  public ResourceWeights getWeights() {
    return weight;
  }

  /**
   * Recomputes the shares for all child queues and applications based on this
   * queue's current share
   */
  public abstract void recomputeShares();

  /**
   * Update the min/fair share preemption timeouts and threshold for this queue.
   */
  public void updatePreemptionVariables() {
    // For min share timeout
    minSharePreemptionTimeout = scheduler.getAllocationConfiguration()
        .getMinSharePreemptionTimeout(getName());
    if (minSharePreemptionTimeout == -1 && parent != null) {
      minSharePreemptionTimeout = parent.getMinSharePreemptionTimeout();
    }
    // For fair share timeout
    fairSharePreemptionTimeout = scheduler.getAllocationConfiguration()
        .getFairSharePreemptionTimeout(getName());
    if (fairSharePreemptionTimeout == -1 && parent != null) {
      fairSharePreemptionTimeout = parent.getFairSharePreemptionTimeout();
    }
    // For fair share preemption threshold
    fairSharePreemptionThreshold = scheduler.getAllocationConfiguration()
        .getFairSharePreemptionThreshold(getName());
    if (fairSharePreemptionThreshold < 0 && parent != null) {
      fairSharePreemptionThreshold = parent.getFairSharePreemptionThreshold();
    }
  }

  /**
   * Gets the children of this queue, if any.
   */
  public abstract List<FSQueue> getChildQueues();
  
  /**
   * Adds all applications in the queue and its subqueues to the given collection.
   * @param apps the collection to add the applications to
   */
  public abstract void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps);
  
  /**
   * Return the number of apps for which containers can be allocated.
   * Includes apps in subqueues.
   */
  public abstract int getNumRunnableApps();

  /**
   * Return the number of apps for which waiting for scheduling.
   * Includes apps in subqueues.
   */
  public abstract int getNumPendingApps();

  /**
   * Helper method to check if the queue should attempt assigning resources
   * 
   * @return true if check passes (can assign) or false otherwise
   */
  protected boolean assignContainerPreCheck(FSSchedulerNode node) {
    if (!Resources.fitsIn(getResourceUsage(),
        scheduler.getAllocationConfiguration().getMaxResources(getName()))
        || node.getReservedContainer() != null) {
      return false;
    }
    return true;
  }

  /**
   * Returns true if queue has at least one app running.
   */
  public boolean isActive() {
    return getNumRunnableApps() > 0;
  }

  /** Convenient toString implementation for debugging. */
  @Override
  public String toString() {
    return String.format("[%s, demand=%s, running=%s, share=%s, w=%s]",
        getName(), getDemand(), getResourceUsage(), fairShare, getWeights());
  }
  
  @Override
  public Set<String> getAccessibleNodeLabels() {
    // TODO, add implementation for FS
    return null;
  }
  
  @Override
  public String getDefaultNodeLabelExpression() {
    // TODO, add implementation for FS
    return null;
  }

  public void updateResourceToPreempt(Resource addedResource) {
    if (getParent() == null || !isStarvedForFairShare()) {
      // for root queue, or when queue is not starved: only should preempt from
      // children
      Resources.addTo(resourceToPreemptBetweenChildren, addedResource);
    } else {
      // for non root queue, divide preemption request to two part: preemption
      // from children, and preemption from sibling
      Resource usageResource = getResourceUsage();

      Resource oldResourceAfterPreemption = Resources.add(
          preemptionRequestFromChildren, usageResource);
      Resources.addTo(preemptionRequestFromChildren, addedResource);
      Resource newResourceAfterPreemption = Resources.add(
          preemptionRequestFromChildren, usageResource);

      // request above fair share should be preempted from children
      resourceToPreemptBetweenChildren = Resources.subtract(
          newResourceAfterPreemption, getFairShare());

      // request below fair share should be preempted from sibling
      Resource newResourceToPreemptFromSibling =
          Resources.subtract(
              Resources.componentwiseMin(newResourceAfterPreemption,
                  getFairShare()),
              Resources.componentwiseMin(oldResourceAfterPreemption,
                  getFairShare())
          );

      // only update preemption request to parent if this current queue is starved
      parent.updateResourceToPreempt(newResourceToPreemptFromSibling);
    }

    if (Resources.greaterThan(scheduler.getResourceCalculator(), scheduler.getClusterResource(),
        resourceToPreemptBetweenChildren, Resources.none())) {
      LOG.info("update resource to preempt, queue: " + getName() + ", " +
          "preemption between children: " + resourceToPreemptBetweenChildren);
    }
  }

  public void clearPreemptedResources() {
    preemptionRequestFromChildren.setMemory(0);
    preemptionRequestFromChildren.setVirtualCores(0);
    resourceToPreemptBetweenChildren.setMemory(0);
    resourceToPreemptBetweenChildren.setVirtualCores(0);
  }

  /**
   * Is a queue being starved for its min share.
   */
  @VisibleForTesting
  boolean isStarvedForMinShare() {
    return isStarved(getMinShare());
  }

  /**
   * Is a queue being starved for its fair share threshold.
   */
  @VisibleForTesting
  boolean isStarvedForFairShare() {
    return isStarved(
        Resources.multiply(getFairShare(), getFairSharePreemptionThreshold()));
  }

  private boolean isStarved(Resource share) {
    Resource desiredShare = Resources.min(scheduler.getResourceCalculator(),
        scheduler.getClusterResource(), share, getDemand());
    return Resources.lessThan(scheduler.getResourceCalculator(),
        scheduler.getClusterResource(), getResourceUsage(), desiredShare);
  }

  protected void preemptResourceBetweenChildren() {
    // warn or kill containers that has already been chosen to preempt
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to preempt resource under queue "
          + getName() + " for resource " + resourceToPreemptBetweenChildren);
    }

    long start = System.currentTimeMillis();
    Iterator<RMContainer> warnedIter = warnedContainers.iterator();
    Resource toPreempt = Resources.clone(resourceToPreemptBetweenChildren);
    while (warnedIter.hasNext()) {
      RMContainer container = warnedIter.next();
      if ((container.getState() == RMContainerState.RUNNING ||
          container.getState() == RMContainerState.ALLOCATED) &&
          Resources.greaterThan(scheduler.getResourceCalculator(),
              scheduler.getClusterResource(),
              toPreempt, Resources.none())) {
        scheduler.warnOrKillContainer(container);
        Resources
            .subtractFrom(toPreempt, container.getContainer().getResource());
      } else {
        // container finished or preemption request is gone
        warnedIter.remove();
        // remove container from its original application's preempted resource
        scheduler.removePreemption(container);
      }
    }

    int maxContainerToPreempt = scheduler.getConf().getMaxContainersPerPreemption();
    int preemptedContainers = 0;
    Resource totalToPreempt = Resources.clone(toPreempt);
    // preempt from children for remaining preemption request
    while (Resources.greaterThan(scheduler.getResourceCalculator(),
        scheduler.getClusterResource(),
        toPreempt, Resources.none()) && preemptedContainers < maxContainerToPreempt) {
      RMContainer container = preemptContainer();
      if (container == null) {
        LOG.warn("Resource: " + toPreempt + " can't be preempted between children on queue: " + getQueueName());
        break;
      } else {
        preemptedContainers++;
        scheduler.warnOrKillContainer(container);
        warnedContainers.add(container); // mark container on this queue
        Resources.subtractFrom(
            toPreempt, container.getContainer().getResource());
        LOG.info("Succeeded preempt resource under queue " + getName()
            + " from container: " + container);
      }
    }

    long cost = (System.currentTimeMillis() - start) / 1000;
    if (cost > 5) {
      LOG.warn("Preempt resource: " + totalToPreempt + " between " + getQueueName()
              + "'s children tasks costs too long: " + cost + " s"
              + ", preempted containers: " + preemptedContainers
              + "with max containers to preempt: " + maxContainerToPreempt);
    }
  }

  public boolean fitsInMaxShare(Resource additionalResource) {
    Resource usagePlusAddition =
        Resources.add(getResourceUsage(), additionalResource);

    if (!Resources.fitsIn(usagePlusAddition, getMaxShare())) {
      return false;
    }

    FSQueue parentQueue = getParent();
    if (parentQueue != null) {
      return parentQueue.fitsInMaxShare(additionalResource);
    }
    return true;
  }

  public void updateUsage(Resource updatedUsage) {
    usageUpdateLock.lock();
    try {
      usage = updatedUsage;
    } finally {
      usageUpdateLock.unlock();
    }
  }

  public void addUsage(Resource assigned) {
    usageUpdateLock.lock();
    try {
      Resources.addTo(usage, assigned);
    } finally {
      usageUpdateLock.unlock();
    }
  }

  /**
   * Update the resource usage of the queue
   */
  public abstract void updateResourceUsage();
}
