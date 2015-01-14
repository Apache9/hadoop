package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies;

import java.util.Comparator;

import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSAppAttempt;

public class FSPreemptionPolicy {
  private AppComparator appComparator = new AppComparator();
  private ContainerComparator containerComparator = new ContainerComparator();

  private static class AppComparator implements Comparator<FSAppAttempt> {
    /**
     * bigger for preemption candidate
     */
    @Override
    public int compare(FSAppAttempt app1, FSAppAttempt app2) {
      int res = (int) Math.signum(app1.getStartTime() - app2.getStartTime());
      if (res == 0) {
        res = app1.getName().compareTo(app2.getName());
      }
      return res;
    }
  }

  private static class ContainerComparator implements Comparator<RMContainer> {
    /**
     * smaller for preemption candidate
     */
    @Override
    public int compare(RMContainer container1, RMContainer container2) {
      return container2.getContainerId().compareTo(container1.getContainerId());
    }
  }

  public Comparator<FSAppAttempt> getAppComparator() {
    return appComparator;
  }

  public Comparator<RMContainer> getContainerComparator() {
    return containerComparator;
  }
}
