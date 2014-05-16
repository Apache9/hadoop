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
package org.apache.hadoop.metrics.jvm;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsFloatValue;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;

import static java.lang.Thread.State.*;
import java.lang.management.GarbageCollectorMXBean;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Singleton class which reports Java Virtual Machine metrics to the metrics API.  
 * Any application can create an instance of this class in order to emit
 * Java VM metrics.  
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class JvmMetrics implements Updater {
    
    private static final float M = 1024*1024;
    private static JvmMetrics theInstance = null;
    private static Log log = LogFactory.getLog(JvmMetrics.class);
    
    private final MetricsRegistry registry = new MetricsRegistry();
    private MetricsRecord metrics;
    
    private final JvmStatistics jvmStatistics;
    
    // The metrics variables are private and only read by JMX through registry.
    // garbage collection counters
    private MetricsLongValue gcCount =
        new MetricsLongValue("gcCount", registry);
    private MetricsLongValue gcTimeMillis =
        new MetricsLongValue("gcTimeMillis", registry);
    
    // logging event counters
    private MetricsLongValue fatalCount =
        new MetricsLongValue("fatalCount", registry);
    private MetricsLongValue errorCount =
        new MetricsLongValue("errorCount", registry);
    private MetricsLongValue warnCount =
        new MetricsLongValue("warnCount", registry);
    private MetricsLongValue infoCount =
        new MetricsLongValue("infoCount", registry);
    
    // memory usage counters
    private MetricsFloatValue memNonHeapUsedM =
        new MetricsFloatValue("memNonHeapUsedM", registry);
    private MetricsFloatValue memNonHeapCommittedM =
        new MetricsFloatValue("memNonHeapCommittedM", registry);
    private MetricsFloatValue memHeapUsedM =
        new MetricsFloatValue("memHeapUsedM", registry);
    private MetricsFloatValue memHeapCommittedM =
        new MetricsFloatValue("memHeapCommittedM", registry);
    private MetricsFloatValue maxMemoryM =
        new MetricsFloatValue("maxMemoryM", registry);
    
    // thread counters
    private MetricsIntValue threadsNew =
        new MetricsIntValue("threadsNew", registry);
    private MetricsIntValue threadsRunnable =
        new MetricsIntValue("threadsRunnable", registry);
    private MetricsIntValue threadsBlocked =
        new MetricsIntValue("threadsBlocked", registry);
    private MetricsIntValue threadsWaiting =
        new MetricsIntValue("threadsWaiting", registry);
    private MetricsIntValue threadsTimedWaiting =
        new MetricsIntValue("threadsTimedWaiting", registry);
    private MetricsIntValue threadsTerminated =
        new MetricsIntValue("threadsTerminated", registry);
   
    
    public synchronized static JvmMetrics init(String processName, String sessionId) {
      return init(processName, sessionId, "metrics");
    }
    
    public synchronized static JvmMetrics init(String processName, String sessionId,
      String recordName) {
        if (theInstance != null) {
            log.info("Cannot initialize JVM Metrics with processName=" + 
                     processName + ", sessionId=" + sessionId + 
                     " - already initialized");
        }
        else {
            log.info("Initializing JVM Metrics with processName=" 
                    + processName + ", sessionId=" + sessionId);
            theInstance = new JvmMetrics(processName, sessionId, recordName);
        }
        return theInstance;
    }
    
    /** Creates a new instance of JvmMetrics */
    private JvmMetrics(String processName, String sessionId,
      String recordName) {
        MetricsContext context = MetricsUtil.getContext("jvm");
        metrics = MetricsUtil.createRecord(context, recordName);
        metrics.setTag("processName", processName);
        metrics.setTag("sessionId", sessionId);
        context.registerUpdater(this);
        
        jvmStatistics = new JvmStatistics(this.registry, processName);
    }
    
    /**
     * This will be called periodically (with the period being configuration
     * dependent).
     */
    public void doUpdates(MetricsContext context) {
        doMemoryUpdates();
        doGarbageCollectionUpdates();
        doThreadUpdates();
        doEventCountUpdates();

        // Both getMetricsList() and pushMetric() are thread-safe
        for (MetricsBase m : registry.getMetricsList()) {
          m.pushMetric(metrics);
        }
        metrics.update();
    }
    
    private void doMemoryUpdates() {
        MemoryMXBean memoryMXBean =
               ManagementFactory.getMemoryMXBean();
        MemoryUsage memNonHeap =
                memoryMXBean.getNonHeapMemoryUsage();
        MemoryUsage memHeap =
                memoryMXBean.getHeapMemoryUsage();
        Runtime runtime = Runtime.getRuntime();

        memNonHeapUsedM.set(memNonHeap.getUsed()/M);
        memNonHeapCommittedM.set(memNonHeap.getCommitted()/M);
        memHeapUsedM.set(memHeap.getUsed()/M);
        memHeapCommittedM.set(memHeap.getCommitted()/M);
        maxMemoryM.set(runtime.maxMemory()/M);
    }
    
    private void doGarbageCollectionUpdates() {
        List<GarbageCollectorMXBean> gcBeans =
                ManagementFactory.getGarbageCollectorMXBeans();
        long count = 0;
        long timeMillis = 0;
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            count += gcBean.getCollectionCount();
            timeMillis += gcBean.getCollectionTime();
        }
        gcCount.set(count);
        gcTimeMillis.set(timeMillis);
    }
    
    private void doThreadUpdates() {
        ThreadMXBean threadMXBean =
                ManagementFactory.getThreadMXBean();
        long threadIds[] = 
                threadMXBean.getAllThreadIds();
        ThreadInfo[] threadInfos =
                threadMXBean.getThreadInfo(threadIds, 0);
        
        int threadsNew = 0;
        int threadsRunnable = 0;
        int threadsBlocked = 0;
        int threadsWaiting = 0;
        int threadsTimedWaiting = 0;
        int threadsTerminated = 0;
        
        for (ThreadInfo threadInfo : threadInfos) {
            // threadInfo is null if the thread is not alive or doesn't exist
            if (threadInfo == null) continue;
            Thread.State state = threadInfo.getThreadState();
            if (state == NEW) {
                threadsNew++;
            } 
            else if (state == RUNNABLE) {
                threadsRunnable++;
            }
            else if (state == BLOCKED) {
                threadsBlocked++;
            }
            else if (state == WAITING) {
                threadsWaiting++;
            } 
            else if (state == TIMED_WAITING) {
                threadsTimedWaiting++;
            }
            else if (state == TERMINATED) {
                threadsTerminated++;
            }
        }
        this.threadsNew.set(threadsNew);
        this.threadsRunnable.set(threadsRunnable);
        this.threadsBlocked.set(threadsBlocked);
        this.threadsWaiting.set(threadsWaiting);
        this.threadsTimedWaiting.set(threadsTimedWaiting);
        this.threadsTerminated.set(threadsTerminated);
    }
    
    private void doEventCountUpdates() {
        fatalCount.set(EventCounter.getFatal());
        errorCount.set(EventCounter.getError());
        warnCount.set(EventCounter.getWarn());
        infoCount.set(EventCounter.getInfo());
    }

    public void shutdown() {
      if (jvmStatistics != null)
        jvmStatistics.shutdown();
    }
}
