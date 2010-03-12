/*
 * @(#)$Id$
 *
 * Copyright 2006-2008 Makoto YUI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Contributors:
 *     Makoto YUI - initial implementation
 */
package gridool.metrics;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridNodeMetrics;
import gridool.GridResourceRegistry;
import gridool.mbean.GridNodeMetricsMBean;
import gridool.metrics.runtime.GridNodeRuntimeMetrics;
import gridool.metrics.runtime.GridTaskMetricsCounter;
import gridool.util.GridUtils;

import java.lang.management.ManagementFactory;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.collections.RingBuffer;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridNodeMetricsProvider {
    private static final Log LOG = LogFactory.getLog(GridNodeMetricsProvider.class);

    private final AtomicReference<GridTaskMetricsCounter> taskMetricsRef;
    private final int collectInterval;
    private final int metricsHistories;

    private final GridLocalNodeMetrics localMetrics;

    @Nullable
    private MetricsCollector metricsCollector;
    @Nullable
    private Timer timer;

    public GridNodeMetricsProvider(@Nonnull GridResourceRegistry registry, @Nonnull GridConfiguration config) {
        this.taskMetricsRef = registry.getTaskMetricsCounter();
        this.collectInterval = config.getMetricsCollectInterval();
        this.metricsHistories = config.getMetricsHistorySize();

        GridNodeRuntimeMetrics runtimeMetrics = new GridNodeRuntimeMetrics(taskMetricsRef);
        this.localMetrics = new GridLocalNodeMetrics(runtimeMetrics);
        registry.setLocalNodeMetrics(localMetrics);
    }

    public GridNodeMetrics getMetrics() {
        GridNodeMetrics capturedMetrics = metricsCollector.captureMetrics();

        // updates counter
        GridTaskMetricsCounter newTaskMetrics = new GridTaskMetricsCounter();
        taskMetricsRef.set(newTaskMetrics);

        if(LOG.isDebugEnabled()) {
            LOG.debug(capturedMetrics);
        }
        return capturedMetrics;
    }

    void start() throws GridException {
        registerMBeans(localMetrics);

        MetricsCollector collectorTask = new MetricsCollector(localMetrics, metricsHistories);
        Timer timer = new Timer("gridool#MetricsCollector", true);
        timer.scheduleAtFixedRate(collectorTask, 1000, collectInterval);
        this.metricsCollector = collectorTask;
        this.timer = timer;
    }

    void stop() throws GridException {
        if(timer != null) {
            timer.cancel();
            this.timer = null;
            unregisterMBeans();
        }
    }

    private static void registerMBeans(GridLocalNodeMetrics metrics) {
        final StandardMBean mbean;
        try {
            mbean = new StandardMBean(metrics, GridNodeMetricsMBean.class);
        } catch (NotCompliantMBeanException e) {
            LOG.error("Unexpected as GridNodeMetricsMBean: " + metrics.getClass().getName(), e);
            return;
        }
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName name = GridUtils.makeMBeanName("gridool", "GridService", "GridLocalNodeMetrics");
        try {
            server.registerMBean(mbean, name);
        } catch (Exception e) {
            LOG.error("Failed registering mbean: " + name, e);
        }
    }

    private static void unregisterMBeans() {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName name = GridUtils.makeMBeanName("gridool", "GridService", "GridLocalNodeMetrics");
        try {
            server.unregisterMBean(name);
        } catch (InstanceNotFoundException inf) {
            LOG.warn("Failed unregistering mbean: " + name);
        } catch (MBeanRegistrationException re) {
            LOG.warn("Failed unregistering mbean: " + name);
        }
    }

    private static final class MetricsCollector extends TimerTask {

        private final GridLocalNodeMetrics metrics;
        private final RingBuffer<MetricsSnapshot> history;

        private final Object lock = new Object();

        MetricsCollector(GridLocalNodeMetrics metics, int historySize) {
            super();
            this.metrics = metics;
            this.history = new RingBuffer<MetricsSnapshot>(historySize);
        }

        @Override
        public void run() {
            addSnapshot();
        }

        private void addSnapshot() {
            final MetricsSnapshot snapshot = new MetricsSnapshot(metrics);
            synchronized(lock) {
                history.insert(snapshot);
            }
        }

        GridNodeMetricsAdapter captureMetrics() {
            final MetricsSnapshot snapshot = new MetricsSnapshot(metrics);
            int hsize = 0;
            float totalCpuLoad = 0f, totalActiveTasks = 0f, totalPassiveTasks = 0f;
            int totalCanceledTasks = 0, totalStealedTasks = 0;
            double totalIoWait = 0d, totalTaskExecutionTime = 0d, totalTaskWaitTime = 0d;
            float maximumCpuLoad = 0f;
            int maximumActiveTasks = 0, maximumPassiveTasks = 0;
            long maximumTaskExecutionTime = 0L, maximumTaskWaitTime = 0L;
            synchronized(lock) {
                history.insert(snapshot);
                for(MetricsSnapshot m : history) {
                    final float cpuLoad = m.lastCpuLoad;
                    final int activeTasks = m.lastActiveTasks;
                    final int passiveTasks = m.lastPassiveTasks;
                    final long taskExecutionTime = m.lastTaskExecutionTime;
                    final long taskWaitTime = m.lastTaskWaitTime;
                    totalCpuLoad += cpuLoad;
                    totalIoWait += m.lastIoWait;
                    totalActiveTasks += activeTasks;
                    totalPassiveTasks += passiveTasks;
                    totalCanceledTasks += m.lastCanceledTasks;
                    totalStealedTasks += m.lastStealedTasks;
                    totalTaskExecutionTime += taskExecutionTime;
                    totalTaskWaitTime += taskWaitTime;
                    if(cpuLoad > maximumCpuLoad) {
                        maximumCpuLoad = cpuLoad;
                    }
                    if(activeTasks > maximumActiveTasks) {
                        maximumActiveTasks = activeTasks;
                    }
                    if(passiveTasks > maximumPassiveTasks) {
                        maximumPassiveTasks = passiveTasks;
                    }
                    if(taskExecutionTime > maximumTaskExecutionTime) {
                        maximumTaskExecutionTime = taskExecutionTime;
                    }
                    if(taskWaitTime > maximumTaskWaitTime) {
                        maximumTaskWaitTime = taskWaitTime;
                    }
                    hsize++;
                }
            }
            if(LOG.isDebugEnabled()) {
                LOG.debug("Metrics history size: " + hsize);
            }
            if(hsize > 0) {
                metrics.setAverageCpuLoad(totalCpuLoad / hsize);
                metrics.setAverageIoWait(totalIoWait / hsize);
                metrics.setAverageActiveTasks(totalActiveTasks / hsize);
                metrics.setAveragePassiveTasks(totalPassiveTasks / hsize);
                metrics.setAverageTaskExecutionTime(totalTaskExecutionTime / hsize);
                metrics.setAverageTaskWaitTime(totalTaskWaitTime / hsize);
                metrics.setMaximumCpuLoad(maximumCpuLoad);
                metrics.setMaximumActiveTasks(maximumActiveTasks);
                metrics.setMaximumPassiveTasks(maximumPassiveTasks);
                metrics.setMaximumTaskExecutionTime(maximumTaskExecutionTime);
                metrics.setMaximumTaskWaitTime(maximumTaskWaitTime);
                metrics.setTotalCanceledTasks(totalCanceledTasks);
                metrics.setTotalStealedTasks(totalStealedTasks);
            } else {
                LOG.warn("Encountered an illegal situation of no metrics history");
            }
            return metrics.createSnapshot(snapshot);
        }

    }

    static final class MetricsSnapshot {

        private final long lastUpdateTime;
        private final float lastCpuLoad;
        private final double lastIoWait;
        private final int lastActiveTasks;
        private final int lastPassiveTasks;
        private final int lastCanceledTasks;
        private final int lastStealedTasks;
        private final long lastTaskExecutionTime;
        private final long lastTaskWaitTime;

        MetricsSnapshot(final GridLocalNodeMetrics metrics) {
            this.lastUpdateTime = System.currentTimeMillis();
            this.lastCpuLoad = metrics.getLastCpuLoad();
            this.lastIoWait = metrics.getLastIoWait();
            this.lastActiveTasks = metrics.getLastActiveTasks();
            this.lastPassiveTasks = metrics.getLastPassiveTasks();
            GridNodeRuntimeMetrics runtime = metrics.getRuntimeMetrics();
            this.lastCanceledTasks = runtime.getTotalCanceledTasks();
            this.lastStealedTasks = runtime.getTotalStealedTasks();
            this.lastTaskExecutionTime = metrics.getLastTaskExecutionTime();
            this.lastTaskWaitTime = metrics.getLastTaskWaitTime();
        }

        long getLastUpdateTime() {
            return lastUpdateTime;
        }

        float getLastCpuLoad() {
            return lastCpuLoad;
        }

        double getLastIoWait() {
            return lastIoWait;
        }

        int getLastActiveTasks() {
            return lastActiveTasks;
        }

        int getLastPassiveTasks() {
            return lastPassiveTasks;
        }

        long getLastTaskExecutionTime() {
            return lastTaskExecutionTime;
        }

        long getLastTaskWaitTime() {
            return lastTaskWaitTime;
        }

    }
}
