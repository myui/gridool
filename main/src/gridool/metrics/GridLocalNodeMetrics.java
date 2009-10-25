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

import gridool.mbean.GridNodeMetricsMBean;
import gridool.metrics.GridNodeMetricsProvider.MetricsSnapshot;
import gridool.metrics.runtime.GridNodeRuntimeMetrics;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class GridLocalNodeMetrics implements GridNodeMetricsMBean {
    private static final long serialVersionUID = -3355182377583817276L;

    private transient GridNodeRuntimeMetrics runtime;

    private float averageCpuLoad = -1f;
    private float maximumCpuLoad = -1f;
    private double averageIoWait = -1d;
    private float averageActiveTasks = -1f;
    private int maximumActiveTasks = -1;
    private float averagePassiveTasks = -1f;
    private int maximumPassiveTasks = -1;
    private int totalCanceledTasks = 0;
    private int totalStealedTasks = 0;   
    private double averageTaskExecutionTime = -1d;
    private long maximumTaskExecutionTime = -1L;
    private double averageTaskWaitTime = -1d;
    private long maximumTaskWaitTime = -1L;

    public GridLocalNodeMetrics(@Nonnull GridNodeRuntimeMetrics runtime) {
        this.runtime = runtime;
    }

    public synchronized GridNodeMetricsAdapter createSnapshot(@Nonnull MetricsSnapshot snapshot) {
        // runtime metrics
        int availableProcessors = runtime.getAvailableProcessors();
        long uptime = runtime.getUptime();
        long heapMemoryFree = runtime.getHeapMemoryFree();
        long availableDiskSpace = runtime.getAvailableDiskSpace();
        // last metrics
        long lastUpdateTime = snapshot.getLastUpdateTime();
        float lastCpuLoad = snapshot.getLastCpuLoad();
        double lastIoWait = snapshot.getLastIoWait();
        int lastActiveTasks = snapshot.getLastActiveTasks();
        int lastPassiveTasks = snapshot.getLastPassiveTasks();
        long lastTaskExecutionTime = snapshot.getLastTaskExecutionTime();
        long lastTaskWaitTime = snapshot.getLastTaskWaitTime();
        return new GridNodeMetricsAdapter(lastUpdateTime, availableProcessors, uptime, lastCpuLoad, averageCpuLoad, maximumCpuLoad, lastIoWait, averageIoWait, heapMemoryFree, availableDiskSpace, lastActiveTasks, averageActiveTasks, maximumActiveTasks, lastPassiveTasks, averagePassiveTasks, maximumPassiveTasks, totalCanceledTasks, totalStealedTasks, lastTaskExecutionTime, averageTaskExecutionTime, maximumTaskExecutionTime, lastTaskWaitTime, averageTaskWaitTime, maximumTaskWaitTime);
    }

    GridNodeRuntimeMetrics getRuntimeMetrics() {
        return runtime;
    }

    // ------------------------------------------------

    public float getAverageCpuLoad() {
        return averageCpuLoad;
    }

    public void setAverageCpuLoad(float averageCpuLoad) {
        this.averageCpuLoad = averageCpuLoad;
    }

    public float getMaximumCpuLoad() {
        return maximumCpuLoad;
    }

    public void setMaximumCpuLoad(float maximumCpuLoad) {
        this.maximumCpuLoad = maximumCpuLoad;
    }

    public double getAverageIoWait() {
        return averageIoWait;
    }

    public void setAverageIoWait(double averageIoWait) {
        this.averageIoWait = averageIoWait;
    }

    public float getAverageActiveTasks() {
        return averageActiveTasks;
    }

    public void setAverageActiveTasks(float averageActiveTasks) {
        this.averageActiveTasks = averageActiveTasks;
    }

    public int getMaximumActiveTasks() {
        return maximumActiveTasks;
    }

    public void setMaximumActiveTasks(int maximumActiveTasks) {
        this.maximumActiveTasks = maximumActiveTasks;
    }

    public float getAveragePassiveTasks() {
        return averagePassiveTasks;
    }

    public void setAveragePassiveTasks(float averagePassiveTasks) {
        this.averagePassiveTasks = averagePassiveTasks;
    }

    public int getMaximumPassiveTasks() {
        return maximumPassiveTasks;
    }

    public void setMaximumPassiveTasks(int maximumPassiveTasks) {
        this.maximumPassiveTasks = maximumPassiveTasks;
    }

    public int getTotalCanceledTasks() {
        return totalCanceledTasks;
    }

    public void setTotalCanceledTasks(int totalCanceledTasks) {
        this.totalCanceledTasks = totalCanceledTasks;
    }

    public int getTotalStealedTasks() {
        return totalStealedTasks;
    }

    public void setTotalStealedTasks(int totalStealedTasks) {
        this.totalStealedTasks = totalStealedTasks;
    }

    public double getAverageTaskExecutionTime() {
        return averageTaskExecutionTime;
    }

    public void setAverageTaskExecutionTime(double averageTaskExecutionTime) {
        this.averageTaskExecutionTime = averageTaskExecutionTime;
    }

    public long getMaximumTaskExecutionTime() {
        return maximumTaskExecutionTime;
    }

    public void setMaximumTaskExecutionTime(long maximumTaskExecutionTime) {
        this.maximumTaskExecutionTime = maximumTaskExecutionTime;
    }

    public double getAverageTaskWaitTime() {
        return averageTaskWaitTime;
    }

    public void setAverageTaskWaitTime(double averageTaskWaitTime) {
        this.averageTaskWaitTime = averageTaskWaitTime;
    }

    public long getMaximumTaskWaitTime() {
        return maximumTaskWaitTime;
    }

    public void setMaximumTaskWaitTime(long maximumTaskWaitTime) {
        this.maximumTaskWaitTime = maximumTaskWaitTime;
    }

    // ------------------------------------------------
    // delegated methods of GridNodeMetricSnapshot

    public long getLastUpdateTime() {
        return runtime.getLastUpdateTime();
    }

    public int getAvailableProcessors() {
        return runtime.getAvailableProcessors();
    }

    public long getUptime() {
        return runtime.getUptime();
    }

    public float getLastCpuLoad() {
        return runtime.getLastCpuLoad();
    }

    public double getLastIoWait() {
        return runtime.getLastIoWait();
    }

    public long getHeapMemoryFree() {
        return runtime.getHeapMemoryFree();
    }

    public long getAvailableDiskSpace() {
        return runtime.getAvailableDiskSpace();
    }

    public int getLastActiveTasks() {
        return runtime.getLastActiveTasks();
    }

    public int getLastPassiveTasks() {
        return runtime.getLastPassiveTasks();
    }

    public long getLastTaskExecutionTime() {
        return runtime.getLastTaskExecutionTime();
    }

    public long getLastTaskWaitTime() {
        return runtime.getLastTaskWaitTime();
    }
}
