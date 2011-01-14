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
package gridool.metrics.runtime;

import gridool.GridNodeMetrics;
import gridool.util.GridUtils;
import gridool.util.system.SystemUtils;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridNodeRuntimeMetrics implements GridNodeMetrics {
    private static final long serialVersionUID = 7300867188084839174L;

    private final AtomicReference<GridTaskMetricsCounter> taskMetrics;
    private final File databaseDir;

    public GridNodeRuntimeMetrics(@Nonnull AtomicReference<GridTaskMetricsCounter> taskMetrics) {
        this.taskMetrics = taskMetrics;
        this.databaseDir = GridUtils.getWorkDir(true);
    }

    // -------------------------------

    public long getLastUpdateTime() {
        return System.currentTimeMillis();
    }

    // -------------------------------
    // node information

    public int getAvailableProcessors() {
        return SystemUtils.availableProcessors();
    }

    public long getUptime() {
        return SystemUtils.getUptime();
    }

    // ---------------------
    // resource metrics

    public float getLastCpuLoad() {
        return (float) SystemUtils.getCpuLoadAverage(); // TODO java 1.5
    }

    public float getAverageCpuLoad() {
        throw new UnsupportedOperationException();
    }

    public float getMaximumCpuLoad() {
        throw new UnsupportedOperationException();
    }

    public double getLastIoWait() {
        return SystemUtils.getIoWait();
    }

    public double getAverageIoWait() {
        throw new UnsupportedOperationException();
    }

    public long getHeapMemoryFree() {
        return SystemUtils.getHeapFreeMemory();
    }

    public long getAvailableDiskSpace() {
        return databaseDir.getFreeSpace();
    }

    // ---------------------
    // task metrics

    public int getLastActiveTasks() {
        return taskMetrics.get().getCurrentActiveTasks();
    }

    public float getAverageActiveTasks() {
        throw new UnsupportedOperationException();
    }

    public int getMaximumActiveTasks() {
        throw new UnsupportedOperationException();
    }

    public int getLastPassiveTasks() {
        return taskMetrics.get().getCurrentPassiveTasks();
    }

    public float getAveragePassiveTasks() {
        throw new UnsupportedOperationException();
    }

    public int getMaximumPassiveTasks() {
        throw new UnsupportedOperationException();
    }

    public int getTotalCanceledTasks() {
        return taskMetrics.get().getCurrentCanceledTasks();
    }

    public int getTotalStealedTasks() {
        return taskMetrics.get().getCurrentStealedTasks();
    }

    public long getLastTaskExecutionTime() {
        return taskMetrics.get().getAverageExecutionTime();
    }

    public double getAverageTaskExecutionTime() {
        throw new UnsupportedOperationException();
    }

    public long getMaximumTaskExecutionTime() {
        return taskMetrics.get().getMaxExecutionTime();
    }

    public long getLastTaskWaitTime() {
        return taskMetrics.get().getAverageWaitTime();
    }

    public double getAverageTaskWaitTime() {
        throw new UnsupportedOperationException();
    }

    public long getMaximumTaskWaitTime() {
        return taskMetrics.get().getMaxWaitTime();
    }

}
