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
package gridool.monitor;

import gridool.GridExecutionListener;
import gridool.GridJob;
import gridool.GridNodeMetrics;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.metrics.GridLocalNodeMetrics;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 * @see GridLocalNodeMetrics
 */
public final class GridExecutionMonitor implements GridExecutionListener {

    private final Queue<GridExecutionListener> listeners;
    private final GridNodeMetrics metrics;

    public GridExecutionMonitor(@Nonnull GridResourceRegistry registry) {
        this.listeners = new ConcurrentLinkedQueue<GridExecutionListener>();
        this.metrics = registry.getLocalNodeMetrics();
        assert (metrics != null);
    }

    public void addListener(@Nonnull GridExecutionListener listener) {
        listeners.add(listener);
    }

    public void removeListener(@Nonnull GridExecutionListener listener) {
        listeners.remove(listener);
    }

    public void onJobStarted(GridJob<?, ?> job) {
        for(GridExecutionListener listenr : listeners) {
            listenr.onJobStarted(job);
        }
    }

    public void onJobFinished(GridJob<?, ?> job) {
        for(GridExecutionListener listenr : listeners) {
            listenr.onJobFinished(job);
        }
    }

    public void onTaskStarted(GridTask task) {
        for(GridExecutionListener listenr : listeners) {
            listenr.onTaskStarted(task);
        }
    }

    public void onTaskFinished(GridTask task) {
        for(GridExecutionListener listenr : listeners) {
            listenr.onTaskFinished(task);
        }
    }

    public void onTaskStealed(GridTask task) {
        for(GridExecutionListener listenr : listeners) {
            listenr.onTaskStealed(task);
        }
    }

    public void onTaskCanceled(GridTask task) {
        for(GridExecutionListener listenr : listeners) {
            listenr.onTaskCanceled(task);
        }
    }

    public void progress(GridTask task, float progress) {
        if(progress == -1f) {
            final double avgExecTime = metrics.getAverageTaskExecutionTime();
            if(avgExecTime != 0) {
                long curTime = System.currentTimeMillis();
                long startTime = task.getStartedTime();
                long elapsed = curTime - startTime;
                if(elapsed >= avgExecTime) {
                    progress = 0.99f;
                } else {
                    progress = (float) (elapsed / avgExecTime);
                }
            }
        }
        for(GridExecutionListener listenr : listeners) {
            listenr.progress(task, progress);
        }
    }

}
