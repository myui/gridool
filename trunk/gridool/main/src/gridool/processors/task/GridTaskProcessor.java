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
package gridool.processors.task;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.annotation.GridAnnotationProcessor;
import gridool.loadblancing.workstealing.GridTaskStealingTask;
import gridool.metrics.runtime.GridTaskMetricsCounter;
import gridool.monitor.GridExecutionMonitor;
import gridool.processors.GridProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import xbird.util.concurrent.ExecutorFactory;
import xbird.util.concurrent.collections.NonblockingUnboundedDeque;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridTaskProcessor implements GridProcessor {

    private final GridAnnotationProcessor annotationProc;
    private final GridExecutionMonitor monitor;
    private final TaskResponseListener respListener;
    private final ExecutorService execPool;
    private final AtomicReference<GridTaskMetricsCounter> metricsRef;

    private final NonblockingUnboundedDeque<GridTask> waitingTaskQueue;

    public GridTaskProcessor(@Nonnull GridResourceRegistry resourceRegistry, @Nonnull TaskResponseListener respListener, @Nonnull GridExecutionMonitor monitor, @Nonnull GridConfiguration config) {
        this.annotationProc = new GridAnnotationProcessor(config, resourceRegistry);
        this.monitor = monitor;
        this.respListener = respListener;
        int poolSize = config.getTaskProcessorPoolSize();
        this.execPool = ExecutorFactory.newFixedThreadPool(poolSize, "GridTaskProcessor");
        this.metricsRef = resourceRegistry.getTaskMetricsCounter();
        this.waitingTaskQueue = new NonblockingUnboundedDeque<GridTask>(8); // 2^8=256
        resourceRegistry.setTaskProcessor(this);
    }

    public void stop(boolean cancel) throws GridException {
        if(cancel) {
            execPool.shutdownNow(); // TODO send a response: task canceled
        } else {
            execPool.shutdown();
        }
    }

    public void processTask(@Nonnull GridTask task) {
        if(task instanceof GridTaskStealingTask) {
            stealTasks((GridTaskStealingTask) task);
            return;
        }
        GridTaskMetricsCounter metrics = metricsRef.get();
        metrics.taskCreated();

        final GridTaskWorker worker;
        if(task.getRelocatability().isRelocatable()) {
            waitingTaskQueue.pushBottom(task);
            worker = new GridTaskWorker(waitingTaskQueue, metrics, monitor, annotationProc, respListener);
        } else {
            worker = new GridTaskWorker(task, metrics, monitor, annotationProc, respListener);
        }
        execPool.execute(worker);
    }

    private void stealTasks(GridTaskStealingTask stealTask) {
        final int maxSteals = stealTask.getNumberOfStealingTasks();
        final List<GridTask> stealedTasks = new ArrayList<GridTask>(maxSteals);
        for(int i = 0; i < maxSteals; i++) {
            final GridTask stealedTask = waitingTaskQueue.popBottom();
            if(stealedTask == null) {
                break;
            } else {
                stealedTasks.add(stealedTask);
                monitor.onTaskStealed(stealedTask);
            }
        }
        final GridTask[] tasks = new GridTask[stealedTasks.size()];
        stealedTasks.toArray(tasks);
        respListener.onResponse(stealTask, tasks);
    }

}
