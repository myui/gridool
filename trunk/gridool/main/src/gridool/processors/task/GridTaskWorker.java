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
import gridool.communication.payload.GridNodeInfo;
import gridool.metrics.runtime.GridTaskMetricsCounter;
import gridool.monitor.GridExecutionMonitor;
import gridool.replication.GridReplicationException;
import gridool.replication.ReplicationManager;
import gridool.util.GridUtils;

import java.io.Serializable;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.concurrent.collections.NonblockingUnboundedDeque;
import xbird.util.datetime.StopWatch;
import xbird.util.lang.ClassUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridTaskWorker implements Runnable {
    private static final Log LOG = LogFactory.getLog(GridTaskWorker.class);

    @Nullable
    private GridTask task;
    @Nullable
    private final NonblockingUnboundedDeque<GridTask> taskQueue;
    @Nonnull
    private final GridTaskMetricsCounter metrics;
    @Nonnull
    private final GridExecutionMonitor monitor;
    @Nonnull
    private final GridAnnotationProcessor annotationProc;
    @Nonnull
    private final TaskResponseListener respListener;
    @Nonnull
    private final GridResourceRegistry registry;
    @Nonnull
    private final ReplicationManager replicationMgr;
    @Nonnull
    private final GridNodeInfo localNode;

    private final long createTime;

    public GridTaskWorker(@CheckForNull GridTask task, @Nonnull GridTaskMetricsCounter metrics, @Nonnull GridExecutionMonitor monitor, @Nonnull GridAnnotationProcessor annotationProc, @Nonnull TaskResponseListener respListener) {
        if(task == null) {
            throw new IllegalArgumentException();
        }
        this.task = task;
        this.taskQueue = null;
        this.metrics = metrics;
        this.monitor = monitor;
        this.annotationProc = annotationProc;
        this.respListener = respListener;
        this.registry = annotationProc.getResourceRegistory(); // REVIEWME
        this.replicationMgr = registry.getReplicationManager();
        GridConfiguration config = annotationProc.getConfiguration();
        this.localNode = config.getLocalNode();
        this.createTime = System.currentTimeMillis();
    }

    public GridTaskWorker(@CheckForNull NonblockingUnboundedDeque<GridTask> taskQueue, @Nonnull GridTaskMetricsCounter metrics, @Nonnull GridExecutionMonitor monitor, @Nonnull GridAnnotationProcessor annotationProc, @Nonnull TaskResponseListener respListener) {
        if(taskQueue == null) {
            throw new IllegalArgumentException();
        }
        this.task = null;
        this.taskQueue = taskQueue;
        this.metrics = metrics;
        this.monitor = monitor;
        this.annotationProc = annotationProc;
        this.respListener = respListener;
        this.registry = annotationProc.getResourceRegistory(); // REVIEWME
        this.replicationMgr = registry.getReplicationManager();
        GridConfiguration config = annotationProc.getConfiguration();
        this.localNode = config.getLocalNode();
        this.createTime = System.currentTimeMillis();
    }

    public void run() {
        final long startTime = System.currentTimeMillis();
        long waitTime = startTime - createTime;
        if(task == null) {
            task = taskQueue.popTop();
            if(task == null) {
                metrics.taskStealed(waitTime);
                return;
            }
        }
        metrics.taskStarted(waitTime);

        final String taskClassName = ClassUtils.getSimpleClassName(task);
        if(LOG.isInfoEnabled()) {
            LOG.info(taskClassName + " [" + task.getTaskId() + "] is started");
        }

        // replication
        if(task.isReplicatable()) {
            if(!replicationMgr.replicateTask(task, localNode)) {
                if(LOG.isWarnEnabled()) {
                    LOG.warn("failed to replicate a task: " + taskClassName + " ["
                            + task.getTaskId() + ']');
                }
                respListener.onCaughtException(task, new GridReplicationException("Replication failed"));
                return;
            }
        }

        // dependency injection
        if(task.injectResources()) {
            try {
                annotationProc.injectResources(task);
            } catch (GridException e) {
                LOG.error(e.getMessage());
                respListener.onCaughtException(task, e);
                return;
            }
        }

        // TODO REVIEWME Is this enough?
        final ClassLoader origLdr = Thread.currentThread().getContextClassLoader();
        final ClassLoader newLdr = GridUtils.getTaskClassLoader(task, origLdr, registry);
        Thread.currentThread().setContextClassLoader(newLdr);
        final Serializable result;
        try {
            monitor.onTaskStarted(task);
            result = task.invokeTask();
        } catch (GridException ex) {
            if(ex == TaskCancelException.getInstance()) {
                return; // intended behavior
            }
            LOG.warn(ex.getMessage(), ex);
            respListener.onCaughtException(task, ex);
            return;
        } finally {
            Thread.currentThread().setContextClassLoader(origLdr);

            long endTime = System.currentTimeMillis();
            long execTime = endTime - createTime;
            metrics.taskFinished(execTime);

            if(task.isCanceled()) {
                monitor.onTaskCanceled(task);
            } else {
                if(LOG.isInfoEnabled()) {
                    long elapsedTime = endTime - startTime;
                    LOG.info(taskClassName + " [" + task.getTaskId() + "] is finished in .. "
                            + StopWatch.elapsedTime(elapsedTime));
                }
                monitor.onTaskFinished(task);
            }
        }
        assert (!task.isCanceled());
        if(!task.isAsyncTask()) {
            respListener.onResponse(task, result);
        }
    }

}
