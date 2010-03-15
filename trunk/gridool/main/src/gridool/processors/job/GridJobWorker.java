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
package gridool.processors.job;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridJob;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.annotation.GridAnnotationProcessor;
import gridool.communication.GridCommunicationManager;
import gridool.deployment.GridPerNodeClassLoader;
import gridool.discovery.GridDiscoveryListener;
import gridool.discovery.GridDiscoveryService;
import gridool.monitor.GridExecutionMonitor;
import gridool.processors.task.GridTaskProcessor;
import gridool.routing.GridNodeSelector;
import gridool.routing.GridTaskRouter;
import gridool.taskqueue.sender.SenderResponseTaskQueue;
import gridool.util.GridUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.config.Settings;
import xbird.util.concurrent.CancellableTask;
import xbird.util.concurrent.ExecutorFactory;
import xbird.util.datetime.StopWatch;
import xbird.util.datetime.TextProgressBar;
import xbird.util.lang.ClassUtils;
import xbird.util.primitive.Primitives;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridJobWorker<A, R> implements CancellableTask<R> {
    private static final Log LOG = LogFactory.getLog(GridJobWorker.class);
    private static final long JOB_INSPECTION_INTERVAL_SEC;
    static {
        JOB_INSPECTION_INTERVAL_SEC = Primitives.parseLong(Settings.get("gridool.job.inspection_interval"), 300);
    }

    private final GridJob<A, R> job;
    private final A arg;

    private final GridExecutionMonitor monitor;

    private final GridTaskRouter router;
    private final GridCommunicationManager communicationMgr;
    private final GridTaskProcessor taskProc;
    private final GridDiscoveryService discoveryService;
    private final SenderResponseTaskQueue taskResponseQueue;
    private final GridAnnotationProcessor annotationProc;
    private final GridConfiguration config;

    private final GridPerNodeClassLoader nodeClassLoader;

    private final ThreadPoolExecutor execPool;

    public GridJobWorker(@Nonnull GridJob<A, R> job, @Nullable A arg, @Nonnull GridExecutionMonitor monitor, @Nonnull GridResourceRegistry resourceRegistry, @Nonnull GridConfiguration config) {
        this.job = job;
        this.arg = arg;
        this.monitor = monitor;
        this.router = resourceRegistry.getTaskRouter();
        this.communicationMgr = resourceRegistry.getCommunicationManager();
        this.taskProc = resourceRegistry.getTaskProcessor();
        this.discoveryService = resourceRegistry.getDiscoveryService();
        this.taskResponseQueue = resourceRegistry.getTaskManager().getSenderResponseQueue();
        this.annotationProc = resourceRegistry.getAnnotationProcessor();
        this.config = config;
        int poolSize = config.getTaskAssignorPoolSize();
        this.execPool = ExecutorFactory.newFixedThreadPool(poolSize, "GridTaskAssignor");
        GridNode localNodeInfo = communicationMgr.getLocalNode();
        job.setJobNode(localNodeInfo);
        this.nodeClassLoader = resourceRegistry.getNodeClassLoader(localNodeInfo);
    }

    public void cancel() {
        if(!job.isAsyncOps()) {
            execPool.shutdownNow();
        }
    }

    public FutureTask<R> newTask() {
        return new FutureTask<R>(this) {
            @SuppressWarnings("finally")
            public boolean cancel(boolean mayInterruptIfRunning) {
                try {
                    GridJobWorker.this.cancel();
                } finally {
                    return super.cancel(mayInterruptIfRunning);
                }
            }
        };
    }

    public R call() throws Exception {
        final long startTime = System.currentTimeMillis();
        ClassLoader origLdr = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(nodeClassLoader);
        try {
            monitor.onJobStarted(job);
            return runJob();
        } catch (GridException ge) {
            LOG.error(ge.getMessage(), ge);
            throw ge;
        } catch (Throwable e) {
            LOG.fatal(e.getMessage(), e);
            throw new GridException(e);
        } finally {
            Thread.currentThread().setContextClassLoader(origLdr);
            if(LOG.isInfoEnabled()) {
                long elapsedTime = System.currentTimeMillis() - startTime;
                LOG.info(ClassUtils.getSimpleClassName(job) + " [" + job.getJobId()
                        + "] finished in " + StopWatch.elapsedTime(elapsedTime));
            }
            monitor.onJobFinished(job);
        }
    }

    private R runJob() throws GridException {
        if(job.injectResources()) {
            annotationProc.injectResources(job);
        }
        final Map<GridTask, GridNode> mappedTasks = job.map(router, arg);
        if(mappedTasks == null) {
            String errmsg = "Job mapping operation produces no mapped tasks for job: " + job;
            LOG.error(errmsg);
            throw new GridException(errmsg);
        }

        final String jobClassName = ClassUtils.getSimpleClassName(job);
        final String jobId = job.getJobId();
        final int numTasks = mappedTasks.size();
        if(numTasks == 0) {
            if(LOG.isInfoEnabled()) {
                LOG.info(jobClassName + "#map of a job [" + jobId + "] returns an empty result");
            }
            return job.reduce();
        }
        if(LOG.isInfoEnabled()) {
            LOG.info(jobClassName + " [" + jobId + "] is mapped to " + numTasks + " tasks (nodes)");
        }
        final TextProgressBar progressBar = new TextProgressBar(jobClassName + " [" + jobId + ']',
                numTasks) {
            protected void show() {
                if(LOG.isInfoEnabled()) {
                    LOG.info(getInfo());
                }
            }
        };
        progressBar.setRefreshTime(5000L);
        progressBar.setRefreshFluctations(20);

        final BlockingQueue<GridTaskResult> resultQueue = new ArrayBlockingQueue<GridTaskResult>(numTasks);
        taskResponseQueue.addResponseQueue(jobId, resultQueue);
        final Map<String, Pair<GridTask, List<Future<?>>>> taskMap = new ConcurrentHashMap<String, Pair<GridTask, List<Future<?>>>>(numTasks); // taskMap contends rarely

        GridDiscoveryListener nodeFailoverHandler = new GridNodeFailureHandler(mappedTasks, taskMap, resultQueue);
        discoveryService.addListener(nodeFailoverHandler);

        // assign map tasks
        assignMappedTasks(mappedTasks, taskMap, resultQueue);

        if(job.isAsyncOps()) {
            discoveryService.removeListener(nodeFailoverHandler); // REVIEWME
            return null;
        }

        // collect map task results
        aggregateTaskResults(taskMap, resultQueue, progressBar);
        discoveryService.removeListener(nodeFailoverHandler);
        // invoke reduce
        R result = job.reduce();

        if(!taskMap.isEmpty()) {//TODO REVIEWME
            assert (!job.isAsyncOps());
            cancelRemainingTasks(taskMap);
        }

        progressBar.finish();
        return result;
    }

    private void assignMappedTasks(final Map<GridTask, GridNode> mappedTasks, final Map<String, Pair<GridTask, List<Future<?>>>> taskMap, final BlockingQueue<GridTaskResult> resultQueue)
            throws GridException {
        for(final Map.Entry<GridTask, GridNode> mappedTask : mappedTasks.entrySet()) {
            GridTask task = mappedTask.getKey();
            GridNode node = mappedTask.getValue();

            if(task == null) {
                String errmsg = "GridJob.map(...) method returned null task: [job=" + job
                        + ", mappedTask:" + mappedTask + ']';
                LOG.error(errmsg);
                throw new GridException(errmsg);
            }
            if(node == null) {
                String errmsg = "GridJob.map(...) method returned null node: [job=" + job
                        + ", mappedTask:" + mappedTask + ']';
                LOG.error(errmsg);
                throw new GridException(errmsg);
            }

            GridTaskAssignor workerTask = new GridTaskAssignor(task, node, taskProc, communicationMgr, resultQueue);
            task.setAssignedNode(node);
            List<Future<?>> futureList = new ArrayList<Future<?>>(2);
            if(taskMap.put(task.getTaskId(), new Pair<GridTask, List<Future<?>>>(task, futureList)) != null) {
                throw new GridException("Found duplicate taskId: " + task.getTaskId());
            }
            Future<?> future = execPool.submit(workerTask);
            futureList.add(future);
        }
    }

    private void aggregateTaskResults(final Map<String, Pair<GridTask, List<Future<?>>>> taskMap, final BlockingQueue<GridTaskResult> resultQueue, final TextProgressBar progressBar)
            throws GridException {
        final int numTasks = taskMap.size();
        for(int finishedTasks = 0; finishedTasks < numTasks; finishedTasks++) {
            GridTaskResult result = null;
            try {
                if(JOB_INSPECTION_INTERVAL_SEC == -1L) {
                    result = resultQueue.take();
                } else {
                    while(true) {
                        result = resultQueue.poll(JOB_INSPECTION_INTERVAL_SEC, TimeUnit.SECONDS);
                        if(result != null) {
                            break;
                        }
                        if(LOG.isInfoEnabled()) {
                            showRemainingTasks(job, taskMap);
                        }
                    }
                }
            } catch (InterruptedException e) {
                LOG.warn("GridTask is interrupted", e);
                throw new GridException("GridTask is interrupted", e);
            }
            final String taskId = result.getTaskId();
            final GridTaskResultPolicy policy = job.result(result);
            switch(policy) {
                case CONTINUE: {
                    progressBar.inc();
                    taskMap.remove(taskId);
                    break;
                }
                case CONTINUE_WITH_SPECULATIVE_TASKS: {
                    progressBar.inc();
                    taskMap.remove(taskId);
                    for(final GridTask task : result.getSpeculativeTasks()) {
                        String speculativeTaskId = task.getTaskId();
                        Pair<GridTask, List<Future<?>>> entry = taskMap.get(speculativeTaskId);
                        if(entry == null) {// Is the task already finished?
                            LOG.info("No need to run a speculative task: " + speculativeTaskId);
                        } else {
                            Future<?> newFuture = runSpeculativeTask(task, resultQueue); // SKIP handling is required for speculative tasks
                            if(newFuture != null) {
                                List<Future<?>> futures = entry.getSecond();
                                futures.add(newFuture);
                            }
                        }
                    }
                    break;
                }
                case RETURN:
                    taskMap.remove(taskId);
                    return;
                case CANCEL_RETURN: {
                    taskMap.remove(taskId);
                    if(!taskMap.isEmpty()) {
                        cancelRemainingTasks(taskMap);
                    }
                    return;
                }
                case FAILOVER: {
                    Pair<GridTask, List<Future<?>>> entry = taskMap.get(taskId);
                    GridTask task = entry.getFirst();
                    Future<?> newFuture = failover(task, resultQueue);
                    List<Future<?>> futureList = entry.getSecond();
                    futureList.add(newFuture);
                    finishedTasks--;
                    break;
                }
                case SKIP:
                    finishedTasks--;
                    break;
                default:
                    assert false : "Unexpected policy: " + policy;
            }
        }
    }

    private void cancelRemainingTasks(final Map<String, Pair<GridTask, List<Future<?>>>> taskMap) {
        for(final Pair<GridTask, List<Future<?>>> entry : taskMap.values()) {
            final List<Future<?>> futures = entry.getSecond();
            assert (futures != null);
            for(final Future<?> future : futures) {
                if(!future.isDone()) {
                    // TODO send cancel request
                    future.cancel(true);
                }
            }
        }
    }

    @Nonnull
    private Future<?> failover(@CheckForNull final GridTask task, final BlockingQueue<GridTaskResult> resultQueue)
            throws GridException {
        if(task == null) {
            throw new IllegalArgumentException();
        }
        if(!task.isFailoverActive()) {
            throw new GridException("Failover is not active for this task: " + task);
        }
        GridNode localNode = config.getLocalNode();
        final List<GridNode> candidates = task.listFailoverCandidates(localNode, router);
        if(candidates.isEmpty()) {
            throw new GridException("Failover failed because there is no relocatable node: [job="
                    + job + ", task:" + task + ']');
        }
        GridNodeSelector selector = config.getNodeSelector();
        GridNode node = selector.selectNode(candidates, config);
        assert (node != null);
        if(LOG.isWarnEnabled()) {
            LOG.warn("[Failover] Assigned a task [" + task.getTaskId() + "] to node [" + node + "]");
        }
        task.setTransferToReplica(localNode);
        GridTaskAssignor workerTask = new GridTaskAssignor(task, node, taskProc, communicationMgr, resultQueue);
        return execPool.submit(workerTask);
    }

    @Nullable
    private Future<?> runSpeculativeTask(@CheckForNull final GridTask task, final BlockingQueue<GridTaskResult> resultQueue) {
        if(task == null) {
            throw new IllegalArgumentException();
        }
        GridNode localNode = config.getLocalNode();
        final List<GridNode> candidates = task.listFailoverCandidates(localNode, router);
        if(candidates.isEmpty()) {
            return null;
        }
        GridNodeSelector selector = config.getNodeSelector();
        GridNode node = selector.selectNode(candidates, config);
        assert (node != null);
        if(LOG.isInfoEnabled()) {
            LOG.info("[Speculative Execution] Assigned a speculative task [" + task.getTaskId()
                    + "] to node [" + node + "]");
        }
        GridTaskAssignor workerTask = new GridTaskAssignor(task, node, taskProc, communicationMgr, resultQueue);
        return execPool.submit(workerTask);
    }

    private static void showRemainingTasks(final GridJob<?, ?> job, final Map<String, Pair<GridTask, List<Future<?>>>> taskMap) {
        final StringBuilder buf = new StringBuilder(256);
        String jobClassName = ClassUtils.getSimpleClassName(job);
        buf.append("Start a job inspection... ");
        buf.append(jobClassName);
        buf.append(" [");
        buf.append(job.getJobId());
        buf.append("] Pending tasks: \n");
        final Iterator<Pair<GridTask, List<Future<?>>>> itor = taskMap.values().iterator();
        boolean first = true;
        while(itor.hasNext()) {
            if(first) {
                first = false;
            } else {
                buf.append(",\n");
            }
            Pair<GridTask, List<Future<?>>> e = itor.next();
            GridTask task = e.getFirst();
            String taskClassName = ClassUtils.getSimpleClassName(task);
            buf.append(taskClassName);
            buf.append('[');
            String taskId = task.getTaskId();
            buf.append(taskId);
            buf.append(']');
            buf.append(" on ");
            GridNode node = task.getAssignedNode();
            String nodeinfo = GridUtils.toHostNameAddrPort(node);
            buf.append(nodeinfo);
        }
        LOG.info(buf.toString());
    }

}
