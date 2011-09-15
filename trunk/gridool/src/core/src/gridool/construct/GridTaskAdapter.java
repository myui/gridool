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
package gridool.construct;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridLocatable;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskRelocatability;
import gridool.annotation.GridExecutionMonitorResource;
import gridool.monitor.GridExecutionMonitor;
import gridool.processors.task.TaskCancelException;
import gridool.routing.GridRouter;
import gridool.util.GridUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class GridTaskAdapter implements GridTask, Callable<Serializable> {
    private static final long serialVersionUID = -560033960154200583L;
    private static final Log LOG = LogFactory.getLog(GridTaskAdapter.class);

    @Nonnull
    protected final String jobId;
    @Nonnull
    protected final String taskId;
    @Nonnull
    protected final String deploymentGroup;
    @Nonnull
    protected final GridNode senderNode;
    protected int taskNumber = -1;

    protected long startedTime = -1L;
    protected long finishedTime = -1L;

    private final boolean isFailoverActive;
    private final boolean handleNodeFailure;

    protected volatile boolean canceled = false;
    private transient volatile FutureTask<Serializable> running;

    @Nullable
    private transient GridNode assignedNode;

    @GridExecutionMonitorResource
    protected transient GridExecutionMonitor monitor;
    @Nullable
    private transient List<GridNode> replicatedNodes = Collections.emptyList();

    @SuppressWarnings("unchecked")
    protected GridTaskAdapter(@CheckForNull GridJob job, boolean failover) {
        if(job == null) {
            throw new IllegalArgumentException();
        }
        this.jobId = job.getJobId();
        assert (jobId != null);
        this.taskId = GridUtils.generateTaskId(jobId, this);
        this.deploymentGroup = job.getDeploymentGroup();
        this.senderNode = job.getJobNode();
        this.isFailoverActive = failover;
        this.handleNodeFailure = job.handleNodeFailure();
    }

    public boolean injectResources() {
        return false; // TODO REVIEWME should be true by the default?
    }

    public boolean isAsyncTask() {
        return false;
    }

    public boolean handleNodeFailure() {
        return handleNodeFailure;
    }

    public final String getJobId() {
        return jobId;
    }

    public final String getTaskId() {
        return taskId;
    }

    public final String getDeploymentGroup() {
        return deploymentGroup;
    }

    public int getTaskNumber() {
        return taskNumber;
    }

    public void setTaskNumber(int i) {
        this.taskNumber = i;
    }

    public String getKey() {
        return taskId;
    }

    public final long getStartedTime() {
        return startedTime;
    }

    public final long getFinishedTime() {
        return finishedTime;
    }

    public final void setStartedTime(long startedTime) {
        this.startedTime = startedTime;
    }

    public final void setFinishedTime(long finishedTime) {
        this.finishedTime = finishedTime;
    }

    public GridNode getSenderNode() {
        return senderNode;
    }

    public void setAssignedNode(GridNode assignedNode) {
        this.assignedNode = assignedNode;
    }

    public GridNode getAssignedNode() {
        return assignedNode;
    }

    public final Serializable invokeTask() throws GridException {
        final FutureTask<Serializable> runningTask = new FutureTask<Serializable>(this);
        this.running = runningTask;
        runningTask.run();
        try {
            return runningTask.get();
        } catch (InterruptedException e) {
            LOG.warn("task canceled: " + getTaskId());
            throw TaskCancelException.getInstance();
        } catch (ExecutionException e) {
            throw new GridException(e);
        } finally {
            this.running = null;
        }
    }

    /** 
     * Just wrapping {@link #execute()} for {@link Callable}.
     */
    public final Serializable call() throws Exception {
        return execute();
    }

    protected abstract Serializable execute() throws GridException;

    public boolean cancel() throws GridException {
        final FutureTask<Serializable> r = running;
        if(r == null) {
            return false;
        }
        boolean status = r.cancel(true);
        this.canceled = status;
        return status;
    }

    public final boolean isFinished() {
        return finishedTime != -1L;
    }

    public final boolean isCanceled() {
        return canceled;
    }

    public GridTaskRelocatability getRelocatability() {
        return GridTaskRelocatability.unable;
    }

    public boolean isFailoverActive() {
        return isFailoverActive;
    }

    public List<GridNode> listFailoverCandidates(GridRouter router) {
        if(!isFailoverActive) {
            throw new IllegalStateException("Failover is not active");
        }
        if(assignedNode == null) {
            throw new IllegalStateException("Task is not assigned yet, but failover is invoked: "
                    + taskId);
        }
        final List<GridNode> nodeList;
        switch(getRelocatability()) {
            case relocatableToAnywhere:
                final GridNode[] nodes = router.getAllNodes();
                nodeList = new ArrayList<GridNode>(nodes.length);
                for(GridNode node : nodes) {
                    if(!node.equals(assignedNode)) {
                        nodeList.add(node);
                    }
                }
                break;
            case restricedToReplica:
                nodeList = assignedNode.getReplicas();
                break;
            case unable:
                nodeList = Collections.emptyList();
                break;
            default:
                throw new IllegalStateException("Unexpected task replicatability: "
                        + getRelocatability());
        }
        return nodeList;
    }

    public boolean isReplicatable() {
        return false;
    }

    public void setTransferToReplica(GridNode masterNode) {}

    @Nullable
    public List<GridNode> getReplicatedNodes() {
        return replicatedNodes;
    }

    public void setReplicatedNodes(@Nonnull List<GridNode> replicatedNodes) {
        this.replicatedNodes = replicatedNodes;
    }

    public int compareTo(GridLocatable other) {
        String otherKey = other.getKey();
        String selfKey = getKey();
        return selfKey.compareTo(otherKey);
    }

    @Override
    public int hashCode() {
        return taskId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }
        if(obj instanceof GridTask) {
            GridTask otherTask = (GridTask) obj;
            String otherId = otherTask.getTaskId();
            return taskId.equals(otherId);
        }
        return false;
    }

    protected final void reportProgress(final float progress) {
        monitor.progress(this, progress);
    }
}
