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
package gridool.replication.jobs;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridLocatable;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskRelocatability;
import gridool.routing.GridTaskRouter;
import gridool.util.GridUtils;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class ReplicatedGridTaskAdapter implements GridTask, Serializable {
    private static final long serialVersionUID = 8784784021314404959L;

    @Nonnull
    private final String jobId;
    @Nonnull
    private final String taskId;
    @Nonnull
    private final GridNode senderNode;
    @Nonnull
    private final GridTask delegated;

    private long startedTime = -1L;
    private long finishedTime = -1L;

    public ReplicatedGridTaskAdapter(@Nonnull GridJob<?, ?> job, @Nonnull GridTask task) {
        this.jobId = job.getJobId();
        this.taskId = GridUtils.generateTaskId(jobId, this);
        this.senderNode = job.getJobNode();
        this.delegated = task;
    }

    public boolean isReplicatable() {
        return false;
    }

    public void setReplication() {
        throw new IllegalStateException();
    }

    public String getKey() {
        return taskId;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getJobId() {
        return jobId;
    }

    public GridNode getSenderNode() {
        return senderNode;
    }

    public boolean isAsyncTask() {
        return false;
    }

    public GridTaskRelocatability getRelocatability() {
        return GridTaskRelocatability.unable;
    }

    public boolean isFailoverActive() {
        return false;
    }

    public List<GridNode> listFailoverCandidates(GridTask task, GridTaskRouter router) {
        return Collections.emptyList();
    }

    public final long getStartedTime() {
        return startedTime;
    }

    public final void setStartedTime(long startedTime) {
        this.startedTime = startedTime;
    }

    public final long getFinishedTime() {
        return finishedTime;
    }

    public final void setFinishedTime(long finishedTime) {
        this.finishedTime = finishedTime;
    }

    public final boolean isFinished() {
        return finishedTime != -1L;
    }

    // -----------------------------------------------------

    public boolean injectResources() {
        return delegated.injectResources();
    }

    public Serializable invokeTask() throws GridException {
        return delegated.invokeTask();
    }

    public Serializable execute() throws GridException {
        return delegated.execute();
    }

    public boolean cancel() throws GridException {
        return delegated.cancel();
    }

    public boolean isCanceled() {
        return delegated.isCanceled();
    }

    // -----------------------------------------------

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

    // -----------------------------------------------
    // unsupported operations

    public int getTaskNumber() {
        throw new UnsupportedOperationException();
    }

    public void setTaskNumber(int i) {
        throw new UnsupportedOperationException();
    }

}