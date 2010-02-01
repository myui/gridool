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
import gridool.GridLocatable;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskRelocatability;
import gridool.routing.GridTaskRouter;

import java.io.Serializable;
import java.util.List;

import javax.annotation.CheckForNull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridTaskInfo implements GridTask {
    private static final long serialVersionUID = -2109185675088636762L;

    private final String taskId;

    public GridTaskInfo(@CheckForNull String taskId) {
        if(taskId == null) {
            throw new IllegalArgumentException();
        }
        this.taskId = taskId;
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
            GridTask task = (GridTask) obj;
            String anotherTaskId = task.getTaskId();
            return taskId.equals(anotherTaskId);
        }
        return false;
    }

    public int compareTo(GridLocatable other) {
        String anotherKey = other.getKey();
        String selfKey = getKey();
        return selfKey.compareTo(anotherKey);
    }

    //---------------------------------------------------
    // unsupported operations

    public boolean injectResources() {
        throw new UnsupportedOperationException();
    }

    public boolean isAsyncTask() {
        throw new UnsupportedOperationException();
    }

    public final String getTaskId() {
        throw new UnsupportedOperationException();
    }

    public final String getJobId() {
        throw new UnsupportedOperationException();
    }

    public GridTaskRelocatability getRelocatability() {
        throw new UnsupportedOperationException();
    }

    public String getKey() {
        throw new UnsupportedOperationException();
    }

    public long getFinishedTime() {
        throw new UnsupportedOperationException();
    }

    public long getStartedTime() {
        throw new UnsupportedOperationException();
    }

    public void setFinishedTime(long time) {
        throw new UnsupportedOperationException();
    }

    public void setStartedTime(long time) {
        throw new UnsupportedOperationException();
    }

    public GridNode getSenderNode() {
        throw new UnsupportedOperationException();
    }

    public Serializable invokeTask() throws GridException {
        throw new UnsupportedOperationException();
    }

    public Serializable execute() throws GridException {
        throw new UnsupportedOperationException();
    }

    public boolean cancel() throws GridException {
        throw new UnsupportedOperationException();
    }

    public boolean isCanceled() {
        throw new UnsupportedOperationException();
    }

    public boolean isFinished() {
        throw new UnsupportedOperationException();
    }

    public boolean isFailoverActive() {
        throw new UnsupportedOperationException();
    }

    public List<GridNode> listFailoverCandidates(GridNode localNode, GridTaskRouter router) {
        throw new UnsupportedOperationException();
    }

    public int getTaskNumber() {
        throw new UnsupportedOperationException();
    }

    public void setTaskNumber(int i) {
        throw new UnsupportedOperationException();
    }

    public boolean isReplicatable() {
        throw new UnsupportedOperationException();
    }

    public void setTransferToReplica(GridNode masterNode) {
        throw new UnsupportedOperationException();
    }

}
