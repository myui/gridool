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
package gridool.communication.payload;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import xbird.util.io.IOUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridTaskResultImpl implements GridTaskResult, Externalizable {
    private static final long serialVersionUID = -4203147223987810971L;

    @Nonnull
    private/* final */String taskId;
    @Nonnull
    private/* final */GridNode executedNode;
    @Nonnull
    private/* final */List<GridNode> replicatedNodes;

    @Nullable
    private/* final */Serializable result;
    @Nullable
    private/* final */GridException exception;

    private boolean failoverScheduled = false;

    // -----------------------------------------------
    // local only resources    

    @Nonnull
    private transient Collection<GridTask> speculativeTasks = Collections.emptyList();

    // -----------------------------------------------

    public GridTaskResultImpl() {}//for Externalizable

    public GridTaskResultImpl(@CheckForNull String taskId, @CheckForNull GridNode executedNode, @CheckForNull List<GridNode> replicatedNodes, @Nullable Serializable result) {
        if(taskId == null) {
            throw new IllegalArgumentException();
        }
        if(executedNode == null) {
            throw new IllegalArgumentException();
        }
        this.taskId = taskId;
        this.executedNode = executedNode;
        this.replicatedNodes = (replicatedNodes == null) ? Collections.<GridNode> emptyList()
                : replicatedNodes;
        this.result = result;
        this.exception = null;
    }

    public GridTaskResultImpl(@CheckForNull String taskId, @CheckForNull GridNode executedNode, @CheckForNull GridException exception) {
        if(taskId == null) {
            throw new IllegalArgumentException();
        }
        if(executedNode == null) {
            throw new IllegalArgumentException();
        }
        if(exception == null) {
            throw new IllegalArgumentException();
        }
        this.taskId = taskId;
        this.executedNode = executedNode;
        this.replicatedNodes = Collections.emptyList();
        this.result = null;
        this.exception = exception;
    }

    public String getTaskId() {
        return taskId;
    }

    public GridNode getExecutedNode() {
        return executedNode;
    }

    public List<GridNode> getReplicatedNodes() {
        return replicatedNodes;
    }

    @SuppressWarnings("unchecked")
    public <T extends Serializable> T getResult() {
        return (T) result;
    }

    public GridException getException() {
        return exception;
    }

    public GridTaskResult scheduleFailover() {
        this.failoverScheduled = true;
        return this;
    }

    public boolean isFailoverScheduled() {
        return failoverScheduled;
    }

    @Nonnull
    public Collection<GridTask> getSpeculativeTasks() {
        return speculativeTasks;
    }

    public void setSpeculativeTasks(@CheckForNull Collection<GridTask> speculativeTasks) {
        if(speculativeTasks == null) {
            throw new IllegalArgumentException();
        }
        this.speculativeTasks = speculativeTasks;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.taskId = IOUtils.readString(in);
        this.executedNode = (GridNode) in.readObject();
        final int numNodes = in.readInt();
        if(numNodes > 0) {
            final List<GridNode> nodes = new ArrayList<GridNode>(numNodes);
            for(int i = 0; i < numNodes; i++) {
                GridNode node = (GridNode) in.readObject();
                nodes.add(node);
            }
            this.replicatedNodes = nodes;
        } else {
            this.replicatedNodes = Collections.emptyList();
        }
        this.result = (Serializable) in.readObject();
        this.exception = (GridException) in.readObject();
        this.failoverScheduled = in.readBoolean();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(taskId, out);
        out.writeObject(executedNode);
        final int numNodes = replicatedNodes.size();
        out.writeInt(numNodes);
        for(int i = 0; i < numNodes; i++) {
            GridNode node = replicatedNodes.get(i);
            out.writeObject(node);
        }
        out.writeObject(result);
        out.writeObject(exception);
        out.writeBoolean(failoverScheduled);
    }

}
