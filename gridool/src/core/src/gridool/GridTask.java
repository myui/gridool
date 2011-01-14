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
package gridool;

import gridool.annotation.GridAnnotatable;
import gridool.routing.GridRouter;

import java.io.Serializable;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public interface GridTask extends GridLocatable, GridAnnotatable {

    // ----------------------
    // task properties

    boolean isAsyncTask();

    boolean handleNodeFailure();

    // ----------------------
    // runtime information

    void setStartedTime(long time);

    long getStartedTime();

    void setFinishedTime(long time);

    long getFinishedTime();

    // ----------------------
    // task information

    @Nonnull
    String getTaskId();

    @Nonnull
    String getJobId();

    @CheckForNull
    GridNode getSenderNode();

    void setAssignedNode(GridNode node);

    @Nullable
    GridNode getAssignedNode();

    // ----------------------
    // optional task information

    void setTaskNumber(int i);

    int getTaskNumber();

    // ----------------------
    // controls

    Serializable invokeTask() throws GridException;

    boolean cancel() throws GridException;

    boolean isCanceled();

    boolean isFinished();

    // ----------------------
    // fail-over

    GridTaskRelocatability getRelocatability();

    boolean isFailoverActive();

    // TODO REVIEWME
    @Nonnull
    List<GridNode> listFailoverCandidates(@Nonnull GridRouter router);

    // ----------------------
    // replication

    boolean isReplicatable();

    void setTransferToReplica(@Nonnull GridNode masterNode);

    @Nullable
    List<GridNode> getReplicatedNodes();

    void setReplicatedNodes(@Nonnull List<GridNode> replicatedNodes);

}
