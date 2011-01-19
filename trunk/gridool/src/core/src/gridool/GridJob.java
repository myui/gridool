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
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The business logic of {@link GridJob} is breakdown into multiple {@link GridTask}s.
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public interface GridJob<A, R> extends Serializable, GridAnnotatable {

    boolean logJobInfo();

    boolean handleNodeFailure();

    boolean isAsyncOps();

    @Nullable
    String getDeploymentGroup();

    void setDeploymentGroup(@Nullable String deployGroup);

    @CheckForNull
    String getJobId();

    void setJobNode(@Nonnull GridNode node);

    @CheckForNull
    GridNode getJobNode();

    /**
     * Split a {@link GridJob} into multiple {@link GridTask}s and map them to {@link GridNode}s.
     */
    Map<GridTask, GridNode> map(@Nonnull GridRouter router, @Nullable A arg) throws GridException;

    /**
     * Asynchronous callback invoked every time a response from remote execution is returned.
     * Note that {@link GridTaskResultPolicy#SKIP} handling is required when enabling speculative tasks.
     * 
     * @return Result policy that tells how to process further upcoming task results.
     */
    GridTaskResultPolicy result(@Nonnull GridTaskResult result) throws GridException;

    /**
     * Synchronously aggregates all results without a timeout.
     */
    R reduce() throws GridException;

}
