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

import java.util.Map;

import javax.annotation.CheckForNull;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.routing.GridTaskRouter;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class GridJobDelegate<A, R> implements GridJob<A, R> {
    private static final long serialVersionUID = -2469128926785218203L;

    protected final GridJob<A, R> job;

    public GridJobDelegate(@CheckForNull GridJob<A, R> job) {
        if(job == null) {
            throw new IllegalArgumentException();
        }
        this.job = job;
    }

    public String getJobId() {
        return job.getJobId();
    }

    public GridNode getJobNode() {
        return job.getJobNode();
    }

    public void setJobNode(GridNode node) {
        job.setJobNode(node);
    }

    public boolean isAsyncOps() {
        return job.isAsyncOps();
    }

    public boolean injectResources() {
        return job.injectResources();
    }

    public Map<GridTask, GridNode> map(GridTaskRouter router, A arg) throws GridException {
        return job.map(router, arg);
    }

    public R reduce() throws GridException {
        return job.reduce();
    }

    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        return job.result(task, result);
    }
}
