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
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.routing.GridRouter;

import java.util.Map;

import javax.annotation.CheckForNull;

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

    @Override
    public boolean logJobInfo() {
        return job.logJobInfo();
    }

    @Override
    public boolean handleNodeFailure() {
        return job.handleNodeFailure();
    }

    @Override
    public boolean isAsyncOps() {
        return job.isAsyncOps();
    }

    @Override
    public boolean injectResources() {
        return job.injectResources();
    }

    @Override
    public String getJobId() {
        return job.getJobId();
    }

    @Override
    public String getDeploymentGroup() {
        return job.getDeploymentGroup();
    }

    @Override
    public void setDeploymentGroup(String deployGroup) {
        job.setDeploymentGroup(deployGroup);
    }

    @Override
    public GridNode getJobNode() {
        return job.getJobNode();
    }

    @Override
    public void setJobNode(GridNode node) {
        job.setJobNode(node);
    }

    @Override
    public Map<GridTask, GridNode> map(GridRouter router, A arg) throws GridException {
        return job.map(router, arg);
    }

    @Override
    public R reduce() throws GridException {
        return job.reduce();
    }

    @Override
    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        return job.result(result);
    }
}
