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

import javax.annotation.Nullable;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridNode;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.Settings;
import gridool.util.GridUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class GridJobBase<A, R> implements GridJob<A, R> {
    private static final long serialVersionUID = 7625192468381092435L;
    private static final boolean logJobInfo = Boolean.parseBoolean(Settings.get("gridool.job.log_jobinfo"));

    private String jobId;
    private GridNode jobNode;
    @Nullable
    private String deploymentGroup;

    public GridJobBase() {}

    @Override
    public boolean logJobInfo() {
        return logJobInfo;
    }

    @Override
    public boolean handleNodeFailure() {
        return true;
    }

    @Override
    public String getJobId() {
        if(jobId == null) {
            throw new IllegalStateException();
        }
        return jobId;
    }

    @Override
    public String getDeploymentGroup() {
        return deploymentGroup;
    }

    @Override
    public void setDeploymentGroup(@Nullable String deployGroup) {
        this.deploymentGroup = deployGroup;
    }

    @Override
    public GridNode getJobNode() {
        if(jobNode == null) {
            throw new IllegalStateException();
        }
        return jobNode;
    }

    @Override
    public void setJobNode(GridNode node) {
        if(node == null) {
            throw new IllegalArgumentException();
        }
        this.jobNode = node;
        String nodeId = GridUtils.getNodeIdentifier(node);
        this.jobId = GridUtils.generateJobId(nodeId, this);
    }

    @Override
    public boolean isAsyncOps() {
        return false;
    }

    @Override
    public boolean injectResources() {
        return false;
    }

    @Override
    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        GridException err = result.getException();
        if(err != null) {
            GridNode node = result.getExecutedNode();
            String errmsg = getClass().getSimpleName() + " failed on node: " + node;
            throw new GridException(errmsg, err);
        }
        return GridTaskResultPolicy.CONTINUE;
    }
}
