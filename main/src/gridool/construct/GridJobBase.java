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

import gridool.GridJob;
import gridool.GridNode;
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

    private String jobId;
    private GridNode jobNode;

    public GridJobBase() {}

    public String getJobId() {
        if(jobId == null) {
            throw new IllegalStateException();
        }
        return jobId;
    }

    public GridNode getJobNode() {
        if(jobNode == null) {
            throw new IllegalStateException();
        }
        return jobNode;
    }

    public void setJobNode(GridNode node) {
        if(node == null) {
            throw new IllegalArgumentException();
        }
        this.jobNode = node;
        String nodeId = GridUtils.getNodeIdentifier(node);
        this.jobId = GridUtils.generateJobId(nodeId, this);
    }

    public boolean isAsyncOps() {
        return false;
    }

    public boolean injectResources() {
        return false;
    }
}
