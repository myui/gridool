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
package gridool.deployment;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.routing.GridTaskRouter;

import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.struct.Triple;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridDeploymentJob extends GridJobBase<Triple<String, byte[], Long>, Boolean> {
    private static final long serialVersionUID = -1441170539457137918L;
    private static final Log LOG = LogFactory.getLog(GridDeploymentJob.class);

    private transient boolean suceed = true;

    public GridDeploymentJob() {
        super();
    }

    public Map<GridTask, GridNode> map(GridTaskRouter router, Triple<String, byte[], Long> args)
            throws GridException {
        final String clsname = args.getFirst();
        final byte[] b = args.getSecond();
        final long timestamp = args.getThird();

        final GridNode[] nodes = router.getAllNodes();
        final Map<GridTask, GridNode> task2node = new IdentityHashMap<GridTask, GridNode>(nodes.length);
        for(GridNode node : nodes) {
            GridTask task = new GridDeployClassTask(this, clsname, b, timestamp);
            task2node.put(task, node);
        }
        return task2node;
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        final GridException error = result.getException();
        if(error != null) {
            if(LOG.isWarnEnabled()) {
                LOG.warn("Caused an error in task: " + result.getTaskId(), error);
            }
            this.suceed = false;
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    public Boolean reduce() throws GridException {
        return suceed;
    }

}
