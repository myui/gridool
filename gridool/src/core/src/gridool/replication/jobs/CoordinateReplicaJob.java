/*
 * @(#)$Id$
 *
 * Copyright 2006-2010 Makoto YUI
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
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridJobBase;
import gridool.replication.ReplicationManager;
import gridool.routing.GridRouter;
import gridool.util.lang.ArrayUtils;
import gridool.util.lang.ObjectUtils;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class CoordinateReplicaJob extends GridJobBase<CoordinateReplicaJobConf, GridNode[]> {
    private static final long serialVersionUID = 7562696673301106475L;
    private static final Log LOG = LogFactory.getLog(CoordinateReplicaJob.class);

    private transient List<GridNode> failedNodes;

    @GridRegistryResource
    private transient GridResourceRegistry registry;

    public CoordinateReplicaJob() {
        super();
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    public Map<GridTask, GridNode> map(GridRouter router, CoordinateReplicaJobConf jobConf)
            throws GridException {
        final ReplicationManager replMgr = registry.getReplicationManager();
        final int numReplicas = jobConf.getNumReplicas();

        final StringBuilder buf = new StringBuilder(256);
        buf.append("Configure replicas as follows ..\n");
        final GridNode[] nodes = router.getAllNodes();
        for(GridNode node : nodes) {
            if(replMgr.coordinateReplica(node, numReplicas, jobConf)) {
                buf.append(node).append(": ").append(node.getReplicas()).append('\n');
            } else {
                buf.append(node).append(": ").append("N/A\n");
                LOG.error("Cannot prepare " + numReplicas + " replicas for node '" + node + "': "
                        + node.getReplicas());
            }
        }
        if(LOG.isInfoEnabled()) {
            LOG.info(buf);
        }

        final Map<GridTask, GridNode> mapping = new IdentityHashMap<GridTask, GridNode>(nodes.length);
        final byte[] nodesBytes = ObjectUtils.toBytes(nodes);
        for(GridNode node : nodes) {
            GridTask task = new CoordinateReplicaTask(this, nodesBytes, jobConf);
            mapping.put(task, node);
        }

        this.failedNodes = new ArrayList<GridNode>(4);
        return mapping;
    }

    @Override
    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        Boolean succeed = result.getResult();
        if(!Boolean.TRUE.equals(succeed)) {
            GridNode fnode = result.getExecutedNode();
            failedNodes.add(fnode);
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    public GridNode[] reduce() throws GridException {
        return ArrayUtils.toArray(failedNodes, GridNode[].class);
    }

}
