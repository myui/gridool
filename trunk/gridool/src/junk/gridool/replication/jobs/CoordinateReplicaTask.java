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
package gridool.replication.jobs;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridJob;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.annotation.GridConfigResource;
import gridool.annotation.GridRegistryResource;
import gridool.communication.payload.GridNodeInfo;
import gridool.construct.GridTaskAdapter;
import gridool.replication.ReplicaCoordinator;
import gridool.replication.ReplicaSelector;
import gridool.replication.ReplicationManager;
import gridool.routing.GridRouter;

import java.util.List;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class CoordinateReplicaTask extends GridTaskAdapter {
    private static final long serialVersionUID = -4073277101747517376L;

    @Nonnull
    private final CoordinateReplicaJobConf jobConf;

    @GridConfigResource
    private transient GridConfiguration conf;
    @GridRegistryResource
    private transient GridResourceRegistry registry;

    @SuppressWarnings("unchecked")
    public CoordinateReplicaTask(GridJob job, CoordinateReplicaJobConf jobConf) {
        super(job, false);
        this.jobConf = jobConf;
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    protected GridNode execute() throws GridException {
        ReplicationManager manager = registry.getReplicationManager();

        ReplicaSelector selector = manager.getReplicaSelector();
        GridRouter router = registry.getRouter();
        GridNodeInfo localNode = conf.getLocalNode();
        int numReplicas = jobConf.getNumReplicas();
        List<GridNode> replicas = selector.selectReplica(router, localNode, numReplicas);

        ReplicaCoordinator coord = manager.getReplicaCoordinator();
        coord.configureReplica(localNode, replicas, jobConf);

        return localNode;
    }

}
