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
package gridool.replication.listener;

import gridool.GridKernel;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.discovery.DiscoveryEvent;
import gridool.replication.ReplicaCoordinatorListener;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class CoordinateReplicaTaskHandler implements ReplicaCoordinatorListener {

    private GridKernel kernel;

    public CoordinateReplicaTaskHandler() {}

    public void setup(GridKernel kernel) {
        this.kernel = kernel;
    }

    public boolean onConfigureReplica(GridNode masterNode, List<GridNode> oldReplicas, List<GridNode> newReplicas) {
        assert (kernel != null);

        final List<GridNode> addedReplicas = new ArrayList<GridNode>();
        for(GridNode newNode : newReplicas) {
            if(!oldReplicas.contains(newNode)) {
                addedReplicas.add(newNode);
            }
        }
        final List<GridNode> removedReplics = new ArrayList<GridNode>();
        for(GridNode oldNode : oldReplicas) {
            if(!newReplicas.contains(oldNode)) {
                removedReplics.add(oldNode);
            }
        }

        GridResourceRegistry registry = kernel.getResourceRegistry();

        return false;
    }

    public void onChannelClosed() {}// NOP

    public void onDiscovery(DiscoveryEvent event, GridNode node) {}// NOP

}
