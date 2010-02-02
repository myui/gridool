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
package gridool.replication;

import gridool.GridConfiguration;
import gridool.GridKernel;
import gridool.communication.payload.GridNodeInfo;
import gridool.replication.listener.CoordinateReplicaTaskHandler;
import gridool.replication.listener.ReplicationEventLogger;
import gridool.replication.strategy.ChainedDeclusteringSelector;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class ReplicationModuleBuilder {

    private ReplicationModuleBuilder() {}

    public static ReplicaSelector createReplicaSelector() {
        final ReplicaSelector replicaSelector = new ChainedDeclusteringSelector();
        // TODO
        return replicaSelector;
    }

    public static ReplicaCoordinator createReplicaCoordinator(@Nonnull GridKernel kernel, @Nonnull GridConfiguration config) {
        GridNodeInfo localNode = config.getLocalNode();
        final ReplicaCoordinator coord = new ReplicaCoordinator(localNode);

        // TODO
        addListeners(coord, kernel, new ReplicationEventLogger(), new CoordinateReplicaTaskHandler());

        return coord;
    }

    private static void addListeners(ReplicaCoordinator coord, GridKernel kernel, ReplicaCoordinatorListener... listeners) {
        for(ReplicaCoordinatorListener listener : listeners) {
            listener.setup(kernel);
            coord.addListeners(listener);
        }
    }

}
