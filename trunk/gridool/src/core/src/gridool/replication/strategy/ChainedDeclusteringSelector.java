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
package gridool.replication.strategy;

import gridool.GridNode;
import gridool.replication.ReplicaSelector;
import gridool.routing.GridRouter;

import java.util.List;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class ChainedDeclusteringSelector implements ReplicaSelector {

    public ChainedDeclusteringSelector() {}

    public List<GridNode> selectReplica(GridRouter router, GridNode masterNode, int numReplicas) {
        List<GridNode> nodes = router.listSuccessorNodesInPhysicalChain(masterNode, numReplicas, false);
        return nodes;
    }

}
