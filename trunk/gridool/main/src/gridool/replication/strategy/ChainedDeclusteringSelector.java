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
import gridool.communication.payload.GridNodeInfo;
import gridool.replication.ReplicaSelector;
import gridool.routing.GridTaskRouter;

import java.util.List;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class ChainedDeclusteringSelector implements ReplicaSelector {

    public ChainedDeclusteringSelector() {}

    public List<GridNode> selectReplica(GridTaskRouter router, GridNodeInfo masterNode, int numReplicas) {
        byte[] key = masterNode.toBytes();
        List<GridNode> nodes = router.listSuccessorNodes(key, false, numReplicas);
        return nodes;
    }

}
