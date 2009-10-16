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
package gridool.dht.job;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.construct.GridJobBase;
import gridool.dht.ops.DHTOperation;
import gridool.dht.ops.KeyOperation;
import gridool.routing.GridTaskRouter;
import xbird.util.collections.CollectionUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class SingleKeyDHTJob<R extends Serializable> extends GridJobBase<DHTOperation, R> {
    private static final long serialVersionUID = -9187339618073498138L;

    public SingleKeyDHTJob() {
        super();
    }

    @Override
    public Map<GridTask, GridNode> map(GridTaskRouter router, DHTOperation ops)
            throws GridException {
        setAsyncOps(ops.isAsyncOps());

        KeyOperation keyOps = (KeyOperation) ops;
        String mapKey = keyOps.getKey();

        List<GridNode> nodes = router.selectNodes(mapKey);
        Collection<GridNode> uniqueNodes = (nodes.size() <= 1) ? nodes
                : CollectionUtils.eliminateDuplication(nodes, true);

        final Map<GridTask, GridNode> map = new HashMap<GridTask, GridNode>(uniqueNodes.size());
        for(GridNode mappedNode : uniqueNodes) {
            GridTask task = new DHTTaskAdapter(this, ops);
            map.put(task, mappedNode);
        }

        return map;
    }

}
