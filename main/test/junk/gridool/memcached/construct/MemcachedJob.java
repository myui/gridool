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
package gridool.memcached.construct;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.construct.GridJobBase;
import gridool.memcached.ops.MemcachedOperation;
import gridool.routing.GridTaskRouter;
import xbird.util.collections.CollectionUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class MemcachedJob<T extends Serializable> extends
        GridJobBase<MemcachedOperation, T> {
    private static final long serialVersionUID = 4320825006356677064L;

    private boolean readOnlyOps = false;
    
    public MemcachedJob() {
        super();
    }

    protected boolean isReadOnlyOps() {
        return readOnlyOps;
    }

    protected void setReadOnlyOps(boolean readOnlyOps) {
        this.readOnlyOps = readOnlyOps;
    }

    @Override
    public Map<GridTask, GridNode> map(GridTaskRouter router, MemcachedOperation ops)
            throws GridException {
        setAsyncOps(ops.isAsyncOps());
        setReadOnlyOps(ops.isReadOnlyOps());

        String mapKey = ops.getKey();
        int replicas = ops.getNumberOfReplicas();
        int maxSelect = 1 + replicas;

        List<GridNode> nodes = router.selectNodes(mapKey, maxSelect);
        Collection<GridNode> uniqueNodes = (nodes.size() <= 1) ? nodes
                : CollectionUtils.eliminateDuplication(nodes, true);

        final Map<GridTask, GridNode> map = new HashMap<GridTask, GridNode>(uniqueNodes.size());
        for(GridNode mappedNode : uniqueNodes) {
            GridTask task = new MemcachedTaskAdapter(this, ops);
            map.put(task, mappedNode);
        }

        return map;
    }

}
