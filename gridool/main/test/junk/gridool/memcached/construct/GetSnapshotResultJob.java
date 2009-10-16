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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.memcached.cache.MCElementSnapshot;
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
public final class GetSnapshotResultJob extends MemcachedJob<MCElementSnapshot> {
    private static final long serialVersionUID = 2248305991638057642L;

    private long latestVersion = Long.MIN_VALUE;
    private MCElementSnapshot snapshot = null;

    public GetSnapshotResultJob() {
        super();
    }

    @Override
    public Map<GridTask, GridNode> map(GridTaskRouter router, MemcachedOperation ops)
            throws GridException {
        String mapKey = ops.getKey();
        final List<GridNode> nodes = router.selectNodes(mapKey);
        Collection<GridNode> uniqueNodes = (nodes.size() <= 1) ? nodes
                : CollectionUtils.eliminateDuplication(nodes, true);

        final Map<GridTask, GridNode> map = new HashMap<GridTask, GridNode>(uniqueNodes.size());
        for(GridNode mappedNode : uniqueNodes) {
            GridTask task = new MemcachedTaskAdapter(this, ops);
            map.put(task, mappedNode);
        }
        return map;
    }

    @Override
    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        if(snapshot != null) {
            final MCElementSnapshot res = result.getResult();
            if(res != null) {
                if(res.isLockAcquired()) {                    
                    return GridTaskResultPolicy.RETURN;
                }
                final long resVersion = res.getVersion();
                if(resVersion > latestVersion) {
                    GridNode residentNode = result.getExecutedNode();
                    res.setResidentNode(residentNode);
                    this.snapshot = res;
                }
            }
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    @Override
    public MCElementSnapshot reduce() throws GridException {

        return snapshot;
    }

}
