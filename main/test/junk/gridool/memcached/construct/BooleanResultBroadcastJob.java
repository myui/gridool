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

import java.util.HashMap;
import java.util.Map;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.memcached.ops.MemcachedOperation;
import gridool.routing.GridTaskRouter;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class BooleanResultBroadcastJob extends BooleanResultJob {
    private static final long serialVersionUID = -8563168729687366344L;

    public BooleanResultBroadcastJob() {
        super();
    }

    @Override
    public Map<GridTask, GridNode> map(GridTaskRouter router, MemcachedOperation ops)
            throws GridException {
        setAsyncOps(ops.isAsyncOps());
        setReadOnlyOps(ops.isReadOnlyOps());

        GridNode[] nodes = router.getAllNodes();
        assert (nodes.length > 0);

        final Map<GridTask, GridNode> map = new HashMap<GridTask, GridNode>(nodes.length);
        for(GridNode node : nodes) {
            GridTask task = new MemcachedTaskAdapter(this, ops);
            map.put(task, node);
        }

        return map;
    }
}
