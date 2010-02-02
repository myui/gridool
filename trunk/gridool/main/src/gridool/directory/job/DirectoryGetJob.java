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
package gridool.directory.job;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.directory.helpers.DirectoryTaskAdapter;
import gridool.directory.ops.GetOperation;
import gridool.routing.GridTaskRouter;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class DirectoryGetJob extends GridJobBase<GetOperation, byte[][]> {
    private static final long serialVersionUID = 4819068754734630653L;

    private transient byte[][] firstResult = null;

    public DirectoryGetJob() {}

    public Map<GridTask, GridNode> map(GridTaskRouter router, GetOperation ops)
            throws GridException {
        byte[][] keys = ops.getKeys();
        assert (keys.length == 1);
        byte[] key = keys[0];
        assert (key != null);
        
        GridTask task = new DirectoryTaskAdapter(this, ops);
        List<GridNode> nodes = router.listSuccessorNodesInVirtualChain(key, 1, true);
        assert (!nodes.isEmpty());
        GridNode node = nodes.get(0);
        if(node == null) {
            throw new GridException("Could not find any grid node.");
        }
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(1);
        map.put(task, node);
        return map;
    }

    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        final byte[][] res = result.getResult();
        if(res == null) {
            return GridTaskResultPolicy.FAILOVER;
        }
        this.firstResult = res;
        return GridTaskResultPolicy.RETURN;
    }

    public byte[][] reduce() throws GridException {
        return firstResult;
    }

}
