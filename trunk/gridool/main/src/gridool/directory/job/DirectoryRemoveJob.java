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
import gridool.directory.ops.RemoveOperation;
import gridool.routing.GridTaskRouter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DirectoryRemoveJob extends GridJobBase<RemoveOperation, Boolean> {
    private static final long serialVersionUID = -7069740940214736156L;

    private boolean isAsyncOps = false;
    private boolean succeed = true;

    public DirectoryRemoveJob() {}

    @Override
    public boolean isAsyncOps() {
        return isAsyncOps;
    }

    public Map<GridTask, GridNode> map(GridTaskRouter router, RemoveOperation ops)
            throws GridException {
        this.isAsyncOps = ops.isAsyncOps();

        final Map<GridNode, List<byte[]>> nodeMap = new HashMap<GridNode, List<byte[]>>(router.getGridSize());
        final byte[][] keys = ops.getKeys();
        for(byte[] key : keys) {
            List<GridNode> nodes = router.selectNodes(key);
            for(GridNode node : nodes) {
                List<byte[]> mappedKeys = nodeMap.get(key);
                if(mappedKeys == null) {
                    mappedKeys = new ArrayList<byte[]>(8);
                    nodeMap.put(node, mappedKeys);
                }
                mappedKeys.add(key);
            }
        }

        final Map<GridTask, GridNode> taskMap = new IdentityHashMap<GridTask, GridNode>(nodeMap.size());
        for(Map.Entry<GridNode, List<byte[]>> e : nodeMap.entrySet()) {
            GridNode node = e.getKey();
            List<byte[]> v = e.getValue();
            RemoveOperation shrinkedOps = ops.makeOperation(v);
            GridTask task = new DirectoryTaskAdapter(this, shrinkedOps);
            taskMap.put(task, node);
        }
        return taskMap;
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        final Boolean status = result.getResult();
        if(status == null || !status.booleanValue()) {
            this.succeed = false;
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    public Boolean reduce() throws GridException {
        return succeed;
    }

}
