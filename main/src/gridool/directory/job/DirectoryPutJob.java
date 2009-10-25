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
import gridool.directory.ops.PutOperation;
import gridool.routing.GridTaskRouter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class DirectoryPutJob extends GridJobBase<PutOperation, Serializable> {
    private static final long serialVersionUID = -1002571879285570222L;

    private boolean isAsyncOps = false;

    public DirectoryPutJob() {
        super();
    }

    @Override
    public boolean isAsyncOps() {
        return isAsyncOps;
    }

    public Map<GridTask, GridNode> map(GridTaskRouter router, PutOperation ops)
            throws GridException {
        this.isAsyncOps = ops.isAsyncOps();

        final byte[][] keys = ops.getKeys();
        final byte[][] values = ops.getValues();

        final int length = keys.length;
        final Map<GridNode, Pair<List<byte[]>, List<byte[]>>> nodeAssignMap = new HashMap<GridNode, Pair<List<byte[]>, List<byte[]>>>(length);
        for(int i = 0; i < length; i++) {
            final byte[] k = keys[i];
            final byte[] v = values[i];

            final List<GridNode> nodes = router.selectNodes(k);
            if(nodes.isEmpty()) {
                throw new GridException("Could not find any grid node.");
            }
            for(GridNode node : nodes) {
                Pair<List<byte[]>, List<byte[]>> mappedKeys = nodeAssignMap.get(node);
                final List<byte[]> keyList;
                final List<byte[]> valueList;
                if(mappedKeys == null) {
                    keyList = new ArrayList<byte[]>();
                    valueList = new ArrayList<byte[]>();
                    mappedKeys = new Pair<List<byte[]>, List<byte[]>>(keyList, valueList);
                    nodeAssignMap.put(node, mappedKeys);
                } else {
                    keyList = mappedKeys.getFirst();
                    valueList = mappedKeys.getSecond();
                }
                keyList.add(k);
                valueList.add(v);
            }
        }

        int gridSize = nodeAssignMap.size();
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(gridSize);
        for(Map.Entry<GridNode, Pair<List<byte[]>, List<byte[]>>> entry : nodeAssignMap.entrySet()) {
            GridNode node = entry.getKey();
            Pair<List<byte[]>, List<byte[]>> mapped = entry.getValue();

            final PutOperation shrinkedOps = ops.makeOperation(mapped.getFirst(), mapped.getSecond());
            GridTask task = new DirectoryTaskAdapter(this, shrinkedOps);
            map.put(task, node);
        }
        return map;
    }

    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public Serializable reduce() throws GridException {
        return null;
    }

}
