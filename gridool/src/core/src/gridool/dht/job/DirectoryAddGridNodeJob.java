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

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.dht.helpers.DirectoryTaskAdapter;
import gridool.dht.ops.AddGridNodeOperation;
import gridool.routing.GridRouter;
import gridool.util.GridUtils;
import gridool.util.collections.CollectionUtils;
import gridool.util.collections.UnmodifiableJointList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
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
public final class DirectoryAddGridNodeJob extends GridJobBase<AddGridNodeOperation, Serializable> {
    private static final long serialVersionUID = 5629533496790915661L;

    public DirectoryAddGridNodeJob() {
        super();
    }

    @Override
    public boolean isAsyncOps() {
        return true;
    }

    @SuppressWarnings("unchecked")
    public Map<GridTask, GridNode> map(GridRouter router, AddGridNodeOperation ops)
            throws GridException {
        final int gridSize = router.getGridSize();
        final GridNode[] allNodes = router.getAllNodes();
        final List<GridNode> superNodes = GridUtils.selectSuperNodes(allNodes);

        final Map<GridNode, List<byte[]>> nodeKeysMap = new HashMap<GridNode, List<byte[]>>(gridSize);
        final byte[][] keys = ops.getKeys();
        for(byte[] key : keys) {
            final List<GridNode> nodes = router.selectNodes(key);
            if(nodes.isEmpty()) {
                throw new GridException("Could not find any grid node.");
            }
            final List<GridNode> jointList = new UnmodifiableJointList<GridNode>(superNodes, nodes);
            final Collection<GridNode> uniqNodes = CollectionUtils.eliminateDuplication(jointList, false);
            for(GridNode node : uniqNodes) {
                List<byte[]> mappedKeys = nodeKeysMap.get(node);
                if(mappedKeys == null) {
                    mappedKeys = new ArrayList<byte[]>(32);
                    nodeKeysMap.put(node, mappedKeys);
                }
                mappedKeys.add(key);
            }
        }

        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodeKeysMap.size());
        for(Map.Entry<GridNode, List<byte[]>> entry : nodeKeysMap.entrySet()) {
            GridNode node = entry.getKey();
            List<byte[]> mappedKeysList = entry.getValue();
            assert (!mappedKeysList.isEmpty());
            byte[][] mappedKeys = new byte[mappedKeysList.size()][];
            mappedKeysList.toArray(mappedKeys);

            AddGridNodeOperation shrinkedOps = ops.makeOperation(mappedKeys);
            GridTask task = new DirectoryTaskAdapter(this, shrinkedOps);
            map.put(task, node);
        }

        return map;
    }

    public Serializable reduce() throws GridException {
        throw new IllegalStateException("Response to an asynchronous job is illegally accepted.");
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        throw new IllegalStateException("Response to an asynchronous job is illegally accepted.");
    }

}
