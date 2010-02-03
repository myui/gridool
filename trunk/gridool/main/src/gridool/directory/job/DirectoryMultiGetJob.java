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
import gridool.directory.ops.MultiGetOperation;
import gridool.routing.GridTaskRouter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import xbird.util.struct.ByteArray;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class DirectoryMultiGetJob extends
        GridJobBase<MultiGetOperation, HashMap<ByteArray, ArrayList<byte[]>>> {
    private static final long serialVersionUID = 4819068754734630653L;

    private transient HashMap<ByteArray, ArrayList<byte[]>> resultMap = null;

    public DirectoryMultiGetJob() {}

    public Map<GridTask, GridNode> map(GridTaskRouter router, MultiGetOperation ops)
            throws GridException {
        int gridsize = router.getGridSize();

        final Map<GridNode, List<byte[]>> nodeMap = new HashMap<GridNode, List<byte[]>>(gridsize);
        final byte[][] keys = ops.getKeys();
        for(byte[] k : keys) {
            final List<GridNode> nodes = router.listSuccessorNodesInVirtualChain(k, 1, true);
            if(nodes.isEmpty()) {
                throw new GridException("Could not find any grid node for key: "
                        + String.valueOf(k));
            }
            GridNode node = nodes.get(0);
            List<byte[]> mappedKeys = nodeMap.get(node);
            if(mappedKeys == null) {
                mappedKeys = new ArrayList<byte[]>(12);
                nodeMap.put(node, mappedKeys);
            }
            mappedKeys.add(k);
        }

        final String idxName = ops.getIndexName();
        final Map<GridTask, GridNode> taskMap = new IdentityHashMap<GridTask, GridNode>(nodeMap.size());
        for(Map.Entry<GridNode, List<byte[]>> entry : nodeMap.entrySet()) {
            GridNode node = entry.getKey();
            List<byte[]> keylist = entry.getValue();
            MultiGetOperation shrinkedOps = new MultiGetOperation(idxName, keylist);
            GridTask task = new DirectoryTaskAdapter(this, shrinkedOps);
            taskMap.put(task, node);
        }

        this.resultMap = new HashMap<ByteArray, ArrayList<byte[]>>(keys.length);
        return taskMap;
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        final Pair<byte[], byte[][]>[] results = result.getResult();
        if(results == null) {
            return GridTaskResultPolicy.FAILOVER;
        }
        final HashMap<ByteArray, ArrayList<byte[]>> map = resultMap;
        for(Pair<byte[], byte[][]> p : results) {
            final byte[][] values = p.getSecond();
            if(values.length == 0) {
                continue;
            }
            final byte[] k = p.getFirst();
            assert (k != null);
            ArrayList<byte[]> vlist = map.get(k);
            if(vlist == null) {
                vlist = new ArrayList<byte[]>(values.length + 4);
                map.put(new ByteArray(k), vlist);
            }
            for(byte[] v : values) {
                vlist.add(v);
            }
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    public HashMap<ByteArray, ArrayList<byte[]>> reduce() throws GridException {
        return resultMap;
    }

}
