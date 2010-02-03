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
import gridool.directory.ops.AddOperation;
import gridool.directory.ops.DestinatedAddOperation;
import gridool.routing.GridTaskRouter;

import java.io.Serializable;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class DirectoryDestinatedAddJob extends
        GridJobBase<DestinatedAddOperation, Serializable> {
    private static final long serialVersionUID = -8182005314124929776L;

    public DirectoryDestinatedAddJob() {}

    public Map<GridTask, GridNode> map(GridTaskRouter router, DestinatedAddOperation ops)
            throws GridException {
        final GridNode[] nodes = ops.getDestinations();
        assert (nodes != null);
        assert (nodes.length > 0);
        if(nodes.length == 1) {
            return singleMap(ops, nodes[0]);
        } else {
            return multiMap(ops, nodes);
        }
    }

    private Map<GridTask, GridNode> singleMap(final DestinatedAddOperation ops, final GridNode dstNode) {
        final byte[][] keys = ops.getKeys();
        if(keys.length == 0) {
            return Collections.emptyMap();
        }
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(1);
        map.put(new DirectoryTaskAdapter(this, ops), dstNode);
        return map;
    }

    private Map<GridTask, GridNode> multiMap(final DestinatedAddOperation ops, final GridNode[] dstNodes) {
        final byte[][] keys = ops.getKeys();
        if(keys.length == 0) {
            return Collections.emptyMap();
        }

        final String idxName = ops.getName();
        final byte[][] values = ops.getValues();
        assert (keys.length == values.length);

        int numNodes = dstNodes.length;
        final int numMappings = keys.length;
        assert (numNodes > 0);
        final int mappingsPerNode = numMappings / numNodes;

        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(numNodes);
        final int lastIndex = numNodes - 1;
        for(int i = 0; i < lastIndex; i++) {
            final byte[][] k = new byte[mappingsPerNode][];
            final byte[][] v = new byte[mappingsPerNode][];
            System.arraycopy(keys, i, k, 0, mappingsPerNode);
            System.arraycopy(values, i, v, 0, mappingsPerNode);
            AddOperation shrinkedOps = new AddOperation(idxName, k, v);
            GridTask task = new DirectoryTaskAdapter(this, shrinkedOps);
            map.put(task, dstNodes[i]);
        }
        {
            int last = lastIndex * mappingsPerNode;
            int rest = numMappings - last;
            final byte[][] k = new byte[rest][];
            final byte[][] v = new byte[rest][];
            System.arraycopy(keys, last, k, 0, rest);
            System.arraycopy(values, last, v, 0, rest);
            AddOperation shrinkedOps = new AddOperation(idxName, k, v);
            GridTask task = new DirectoryTaskAdapter(this, shrinkedOps);
            map.put(task, dstNodes[lastIndex]);
        }
        return map;
    }

    public Serializable reduce() throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        return null;
    }

}
