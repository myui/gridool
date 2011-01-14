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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.memcached.ops.GetAllOperation;
import gridool.memcached.ops.MemcachedOperation;
import gridool.routing.GridTaskRouter;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GetMultiJob extends MemcachedJob<HashMap<String, Serializable>> {
    private static final long serialVersionUID = 3528913827027187720L;

    private final LinkedHashMap<String, Serializable> orderedResult;

    public GetMultiJob() {
        super();
        this.orderedResult = new LinkedHashMap<String, Serializable>();
    }

    @Override
    public Map<GridTask, GridNode> map(GridTaskRouter router, MemcachedOperation ops)
            throws GridException {
        setAsyncOps(ops.isAsyncOps());
        //setReadOnlyOps(ops.isReadOnlyOps());

        GetAllOperation multiOps = (GetAllOperation) ops;
        final String[] keys = multiOps.getKeys();

        final Map<GridNode, List<String>> nodeKeysMap = new HashMap<GridNode, List<String>>(keys.length);
        for(String key : keys) {
            final List<GridNode> nodes = router.selectNodes(key, 1);
            if(nodes.isEmpty()) {
                throw new GridException("Could not find any grid node.");
            }
            GridNode node = nodes.get(0);
            List<String> mappedKeys = nodeKeysMap.get(key);
            if(mappedKeys == null) {
                mappedKeys = new LinkedList<String>();
                nodeKeysMap.put(node, mappedKeys);
            }
            mappedKeys.add(key);
        }

        final Map<GridTask, GridNode> map = new HashMap<GridTask, GridNode>(keys.length);
        for(Map.Entry<GridNode, List<String>> entry : nodeKeysMap.entrySet()) {
            GridNode node = entry.getKey();
            List<String> mappedKeysList = entry.getValue();
            assert (!mappedKeysList.isEmpty());
            String[] mappedKeys = new String[mappedKeysList.size()];
            mappedKeysList.toArray(mappedKeys);

            GetAllOperation getOps = MemcachedOperationFactory.createGetAllOperation(mappedKeys);
            GridTask task = new MemcachedTaskAdapter(this, getOps);
            map.put(task, node);
        }
        return map;
    }

    @Override
    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        Serializable res = result.getResult();
        if(result != null) {
            String taskId = task.getTaskId();
            orderedResult.put(taskId, res);
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    @Override
    public HashMap<String, Serializable> reduce() throws GridException {
        return orderedResult;
    }

}
