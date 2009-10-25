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

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.construct.GridJobBase;
import gridool.dht.ops.MultiKeyOperation;
import gridool.routing.GridTaskRouter;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class MultiKeyDHTJob<R extends Serializable> extends
        GridJobBase<MultiKeyOperation, R> {
    private static final long serialVersionUID = -1369202000937917718L;

    public MultiKeyDHTJob() {
        super();
    }

    @Override
    public Map<GridTask, GridNode> map(GridTaskRouter router, MultiKeyOperation ops)
            throws GridException {
        final String[] keys = ops.getKeys();

        final Map<GridNode, List<String>> nodeKeysMap = new HashMap<GridNode, List<String>>(keys.length);
        for(String key : keys) {
            final List<GridNode> nodes = router.selectNodes(key);
            if(nodes.isEmpty()) {
                throw new GridException("Could not find any grid node.");
            }
            for(GridNode node : nodes) {
                List<String> mappedKeys = nodeKeysMap.get(node);
                if(mappedKeys == null) {
                    mappedKeys = new LinkedList<String>();
                    nodeKeysMap.put(node, mappedKeys);
                }
                mappedKeys.add(key);
            }
        }

        final Map<GridTask, GridNode> map = new HashMap<GridTask, GridNode>(router.getGridSize());
        for(Map.Entry<GridNode, List<String>> entry : nodeKeysMap.entrySet()) {
            GridNode node = entry.getKey();
            List<String> mappedKeysList = entry.getValue();
            assert (!mappedKeysList.isEmpty());
            String[] mappedKeys = new String[mappedKeysList.size()];
            mappedKeysList.toArray(mappedKeys);

            MultiKeyOperation shrinkedOps = ops.makeOperation(mappedKeys);
            GridTask task = new DHTTaskAdapter(this, shrinkedOps);
            map.put(task, node);
        }

        return map;
    }

}
