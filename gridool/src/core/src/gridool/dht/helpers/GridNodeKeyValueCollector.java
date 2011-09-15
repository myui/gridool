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
package gridool.dht.helpers;

import gridool.GridNode;
import gridool.communication.payload.GridNodeInfo;
import gridool.dht.btree.CallbackHandler;
import gridool.dht.btree.Value;
import gridool.util.string.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridNodeKeyValueCollector implements CallbackHandler {

    private final Set<GridNode> includeValues;
    private final Map<String, List<GridNode>> mapping = new HashMap<String, List<GridNode>>(128);

    public GridNodeKeyValueCollector(Set<GridNode> includeValues) {
        if(includeValues == null) {
            throw new IllegalArgumentException();
        }
        this.includeValues = includeValues;
    }

    public Map<String, List<GridNode>> getMapping() {
        return mapping;
    }

    public boolean indexInfo(Value key, byte[] value) {
        byte[] keyData = key.getData();
        String path = StringUtils.toString(keyData);
        GridNodeInfo node = GridNodeInfo.fromBytes(value);

        if(includeValues.contains(node)) {//TODO REVIEWME What's preferable behavior when all mapped node is down?
            List<GridNode> nodes = mapping.get(path);
            if(nodes == null) {
                nodes = new ArrayList<GridNode>(32);
                mapping.put(path, nodes);
            }
            if(!nodes.contains(node)) {
                nodes.add(node);
            }
        }
        return true;
    }

    public boolean indexInfo(Value value, long pointer) {
        throw new UnsupportedOperationException();
    }

}