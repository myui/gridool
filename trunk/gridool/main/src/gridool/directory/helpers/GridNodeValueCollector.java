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
package gridool.directory.helpers;

import gridool.GridNode;
import gridool.communication.payload.GridNodeInfo;

import java.util.ArrayList;
import java.util.List;

import xbird.storage.index.BTreeCallback;
import xbird.storage.index.Value;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class GridNodeValueCollector implements BTreeCallback {

    private final List<GridNode> exclude;
    private final List<GridNode> nodes = new ArrayList<GridNode>(8);

    public GridNodeValueCollector(List<GridNode> exclude) {
        this.exclude = exclude;
    }

    public List<GridNode> getMatched() {
        return nodes;
    }

    public boolean indexInfo(Value key, byte[] value) {
        final GridNodeInfo node = GridNodeInfo.fromBytes(value);
        if(!exclude.contains(node)) {
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