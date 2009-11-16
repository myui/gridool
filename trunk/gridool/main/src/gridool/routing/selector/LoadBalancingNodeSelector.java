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
package gridool.routing.selector;

import gridool.GridConfiguration;
import gridool.GridNode;
import gridool.loadblancing.GridLoadProbe;
import gridool.routing.GridNodeSelector;

import java.util.LinkedList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class LoadBalancingNodeSelector implements GridNodeSelector {

    public LoadBalancingNodeSelector() {}

    @Nullable
    public GridNode selectNode(@Nonnull List<GridNode> nodeList, @Nullable byte[] key, @Nonnull GridConfiguration config) {
        final int size = nodeList.size();
        if(size == 0) {
            return null;
        } else if(size == 1) {
            return nodeList.get(0);
        }

        final GridLoadProbe probe = config.getProbe();
        GridNode node = null;
        float minLoad = Float.MAX_VALUE;
        for(int i = 0; i < size; i++) {
            GridNode n = nodeList.get(i);
            float load = probe.getLoad(n);
            if(load < minLoad) {
                node = n;
                minLoad = load;
            }
        }
        return node;
    }

    public List<GridNode> sortNodes(@Nonnull List<GridNode> nodeList, @Nonnull byte[] key, @Nonnull GridConfiguration config) {
        if(nodeList.isEmpty()) {
            return nodeList;
        }

        final LinkedList<GridNode> sortedNodes = new LinkedList<GridNode>();
        final GridLoadProbe probe = config.getProbe();

        float minLoad = Float.MAX_VALUE;
        for(GridNode n : nodeList) {
            float load = probe.getLoad(n);
            if(load < minLoad) {
                sortedNodes.addFirst(n);
                minLoad = load;
            } else {
                sortedNodes.addLast(n);
            }
        }
        return sortedNodes;
    }

}
