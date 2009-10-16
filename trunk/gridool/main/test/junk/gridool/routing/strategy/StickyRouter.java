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
package gridool.routing.strategy;

import java.util.concurrent.locks.Lock;

import javax.annotation.Nonnull;

import gridool.GridConfiguration;
import gridool.GridErrorDescription;
import gridool.GridNode;
import gridool.GridRuntimeException;
import gridool.GridTask;
import gridool.routing.GridNodeSelector;
import gridool.routing.SimpleTaskRouterBase;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class StickyRouter extends SimpleTaskRouterBase {

    public StickyRouter(@Nonnull GridConfiguration config) {
        super(config);
    }

    @Override
    @Nonnull
    public GridNode selectNode(@Nonnull GridTask task) {
        final Lock rlock = rwLock.readLock();
        rlock.lock();
        GridNode node;
        try {
            node = nodeMap.get(task);
            if(node == null) {
                GridNodeSelector selector = config.getNodeSelector();
                node = selector.selectNode(nodeList, task, config);
            }
        } finally {
            rlock.unlock();
        }
        if(node == null) {
            // There should be at least one node, i.e., local node, in the gird.
            throw new GridRuntimeException(GridErrorDescription.NODE_NOT_FOUND);
        }
        return node;
    }
}
