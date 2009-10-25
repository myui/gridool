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
package gridool.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import gridool.GridConfiguration;
import gridool.GridNode;
import gridool.discovery.DiscoveryEvent;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class SimpleTaskRouterBase implements GridTaskRouter {

    @Nonnull
    protected final GridConfiguration config;

    @Nonnull
    @GuardedBy("rwLock")
    protected final Map<String, GridNode> nodeMap;

    @Nonnull
    @GuardedBy("rwLock")
    protected final List<GridNode> nodeList;

    @Nonnull
    protected final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public SimpleTaskRouterBase(@Nonnull GridConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException();
        }
        this.config = config;
        this.nodeMap = new HashMap<String, GridNode>();
        this.nodeList = new ArrayList<GridNode>();
    }

    @Override
    public void onDiscovery(@Nonnull DiscoveryEvent event, @Nonnull GridNode node) {
        final String nodeId = node.getIdentifier();

        final Lock wlock = rwLock.writeLock();
        wlock.lock();
        try {
            switch(event) {
                case join:
                    if(nodeMap.put(nodeId, node) == null) {
                        nodeList.add(node);
                    }
                    break;
                case leave:
                    if(nodeMap.remove(nodeId) != null) {
                        nodeList.remove(node);
                    }
                    break;
                default:
                    assert false;
            }
        } finally {
            wlock.unlock();
        }
    }

    @Override
    public void onChannelClosed() {
        final Lock wlock = rwLock.writeLock();
        wlock.lock();
        try {
            nodeMap.clear();
            nodeList.clear();
        } finally {
            wlock.unlock();
        }
    }

}
