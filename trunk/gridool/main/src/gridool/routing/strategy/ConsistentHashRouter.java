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

import gridool.GridConfiguration;
import gridool.GridNode;
import gridool.discovery.DiscoveryEvent;
import gridool.routing.GridTaskRouter;
import gridool.util.ConsistentHash;
import gridool.util.HashFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
@ThreadSafe
public final class ConsistentHashRouter implements GridTaskRouter {

    @Nonnull
    @GuardedBy("rwLock")
    private final ConsistentHash consistentHash;

    @GuardedBy("rwLock")
    private int gridSize = 0;

    @Nonnull
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public ConsistentHashRouter(@Nonnull GridConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException();
        }
        HashFunction hasher = config.getHashFunction();
        int virtualNodes = config.getNumberOfVirtualNodes();
        this.consistentHash = new ConsistentHash(hasher, virtualNodes);
    }

    public int getGridSize() {
        return gridSize;
    }

    public void onDiscovery(@Nonnull DiscoveryEvent event, @Nonnull GridNode node) {
        final Lock wlock = rwLock.writeLock();
        switch(event) {
            case join:
                wlock.lock();
                consistentHash.add(node);
                gridSize++;
                wlock.unlock();
                break;
            case leave:
                wlock.lock();
                consistentHash.remove(node);
                gridSize--;
                wlock.unlock();
                break;
            case dropout:
                // TODO
            default:
                break;
        }
    }

    public GridNode[] getAllNodes() {
        final Lock rlock = rwLock.readLock();
        rlock.lock();
        try {
            return consistentHash.getAll();
        } finally {
            rlock.unlock();
        }
    }

    @Nonnull
    public GridNode selectNode(@Nonnull byte[] key) {
        final GridNode node;
        final Lock rlock = rwLock.readLock();
        rlock.lock();
        try {
            node = consistentHash.get(key);
        } finally {
            rlock.unlock();
        }
        return node;
    }

    @Deprecated
    public List<GridNode> selectNodes(byte[] key) {
        return listSuccessorNodesInVirtualChain(key, Integer.MAX_VALUE, true);
    }

    @Nonnull
    public List<GridNode> listSuccessorNodesInVirtualChain(byte[] key, int maxNumSelect, boolean inclusive) {
        final Lock rlock = rwLock.readLock();
        rlock.lock();
        try {
            return consistentHash.listSuccessorsInVirtualChain(key, maxNumSelect, !inclusive);
        } finally {
            rlock.unlock();
        }
    }

    public List<GridNode> listSuccessorNodesInPhysicalChain(GridNode node, int maxNodesToSelect, boolean inclusive) {
        final Lock rlock = rwLock.readLock();
        rlock.lock();
        try {
            return consistentHash.listSuccessorsInPhysicalChain(node, maxNodesToSelect, !inclusive);
        } finally {
            rlock.unlock();
        }
    }

    public void onChannelClosed() {
        final Lock wlock = rwLock.writeLock();
        wlock.lock();
        try {
            consistentHash.clear();
            this.gridSize = 0;
        } finally {
            wlock.unlock();
        }
    }

    public void resolve(final GridNode[] nodes) {
        if(nodes.length == 0) {
            return;
        }
        final GridNode[] curNodes = getAllNodes();
        final Map<String, GridNode> map = new HashMap<String, GridNode>(curNodes.length);
        for(final GridNode node : curNodes) {
            map.put(node.getKey(), node);
        }
        final int numNodes = nodes.length;
        for(int i = 0; i < numNodes; i++) {
            GridNode n = nodes[i];
            String nodeid = n.getKey();
            GridNode curNode = map.get(nodeid);
            if(curNode != null) {
                nodes[i] = curNode;
            }
        }
    }

}
