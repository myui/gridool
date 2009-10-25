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
import gridool.GridErrorDescription;
import gridool.GridNode;
import gridool.GridRuntimeException;
import gridool.GridTask;
import gridool.discovery.DiscoveryEvent;
import gridool.routing.GridNodeSelector;
import gridool.routing.GridTaskRouter;
import gridool.util.ConsistentHash;
import gridool.util.HashFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import xbird.util.iterator.IteratorUtils;

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
    private final GridConfiguration config;

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
        this.config = config;
        HashFunction hasher = config.getHashFunction();
        int virtualNodes = config.getNumberOfVirtualNodes();
        this.consistentHash = new ConsistentHash(hasher, virtualNodes);
    }

    public GridNodeSelector getNodeSelector() {
        return config.getNodeSelector();
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
                wlock.unlock();
                gridSize++;
                break;
            case leave:
                wlock.lock();
                consistentHash.remove(node);
                wlock.unlock();
                gridSize--;
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
            return consistentHash.getNodes();
        } finally {
            rlock.unlock();
        }
    }

    public List<GridNode> getNodes(int maxNodesToSelect) {
        GridNode[] nodes = getAllNodes();
        List<GridNode> nodeList = Arrays.asList(nodes);
        List<GridNode> uniqList = IteratorUtils.toListUnique(nodeList.iterator());
        final int count = uniqList.size();
        if(count > maxNodesToSelect) {
            uniqList = uniqList.subList(0, maxNodesToSelect);
        }
        GridNodeSelector selector = config.getNodeSelector();
        List<GridNode> sortedNodes = selector.sortNodes(uniqList, null, config);
        return sortedNodes;
    }

    @Nonnull
    public GridNode selectNode(@Nonnull GridTask task) {
        final GridNodeSelector selector = config.getNodeSelector();

        final List<GridNode> nodes;
        final Lock rlock = rwLock.readLock();
        rlock.lock();
        try {
            Iterator<GridNode> itor = consistentHash.getAll(task);
            nodes = IteratorUtils.toListUnique(itor);
        } finally {
            rlock.unlock();
        }

        final GridNode node = selector.selectNode(nodes, task, config);
        if(node == null) {// There should be at least one node, i.e., local node, in the gird.
            throw new GridRuntimeException(GridErrorDescription.NODE_NOT_FOUND);
        }
        return node;
    }

    public List<GridNode> selectNodes(byte[] key) {
        return selectNodes(key, Integer.MAX_VALUE);
    }

    public List<GridNode> selectNodes(byte[] key, int maxNumSelect) {
        final GridNodeSelector selector = config.getNodeSelector();
        final int replicas = consistentHash.getNumberOfVirtualNodes();
        final int numSelect = Math.min(maxNumSelect, replicas);

        List<GridNode> nodes;
        final Lock rlock = rwLock.readLock();
        rlock.lock();
        try {
            Iterator<GridNode> itor = consistentHash.getAll(key);
            nodes = IteratorUtils.toListUnique(itor);
        } finally {
            rlock.unlock();
        }
        if(nodes.isEmpty()) {
            throw new GridRuntimeException(GridErrorDescription.NODE_NOT_FOUND);
        }

        final int count = nodes.size();
        if(count > maxNumSelect) {
            nodes = nodes.subList(0, numSelect);
        }

        List<GridNode> sortedNodes = selector.sortNodes(nodes, key, config);
        return sortedNodes;
    }

    public void onChannelClosed() {
        final Lock wlock = rwLock.writeLock();
        wlock.lock();
        try {
            consistentHash.clear();
        } finally {
            wlock.unlock();
        }
    }
}
