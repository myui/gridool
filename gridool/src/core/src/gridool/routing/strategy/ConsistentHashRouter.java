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
import gridool.GridNodeMetrics;
import gridool.discovery.DiscoveryEvent;
import gridool.routing.GridRouter;
import gridool.util.hashes.HashFunction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
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
public final class ConsistentHashRouter implements GridRouter {
    private static final long serialVersionUID = -3597764190794712761L;

    @Nonnull
    @GuardedBy("rwLock")
    private/* final */ConsistentHash consistentHash;
    @GuardedBy("rwLock")
    private int gridSize = 0;

    @Nonnull
    private transient/* final */ReadWriteLock rwLock;

    public ConsistentHashRouter() { // for Externalizable
        this.rwLock = new ReentrantReadWriteLock();
    }

    public ConsistentHashRouter(@Nonnull GridConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException();
        }
        HashFunction hasher = config.getHashFunction();
        int virtualNodes = config.getNumberOfVirtualNodes();
        this.consistentHash = new ConsistentHash(hasher, virtualNodes);
        this.rwLock = new ReentrantReadWriteLock();
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
            case metricsUpdate:
                GridNode resolved = resolve(node);
                GridNodeMetrics metrics = node.getMetrics();                
                resolved.setMetrics(metrics);
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

    public GridNode resolve(GridNode node) {
        return consistentHash.resolveNode(node);
    }

    public void resolve(final GridNode[] nodes) {
        if(nodes.length == 0) {
            return;
        }
        final int numNodes = nodes.length;
        for(int i = 0; i < numNodes; i++) {
            GridNode n = nodes[i];
            GridNode resolved = consistentHash.resolveNode(n);
            if(resolved != null) {
                nodes[i] = resolved;
            }
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.consistentHash = (ConsistentHash) in.readObject();
        this.gridSize = in.readInt();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(consistentHash);
        out.writeInt(gridSize);
    }

}
