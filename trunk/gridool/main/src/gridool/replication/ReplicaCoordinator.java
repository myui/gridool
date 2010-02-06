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
package gridool.replication;

import gridool.GridNode;
import gridool.communication.payload.GridNodeInfo;
import gridool.discovery.DiscoveryEvent;
import gridool.discovery.GridDiscoveryListener;
import gridool.replication.jobs.CoordinateReplicaJobConf;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class ReplicaCoordinator implements GridDiscoveryListener {

    @Nonnull
    private final GridNodeInfo localNode;
    
    private final List<ReplicaCoordinatorListener> listeners;
    private final Lock rlock;
    private final Lock wlock;

    public ReplicaCoordinator(@CheckForNull GridNodeInfo localNode) {
        if(localNode == null) {
            throw new IllegalArgumentException();
        }
        this.localNode = localNode;
        this.listeners = new LinkedList<ReplicaCoordinatorListener>();
        ReadWriteLock rwlock = new ReentrantReadWriteLock();
        this.rlock = rwlock.readLock();
        this.wlock = rwlock.writeLock();
    }

    void addListeners(ReplicaCoordinatorListener... newListeners) {
        wlock.lock();
        try {
            for(ReplicaCoordinatorListener listener : newListeners) {
                if(!listeners.add(listener)) {
                    assert false : "listner already defined: " + listener;
                }
            }
        } finally {
            wlock.unlock();
        }
    }

    public void configureReplica(@Nonnull GridNodeInfo masterNode, @Nonnull List<GridNode> replicas, @Nonnull CoordinateReplicaJobConf jobConf) {
        final List<GridNode> oldReplicas = masterNode.getReplicas();

        rlock.lock();
        try {
            for(ReplicaCoordinatorListener listener : listeners) {
                listener.onConfigureReplica(masterNode, oldReplicas, replicas, jobConf);
            }
        } finally {
            rlock.unlock();
        }

        masterNode.setReplicas(replicas);
        // TODO notify other nodes.
    }

    // ----------------------------------------------------
    // GridDiscoveryListener implementation

    public void onDiscovery(DiscoveryEvent event, GridNode node) {
        rlock.lock();
        try {
            for(ReplicaCoordinatorListener listener : listeners) {
                listener.onDiscovery(event, node);
            }
        } finally {
            rlock.unlock();
        }
    }

    public void onChannelClosed() {
        onDiscovery(DiscoveryEvent.leave, localNode);
    }

}
