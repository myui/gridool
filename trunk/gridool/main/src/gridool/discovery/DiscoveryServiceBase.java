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
package gridool.discovery;

import gridool.GridConfiguration;
import gridool.GridNode;

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
public abstract class DiscoveryServiceBase implements GridDiscoveryService {

    protected final GridConfiguration config;

    private final List<GridDiscoveryListener> listeners;
    private final Lock rlock;
    private final Lock wlock;

    public DiscoveryServiceBase(@CheckForNull GridConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException();
        }
        this.config = config;
        this.listeners = new LinkedList<GridDiscoveryListener>();
        ReadWriteLock rwlock = new ReentrantReadWriteLock();
        this.rlock = rwlock.readLock();
        this.wlock = rwlock.writeLock();
    }

    public String getServiceName() {
        return GridDiscoveryService.class.getName();
    }

    public void addListener(@Nonnull GridDiscoveryListener listener) {
        wlock.lock();
        try {
            if(!listeners.add(listener)) {
                assert false : "listner already defined: " + listener;
            }
        } finally {
            wlock.unlock();
        }
    }

    public boolean removeListener(@Nonnull GridDiscoveryListener listener) {
        wlock.lock();
        try {
            return listeners.remove(listener);
        } finally {
            wlock.unlock();
        }
    }

    protected void handleJoin(@Nonnull GridNode node) {
        rlock.lock();
        try {
            for(GridDiscoveryListener listener : listeners) {
                listener.onDiscovery(DiscoveryEvent.join, node);
            }
        } finally {
            rlock.unlock();
        }
    }

    protected void handleLeave(@Nonnull GridNode node) {
        rlock.lock();
        try {
            for(GridDiscoveryListener listener : listeners) {
                listener.onDiscovery(DiscoveryEvent.leave, node);
            }
        } finally {
            rlock.unlock();
        }
    }

    protected void handleDropout(@Nonnull GridNode node) {
        rlock.lock();
        try {
            for(GridDiscoveryListener listener : listeners) {
                listener.onDiscovery(DiscoveryEvent.dropout, node);
            }
        } finally {
            rlock.unlock();
        }
    }

    protected void handleMetricsUpdate(@Nonnull GridNode node) {
        rlock.lock();
        try {
            for(GridDiscoveryListener listener : listeners) {
                listener.onDiscovery(DiscoveryEvent.metricsUpdate, node);
            }
        } finally {
            rlock.unlock();
        }
    }

    protected void handleClose() {
        rlock.lock();
        try {
            for(GridDiscoveryListener listener : listeners) {
                listener.onChannelClosed();
            }
        } finally {
            rlock.unlock();
        }
    }

}
