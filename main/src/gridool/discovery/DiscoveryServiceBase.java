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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

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

    public DiscoveryServiceBase(@CheckForNull GridConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException();
        }
        this.config = config;
        this.listeners = Collections.synchronizedList(new LinkedList<GridDiscoveryListener>());
    }

    public String getServiceName() {
        return GridDiscoveryService.class.getName();
    }

    public void addListener(@Nonnull GridDiscoveryListener listener) {
        assert (!listeners.contains(listener)) : listener;
        listeners.add(listener);
    }

    public boolean removeListener(GridDiscoveryListener listener) {
        return listeners.remove(listener);
    }

    protected void handleJoin(@Nonnull GridNode node) {
        for(GridDiscoveryListener listener : listeners) {
            listener.onDiscovery(DiscoveryEvent.join, node);
        }
    }

    protected void handleLeave(@Nonnull GridNode node) {
        for(GridDiscoveryListener listener : listeners) {
            listener.onDiscovery(DiscoveryEvent.leave, node);
        }
    }

    protected void handleDropout(@Nonnull GridNode node) {
        for(GridDiscoveryListener listener : listeners) {
            listener.onDiscovery(DiscoveryEvent.dropout, node);
        }
    }

    protected void handleMetricsUpdate(@Nonnull GridNode node) {
        for(GridDiscoveryListener listener : listeners) {
            listener.onDiscovery(DiscoveryEvent.metricsUpdate, node);
        }
    }

    protected void handleClose() {
        for(GridDiscoveryListener listener : listeners) {
            listener.onChannelClosed();
        }
    }

}
