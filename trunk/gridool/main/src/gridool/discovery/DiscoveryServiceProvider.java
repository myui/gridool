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
import gridool.GridResourceRegistry;
import gridool.discovery.jgroups.JGroupsDiscoveryService;
import gridool.replication.ReplicaCoordinator;
import gridool.replication.ReplicationManager;
import gridool.routing.GridTaskRouter;

import javax.annotation.Nonnull;

import org.apache.commons.logging.LogFactory;

import xbird.config.Settings;
import xbird.util.lang.ObjectUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DiscoveryServiceProvider {

    private DiscoveryServiceProvider() {}

    @Nonnull
    public static GridDiscoveryService createService(@Nonnull GridTaskRouter taskRouter, @Nonnull GridResourceRegistry resourceRegistry, @Nonnull GridConfiguration config) {
        GridDiscoveryService srv = new JGroupsDiscoveryService(resourceRegistry, config);
        srv.addListener(taskRouter);

        ReplicationManager replMgr = resourceRegistry.getReplicationManager();
        ReplicaCoordinator replCoord = replMgr.getReplicaCoordinator();
        srv.addListener(replCoord); // TODO REVIEWME

        final String additional = Settings.get("gridool.discovery.listener");
        if(additional != null) {
            setupAdditionalListeners(srv, additional);
        }

        resourceRegistry.setDiscoveryService(srv);
        return srv;
    }

    private static void setupAdditionalListeners(@Nonnull GridDiscoveryService srv, @Nonnull String additional) {
        final String[] listeners = additional.split(",");
        for(String listenerClazz : listeners) {
            Object obj = ObjectUtils.instantiateSafely(listenerClazz.trim());
            if(obj != null && obj instanceof GridDiscoveryListener) {
                GridDiscoveryListener listener = (GridDiscoveryListener) obj;
                srv.addListener(listener);
            } else {
                LogFactory.getLog(DiscoveryServiceProvider.class).warn("Specified DiscoveryListener not found: "
                        + listenerClazz);
            }
        }
    }

}
