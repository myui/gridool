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
package gridool.replication.listener;

import gridool.GridKernel;
import gridool.GridNode;
import gridool.discovery.DiscoveryEvent;
import gridool.replication.ReplicaCoordinatorListener;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class ReplicationEventLogger implements ReplicaCoordinatorListener {
    private static final Log LOG = LogFactory.getLog(ReplicationEventLogger.class);

    public ReplicationEventLogger() {}

    public void setup(GridKernel kernel) {}

    public boolean onConfigureReplica(GridNode masterNode, List<GridNode> oldReplicas, List<GridNode> newReplicas) {
        if(LOG.isInfoEnabled()) {
            LOG.info("Configure new replicas '" + newReplicas + "' for node '" + masterNode
                    + "'.\nOld replicas are '" + oldReplicas + '\'');
        }
        return true;
    }

    public void onChannelClosed() {
        if(LOG.isDebugEnabled()) {
            LOG.debug("Channel closed");
        }
    }

    public void onDiscovery(DiscoveryEvent event, GridNode node) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("Event '" + event + "' happend on " + node);
        }
    }
}
