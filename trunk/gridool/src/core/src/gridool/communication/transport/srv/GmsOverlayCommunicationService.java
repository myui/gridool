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
package gridool.communication.transport.srv;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridNode;
import gridool.communication.GridCommunicationMessage;
import gridool.communication.transport.CommunicationServiceBase;
import gridool.discovery.DiscoveryEvent;
import gridool.discovery.GridDiscoveryListener;
import gridool.discovery.GridDiscoveryService;

import javax.annotation.CheckForNull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class GmsOverlayCommunicationService extends CommunicationServiceBase {

    private final GridDiscoveryService gms;

    public GmsOverlayCommunicationService(@CheckForNull GridDiscoveryService gms, GridConfiguration config) {
        super(config);
        if(gms == null) {
            throw new IllegalArgumentException();
        }
        this.gms = gms;
    }

    public void start() throws GridException {
        gms.addListener(new GridDiscoveryListener() {
            public void onChannelClosed() {}

            public void onDiscovery(DiscoveryEvent event, GridNode node) {}
        });
    }

    public void stop() throws GridException {}

    public void sendMessage(GridNode dstNode, GridCommunicationMessage msg) throws GridException {
        msg.setSenderNode(getLocalNode());
        // TODO
        throw new UnsupportedOperationException();
    }

}
