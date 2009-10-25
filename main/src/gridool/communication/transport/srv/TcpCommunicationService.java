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
import gridool.communication.transport.GridTransportClient;
import gridool.communication.transport.GridTransportServer;
import gridool.communication.transport.tcp.GridMasterSlaveWorkerServer;
import gridool.communication.transport.tcp.GridNioServer;
import gridool.communication.transport.tcp.GridNioSocketPoolingClient;
import gridool.communication.transport.tcp.GridOioClient;
import gridool.util.GridUtils;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.Nonnull;

import org.apache.commons.logging.LogFactory;

import xbird.config.Settings;
import xbird.util.io.IOUtils;
import xbird.util.system.SystemUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class TcpCommunicationService extends CommunicationServiceBase {

    private static final boolean forceOio;
    static {
        String prop = Settings.getThroughSystemProperty("gridool.transport.force_oio");
        forceOio = Boolean.parseBoolean(prop);
    }

    protected final GridTransportClient client;
    protected final GridTransportServer server;

    public TcpCommunicationService(@Nonnull GridConfiguration config) {
        super(config);
        final boolean hasEpoll = SystemUtils.isEpollEnabled();
        if(!forceOio && hasEpoll) {
            this.client = new GridNioSocketPoolingClient(config);
            this.server = new GridNioServer(config);
        } else {
            this.client = new GridOioClient(config);
            this.server = new GridMasterSlaveWorkerServer(config);
        }
    }

    public void start() throws GridException {
        server.setListener(this);
        server.start();
        client.start();
    }

    public void stop() throws GridException {
        IOUtils.closeQuietly(client);
        try {
            server.close(); // TODO REVIEWME
        } catch (IOException ex) {
            LogFactory.getLog(getClass()).trace(ex.getMessage(), ex);
            throw new GridException(ex);
        }
    }

    public void sendMessage(GridNode dstNode, GridCommunicationMessage msg) throws GridException {
        msg.setSenderNode(getLocalNode());
        InetSocketAddress sockAddr = GridUtils.getDestination(dstNode);
        client.sendMessage(sockAddr, msg);
    }

}
