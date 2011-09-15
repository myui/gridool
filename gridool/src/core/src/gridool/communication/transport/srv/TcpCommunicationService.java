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
import gridool.Settings;
import gridool.communication.GridCommunicationMessage;
import gridool.communication.transport.CommunicationServiceBase;
import gridool.communication.transport.GridTransportClient;
import gridool.communication.transport.GridTransportServer;
import gridool.communication.transport.tcp.GridMasterSlaveWorkerServer;
import gridool.communication.transport.tcp.GridNioClient;
import gridool.communication.transport.tcp.GridNioServer;
import gridool.communication.transport.tcp.GridOioClient;
import gridool.communication.transport.tcp.GridSharedClient;
import gridool.communication.transport.tcp.GridSocketPoolingClient;
import gridool.communication.transport.tcp.GridThreadPerConnectionServer;
import gridool.util.GridUtils;
import gridool.util.io.IOUtils;
import gridool.util.system.SystemUtils;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class TcpCommunicationService extends CommunicationServiceBase {
    private static final Log LOG = LogFactory.getLog(TcpCommunicationService.class);

    private static final String clientClass;
    private static final String serverClass;
    static {
        clientClass = Settings.get("gridool.transport.client_class", GridOioClient.class.getSimpleName());
        serverClass = Settings.get("gridool.transport.server_class", GridThreadPerConnectionServer.class.getSimpleName());
    }

    protected final GridTransportClient client;
    protected final GridTransportServer server;

    public TcpCommunicationService(@Nonnull GridConfiguration config) {
        super(config);
        // client
        if(GridNioClient.class.getSimpleName().equals(clientClass)) {
            this.client = new GridNioClient(config);
        } else if(GridOioClient.class.getSimpleName().equals(clientClass)) {
            this.client = new GridOioClient(config);
        } else if(GridSharedClient.class.getSimpleName().equals(clientClass)) {
            this.client = new GridSharedClient(config);
        } else if(GridSocketPoolingClient.class.getSimpleName().equals(clientClass)) {
            this.client = new GridSocketPoolingClient(config);
        } else {
            throw new IllegalStateException("Unexpected GridTransportClient class: " + clientClass);
        }
        // server
        if(GridThreadPerConnectionServer.class.getSimpleName().equals(serverClass)) {
            this.server = new GridThreadPerConnectionServer(config);
        } else if(GridMasterSlaveWorkerServer.class.getSimpleName().equals(serverClass)) {
            this.server = new GridMasterSlaveWorkerServer(config);
        } else if(GridNioServer.class.getSimpleName().equals(serverClass)) {
            if(!SystemUtils.isEpollEnabled()) {
                throw new IllegalStateException("GridNioServer cannot be used because 'Epoll' is not supported in this machine");
            }
            this.server = new GridNioServer(config);
        } else {
            throw new IllegalStateException("Unexpected GridTransportServer class: " + serverClass);
        }
        if(LOG.isInfoEnabled()) {
            LOG.info("GridTransportClient: " + client.getClass().getSimpleName()
                    + ", GridTransportServer: " + server.getClass().getSimpleName());
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
