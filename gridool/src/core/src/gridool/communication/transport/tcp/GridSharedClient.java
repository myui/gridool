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
package gridool.communication.transport.tcp;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.communication.GridCommunicationMessage;
import gridool.communication.transport.GridTransportClient;
import gridool.util.GridUtils;
import gridool.util.lang.PrintUtils;
import gridool.util.net.NetUtils;
import gridool.util.net.SocketUtils;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

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
public final class GridSharedClient implements GridTransportClient {
    private static final Log LOG = LogFactory.getLog(GridSharedClient.class);

    private final Map<SocketAddress, Socket> clients;

    public GridSharedClient(@Nonnull GridConfiguration config) {
        this.clients = new HashMap<SocketAddress, Socket>(128);
    }

    private Socket getSocket(SocketAddress sockAddr) throws GridException {
        Socket socket;
        synchronized(clients) {
            socket = clients.get(sockAddr);
            if(socket == null) {
                socket = openSocket(sockAddr);
            } else {
                if(!socket.isConnected()) {
                    NetUtils.closeQuietly(socket);
                    socket = openSocket(sockAddr);
                    clients.put(sockAddr, socket);
                }
            }
        }
        assert (socket != null);
        return socket;
    }

    public void sendMessage(SocketAddress sockAddr, GridCommunicationMessage msg)
            throws GridException {
        final byte[] b = GridUtils.toBytes(msg);
        if(LOG.isDebugEnabled()) {
            LOG.debug("Sending a message [" + msg.getMessageId() + " (" + b.length
                    + " bytes)] to a node [" + sockAddr + "] using a socket [" + sockAddr + ']');
        }
        Socket socket = getSocket(sockAddr);
        boolean succeed = false;
        try {
            for(int i = 0; !succeed && i < 2; i++) {
                synchronized(socket) {
                    succeed = SocketUtils.write(socket, b, 5000L, 3);
                }
                if(!succeed) {
                    if(LOG.isWarnEnabled()) {
                        LOG.warn("Retry sending GridCommunicationMessage [" + msg + "] to host ["
                                + sockAddr + ']');
                    }
                    synchronized(clients) {
                        clients.remove(sockAddr);
                        NetUtils.closeQuietly(socket);
                    }
                    socket = getSocket(sockAddr);
                }
            }
        } catch (Throwable e) {
            LOG.error(PrintUtils.prettyPrintStackTrace(e, -1));
            synchronized(clients) {
                clients.remove(sockAddr);
                NetUtils.closeQuietly(socket);
            }
            throw new GridException("Unexpected exception was caused", e);
        }
        if(!succeed) {
            String errmsg = "Failed to send a GridCommunicationMessage [" + msg + "] to host ["
                    + sockAddr + ']';
            LOG.error(errmsg);
            synchronized(clients) {
                clients.remove(sockAddr);
                NetUtils.closeQuietly(socket);
            }
            throw new GridException(errmsg);
        }
    }

    public void start() throws GridException {}

    public void close() throws IOException {}

    private static Socket openSocket(final SocketAddress sockAddr) throws GridException {
        try {
            return SocketUtils.openSocket(sockAddr, 0, 2000L, 3);
        } catch (IOException e) {
            LOG.error("failed to connect: " + sockAddr, e);
            throw new GridException(e);
        }
    }
}
