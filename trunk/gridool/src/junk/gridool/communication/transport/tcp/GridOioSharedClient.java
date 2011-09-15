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

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.lang.PrintUtils;
import xbird.util.net.NetUtils;
import xbird.util.net.SocketUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridOioSharedClient implements GridTransportClient {
    private static final Log LOG = LogFactory.getLog(GridOioSharedClient.class);

    private final Map<SocketAddress, Socket> clients;

    public GridOioSharedClient(@Nonnull GridConfiguration config) {
        this.clients = new HashMap<SocketAddress, Socket>(128);
    }

    public void sendMessage(SocketAddress sockAddr, GridCommunicationMessage msg)
            throws GridException {
        Socket socket;
        synchronized(clients) {
            socket = clients.get(sockAddr);            
            if(socket == null) {
                socket = opneSocket(sockAddr);
            } else {
                if(!socket.isConnected()) {
                    NetUtils.closeQuietly(socket);
                    socket = opneSocket(sockAddr);
                }
            }
        }
        assert (socket != null);

        final byte[] b = GridUtils.toBytes(msg);
        if(LOG.isDebugEnabled()) {
            LOG.debug("Sending a message [" + msg.getMessageId() + " (" + b.length
                    + " bytes)] to a node [" + sockAddr + "] using a socket [" + socket + ']');
        }
        try {
            syncWrite(socket, b);
        } catch (IOException ioe) {
            final String errmsg = "Failed to send a GridCommunicationMessage [" + msg
                    + "] to host [" + sockAddr + ']';
            LOG.error(errmsg, ioe);
            throw new GridException(errmsg, ioe);
        } catch (Throwable e) {
            LOG.fatal(PrintUtils.prettyPrintStackTrace(e, -1));
            throw new GridException("Unexpected exception was caused", e);
        }
    }   
    
    private synchronized void syncWrite(final Socket socket, final byte[] b) throws IOException {
        OutputStream sockout = socket.getOutputStream();
        sockout.write(b);
        sockout.flush();
    }

    public void start() throws GridException {}

    public void close() throws IOException {}
    
    private static Socket opneSocket(final SocketAddress sockAddr) throws GridException {
        try {
            return SocketUtils.openSocket(sockAddr, 0, 2000L, 3);
        } catch (IOException e) {
            LOG.error("failed to connect: " + sockAddr, e);
            throw new GridException(e);
        }
    }

}
