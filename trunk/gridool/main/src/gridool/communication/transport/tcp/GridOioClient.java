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

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.lang.PrintUtils;
import xbird.util.net.NetUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridOioClient implements GridTransportClient {
    private static final Log LOG = LogFactory.getLog(GridOioClient.class);

    public GridOioClient(@Nonnull GridConfiguration config) {}

    public void sendMessage(SocketAddress sockAddr, GridCommunicationMessage msg)
            throws GridException {
        final Socket socket = new Socket();
        try {
            socket.connect(sockAddr);
        } catch (IOException e) {
            LOG.error("failed to connect: " + sockAddr, e);
            throw new GridException(e);
        } catch (Throwable e) {
            LOG.fatal("failed to connect: " + sockAddr, e);
            throw new GridException(e);
        }

        final byte[] b = GridUtils.toBytes(msg);
        if(LOG.isDebugEnabled()) {
            LOG.debug("Sending a message [" + msg.getMessageId() + " (" + b.length
                    + " bytes)] to a node [" + sockAddr + "] using a socket [" + socket + ']');
        }
        try {
            OutputStream sockout = socket.getOutputStream();
            sockout.write(b);
            sockout.flush();
            NetUtils.shutdownOutputQuietly(socket); // terminate socket output (send FIN)
        } catch (IOException ioe) {
            final String errmsg = "Failed to send a GridCommunicationMessage [" + msg
                    + "] to host [" + sockAddr + ']';
            LOG.error(errmsg, ioe);
            throw new GridException(errmsg, ioe);
        } catch (Throwable e) {
            LOG.fatal(PrintUtils.prettyPrintStackTrace(e, -1));
            throw new GridException("Unexpected exception was caused", e);
        } finally {
            NetUtils.closeQuietly(socket);
        }
    }

    public void start() throws GridException {}

    public void close() throws IOException {}

}
