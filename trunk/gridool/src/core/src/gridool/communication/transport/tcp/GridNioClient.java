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
import gridool.util.io.IOUtils;
import gridool.util.lang.PrintUtils;
import gridool.util.net.NetUtils;
import gridool.util.net.SocketUtils;
import gridool.util.nio.NIOUtils;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

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
public final class GridNioClient implements GridTransportClient {
    private static final Log LOG = LogFactory.getLog(GridNioClient.class);

    public GridNioClient(@Nonnull GridConfiguration config) {}

    public void sendMessage(SocketAddress sockAddr, GridCommunicationMessage msg)
            throws GridException {
        final SocketChannel channel;
        try {
            channel = SocketChannel.open();
        } catch (IOException e) {
            LOG.error(PrintUtils.prettyPrintStackTrace(e, -1));
            throw new GridException(e);
        }
        final Socket socket = channel.socket();
        try {
            SocketUtils.openSocket(socket, sockAddr, 0, 2000L, 3);
        } catch (IOException e) {
            NetUtils.closeQuietly(socket);
            IOUtils.closeQuietly(channel);
            LOG.error(PrintUtils.prettyPrintStackTrace(e, -1));
            throw new GridException(e);
        }

        final ByteBuffer buf = toBuffer(msg);
        if(LOG.isDebugEnabled()) {
            LOG.debug("Sending a GridCommunicationMessage [" + msg.getMessageId() + " ("
                    + buf.remaining() + " bytes)] to a node [" + sockAddr + "] using a channel ["
                    + channel + ']');
        }
        final int written;
        try {
            written = NIOUtils.countingWriteFully(channel, buf);
            NetUtils.shutdownOutputQuietly(socket); // terminate socket output (send FIN)
        } catch (IOException ioe) {
            final String errmsg = "Failed to send a GridCommunicationMessage [" + msg
                    + "] to host [" + sockAddr + ']';
            LOG.error(errmsg, ioe);
            throw new GridException(errmsg, ioe);
        } catch (Throwable e) {
            LOG.fatal(e.getMessage(), e);
            throw new GridException("Unexpected exception was caused", e);
        } finally {
            NetUtils.closeQuietly(socket);
            IOUtils.closeQuietly(channel);
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("Succeeded to send a GridCommunicationMessage (" + written
                    + " bytes) to host [" + sockAddr + "]");
        }
    }

    private static ByteBuffer toBuffer(final GridCommunicationMessage msg) {
        final byte[] b = GridUtils.toBytes(msg);
        final ByteBuffer buf = ByteBuffer.wrap(b);
        return buf;
    }

    public void start() throws GridException {}

    public void close() throws IOException {}

}
