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
import gridool.util.datetime.DateTimeFormatter;
import gridool.util.io.FastByteArrayOutputStream;
import gridool.util.io.IOUtils;
import gridool.util.lang.ObjectUtils;
import gridool.util.net.PoolableSocketChannelFactory;
import gridool.util.nio.NIOUtils;
import gridool.util.pool.ConcurrentKeyedStackObjectPool;
import gridool.util.primitive.Primitives;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV> <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridSocketPoolingClient implements GridTransportClient {
    private static final Log LOG = LogFactory.getLog(GridSocketPoolingClient.class);

    @Nonnull
    private final ConcurrentKeyedStackObjectPool<SocketAddress, ByteChannel> sockPool;

    public GridSocketPoolingClient(@Nonnull GridConfiguration config) {
        PoolableSocketChannelFactory<ByteChannel> factory = new PoolableSocketChannelFactory<ByteChannel>(false, true);
        factory.configure(config.getTransportChannelSweepInterval(), config.getTransportChannelTTL(), config.getTransportSocketReceiveBufferSize());
        this.sockPool = new ConcurrentKeyedStackObjectPool<SocketAddress, ByteChannel>("GridSocketPoolingClient", factory);
    }

    public void sendMessage(SocketAddress sockAddr, GridCommunicationMessage msg)
            throws GridException {
        final ByteBuffer buf = toBuffer(msg);
        final ByteChannel channel = sockPool.borrowObject(sockAddr);
        if(LOG.isDebugEnabled()) {
            LOG.debug("Sending a message [" + msg.getMessageId() + " (" + buf.remaining()
                    + " bytes)] to a node [" + sockAddr + "] using a channel [" + channel + ']');
        }
        final int written;
        try {
            written = NIOUtils.countingWriteFully(channel, buf);
            sockPool.returnObject(sockAddr, channel);
        } catch (IOException ioe) {
            IOUtils.closeQuietly(channel);
            final String errmsg = "Failed to send a GridCommunicationMessage [" + msg
                    + "] to host [" + sockAddr + ']';
            LOG.error(errmsg, ioe);
            throw new GridException(errmsg, ioe);
        } catch (Throwable e) {
            IOUtils.closeQuietly(channel);
            LOG.fatal(e.getMessage(), e);
            throw new GridException("Unexpected exception was caused", e);
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("Succeeded to send a GridCommunicationMessage (" + written
                    + " bytes) to host [" + sockAddr + "]");
        }
    }

    private static ByteBuffer toBuffer(final GridCommunicationMessage msg) {
        final long startTime = System.currentTimeMillis();
        final FastByteArrayOutputStream out = new FastByteArrayOutputStream();
        try {
            IOUtils.writeInt(0, out);// allocate first 4 bytes for size
            ObjectUtils.toStreamVerbose(msg, out);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
        final byte[] b = out.toByteArray();
        final int objsize = b.length - 4;
        Primitives.putInt(b, 0, objsize);

        if(LOG.isDebugEnabled()) {
            final long elapsedTime = System.currentTimeMillis() - startTime;
            LOG.debug("Elapsed time for serializing (including lazy evaluation) a GridCommunicationMessage ["
                    + msg.getMessageId()
                    + "] of "
                    + b.length
                    + " bytes: "
                    + DateTimeFormatter.formatTime(elapsedTime));
        }

        final ByteBuffer buf = ByteBuffer.wrap(b);
        return buf;
    }

    public void start() throws GridException {}

    public void close() throws IOException {
        sockPool.close();
    }

}
