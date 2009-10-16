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

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.datetime.StopWatch;
import xbird.util.io.FastByteArrayOutputStream;
import xbird.util.io.IOUtils;
import xbird.util.lang.ObjectUtils;
import xbird.util.lang.Primitives;
import xbird.util.net.PoolableSocketChannelFactory;
import xbird.util.nio.NIOUtils;
import xbird.util.pool.ConcurrentKeyedStackObjectPool;

/**
 * 
 * <DIV lang="en"></DIV> <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridNioSocketPoolingClient implements GridTransportClient {
    private static final Log LOG = LogFactory.getLog(GridNioSocketPoolingClient.class);

    @Nonnull
    private final ConcurrentKeyedStackObjectPool<SocketAddress, ByteChannel> sockPool;

    private static final int SWEEP_INTERVAL;
    private static final int TIME_TO_LIVE;
    private static final int SO_RCVBUF_SIZE;

    static {
        String sweep = System.getProperty("gridool.nio.socketpool.sweep_interval");
        String ttl = System.getProperty("gridool.nio.socketpool.connection.ttl");
        String rcvbuf = System.getProperty("gridool.nio.socketpool.so_rcvbufsize");
        SWEEP_INTERVAL = Primitives.parseInt(sweep, 60000);
        TIME_TO_LIVE = Primitives.parseInt(ttl, 30000);
        SO_RCVBUF_SIZE = Primitives.parseInt(rcvbuf, 4096);
    }

    public GridNioSocketPoolingClient(@Nonnull GridConfiguration config) {
        PoolableSocketChannelFactory factory = new PoolableSocketChannelFactory(false, true);
        factory.configure(SWEEP_INTERVAL, TIME_TO_LIVE, SO_RCVBUF_SIZE);
        this.sockPool = new ConcurrentKeyedStackObjectPool<SocketAddress, ByteChannel>("GridNioClient", factory);
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
                    + StopWatch.elapsedTime(elapsedTime));
        }

        final ByteBuffer buf = ByteBuffer.wrap(b);
        return buf;
    }

    public void start() throws GridException {}

    public void close() throws IOException {
        sockPool.close();
    }

}
