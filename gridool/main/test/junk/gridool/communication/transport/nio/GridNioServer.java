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
package gridool.communication.transport.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.communication.GridCommunicationMessage;
import gridool.communication.GridTransportListener;
import gridool.communication.transport.GridTransportServer;
import gridool.util.GridMessageBuffer;
import xbird.util.concurrent.ExecutorFactory;
import xbird.util.nio.NIOUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridNioServer implements GridTransportServer, Runnable {
    private static final Log LOG = LogFactory.getLog(GridNioServer.class);

    @Nonnull
    private final GridConfiguration config;
    @Nullable
    private Selector selector = null;
    private boolean closed = true;

    private final ByteBuffer sharedReadBuf;
    private GridTransportListener notifier;

    private final ExecutorService execPool;

    public GridNioServer(@Nonnull GridConfiguration config) {
        this.config = config;
        this.sharedReadBuf = ByteBuffer.allocate(8192);
        int nthreads = config.getMessageProcessorPoolSize();
        this.execPool = ExecutorFactory.newFixedThreadPool(nthreads, "NioMessageProcessor");
    }

    public void setListener(GridTransportListener listener) {
        this.notifier = listener;
    }

    @Override
    public void start() throws GridException {
        if(notifier == null) {
            throw new IllegalStateException("GridTransportListener is not set");
        }
        this.closed = false;

        final int port = config.getTransportServerPort();
        try {
            this.selector = createSelector(port);
        } catch (IOException e) {
            final String errmsg = "Failed to create selector on port: " + port;
            LOG.error(errmsg);
            throw new GridException(errmsg, e);
        }

        final Thread thread = new Thread(this, GridNioServer.class.getSimpleName());
        //thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void run() {
        try {
            accept(selector);
        } catch (IOException e) {
            LOG.error("NIO selector caused an error", e);
            NIOUtils.close(selector);
        }
    }

    private static Selector createSelector(int port) throws IOException {
        final Selector selector = SelectorProvider.provider().openSelector();

        ServerSocketChannel serverChannel = ServerSocketChannel.open();        
        serverChannel.configureBlocking(false);

        ServerSocket servSocket = serverChannel.socket();
        servSocket.setReuseAddress(true);
        servSocket.bind(new InetSocketAddress(port));

        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        if(LOG.isInfoEnabled()) {
            LOG.info("GridNioServer is started at port: " + port);
        }
        return selector;
    }

    public void accept(@Nonnull final Selector selector) throws IOException {
        while(!closed && selector.select() > 0) {
            for(Iterator<SelectionKey> iter = selector.selectedKeys().iterator(); iter.hasNext();) {
                SelectionKey key = iter.next();
                iter.remove();

                if(!key.isValid()) { // Is key closed?
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("Non valid key was detected. Key is already closed?");
                    }
                    continue;
                }

                if(key.isAcceptable()) {
                    ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
                    handleAccept(serverChannel, selector);
                } else if(key.isReadable()) {
                    SocketChannel channel = (SocketChannel) key.channel();
                    handleRead(channel, key, sharedReadBuf, notifier, execPool);
                }
            }
        }
        if(LOG.isInfoEnabled()) {
            LOG.info("GridNioServer is going to be closed");
        }
        this.closed = true;
        if(selector.isOpen()) {
            for(SelectionKey key : selector.keys()) {
                NIOUtils.close(key);
            }
            selector.close();
        }
    }

    private static void handleAccept(final ServerSocketChannel serverChannel, final Selector selector)
            throws IOException {
        final SocketChannel channel = serverChannel.accept();
        if(channel == null) {
            return;
        }
        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_READ, new GridMessageBuffer());
        if(LOG.isDebugEnabled()) {
            LOG.debug("Accepted a new client connection: "
                    + channel.socket().getRemoteSocketAddress());
        }
    }

    private static void handleRead(final SocketChannel channel, final SelectionKey key, final ByteBuffer sharedReadBuf, final GridTransportListener notifier, final ExecutorService exec) {
        sharedReadBuf.clear();
        final SocketAddress remoteAddr = channel.socket().getRemoteSocketAddress();
        final int bytesRead;
        try {
            bytesRead = channel.read(sharedReadBuf);
        } catch (IOException e) {
            LOG.warn("Failed to read data from client: " + remoteAddr, e);
            NIOUtils.close(key);
            return;
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("Read " + bytesRead + " bytes from a client socket: " + remoteAddr);
        }
        if(bytesRead == -1) {
            if(LOG.isTraceEnabled()) {
                LOG.trace("Remote client closed connection: " + remoteAddr);
            }
            NIOUtils.close(key);
            return;
        } else if(bytesRead == 0) {
            return;
        }

        final GridMessageBuffer msgBuf = (GridMessageBuffer) key.attachment();
        sharedReadBuf.flip();
        while(sharedReadBuf.remaining() > 0) {
            msgBuf.read(sharedReadBuf);
            if(msgBuf.isFilled()) {
                exec.execute(new Runnable() {
                    public void run() {
                        final GridCommunicationMessage msg = msgBuf.toMessage();
                        msgBuf.reset();
                        if(LOG.isInfoEnabled()) {
                            LOG.info("Recieved a GridCommunicationMessage [" + msg.getMessageId()
                                    + "]");
                        }
                        notifier.notifyListener(msg);
                    }
                });
                break;
            }
        }
    }

    @Override
    public void close() throws IOException {
        if(!closed) {
            this.closed = true;
            selector.wakeup(); // Causes the first selection operation that has not yet returned to return immediately.
            NIOUtils.close(selector);
        }
    }
}
