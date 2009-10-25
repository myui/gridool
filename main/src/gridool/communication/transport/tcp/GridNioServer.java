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
import gridool.communication.GridTransportListener;
import gridool.communication.transport.GridTransportServer;
import gridool.util.GridMessageBuffer;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.concurrent.ExecutorFactory;
import xbird.util.concurrent.jsr166.LinkedTransferQueue;
import xbird.util.io.IOUtils;
import xbird.util.nio.NIOUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridNioServer implements GridTransportServer {
    private static final Log LOG = LogFactory.getLog(GridNioServer.class);

    @Nonnull
    private final GridConfiguration config;

    private GridTransportListener _listener;
    private AcceptorThread _acceptorThread;
    private SelectorReadThread[] _readThreads;

    public GridNioServer(@Nonnull GridConfiguration config) {
        this.config = config;
    }

    public void setListener(@Nonnull GridTransportListener listener) {
        this._listener = listener;
    }

    public void start() throws GridException {
        if(_listener == null) {
            throw new IllegalStateException("GridTransportListener is not set");
        }

        final Selector connectSelector;
        try {
            connectSelector = Selector.open();
        } catch (IOException ioe) {
            LOG.error(ioe.getMessage(), ioe);
            throw new GridException("Failed to open Selectors", ioe);
        }
        final int port = config.getTransportServerPort();
        try {
            startListening(connectSelector, port);
        } catch (IOException e) {
            final String errmsg = "Failed to create selector on port: " + port;
            LOG.error(errmsg, e);
            throw new GridException(errmsg, e);
        }

        final int nthreads = config.getMessageProcessorPoolSize();
        final ExecutorService exec = ExecutorFactory.newFixedThreadPool(nthreads, "NioMessageProcessor", false);

        final int nSelectors = config.getSelectorReadThreadsCount();
        final SelectorReadThread[] readThreads = new SelectorReadThread[nSelectors];
        this._readThreads = readThreads;
        final AcceptorThread acceptorThread = new AcceptorThread(connectSelector, readThreads);
        this._acceptorThread = acceptorThread;

        for(int i = 0; i < nSelectors; i++) {
            final SelectorReadThread th;
            try {
                th = new SelectorReadThread(exec, _listener);
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
                throw new GridException("Failed to instantiate a SelectorReadThread instance", e);
            }
            th.setName("GridNioServer-SelectorReadThread#" + i);
            th.start();
            readThreads[i] = th;
        }
        acceptorThread.start();
    }

    private static void startListening(final Selector connectSelector, final int port)
            throws IOException {
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);

        ServerSocket servSocket = serverChannel.socket();
        servSocket.setReuseAddress(true);
        servSocket.bind(new InetSocketAddress(port));

        serverChannel.register(connectSelector, SelectionKey.OP_ACCEPT);
        if(LOG.isInfoEnabled()) {
            LOG.info("GridNioServer is started at port: " + port);
        }
    }

    public void close() throws IOException {
        IOUtils.closeQuietly(_readThreads);
        IOUtils.closeQuietly(_acceptorThread);
    }

    private static final class AcceptorThread extends Thread implements Closeable {

        final Selector selector;
        final SelectorReadThread[] readThreads;
        int curReadThread = 0;
        boolean closed = false;

        public AcceptorThread(Selector connectSelector, SelectorReadThread[] readThreads) {
            super("GridNioServer#AcceptorThread");
            this.selector = connectSelector;
            this.readThreads = readThreads;
        }

        @Override
        public void run() {
            try {
                while(!closed && selector.select() > 0) {
                    acceptPendingConnection();
                }
            } catch (IOException e) {
                LOG.error("NIO selector caused an error", e);
                throw new IllegalStateException(e);
            } finally {
                NIOUtils.close(selector);
            }
            if(LOG.isInfoEnabled()) {
                LOG.info("AcceptorThread is closed");
            }
        }

        private void acceptPendingConnection() throws IOException {
            for(Iterator<SelectionKey> iter = selector.selectedKeys().iterator(); iter.hasNext();) {
                final SelectionKey key = iter.next();
                iter.remove();

                if(!key.isValid()) { // Is key closed?
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("Non valid key was detected. Key is already closed?");
                    }
                    continue;
                }

                if(key.isAcceptable()) {
                    ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
                    final SocketChannel channel = serverChannel.accept();
                    if(channel != null) {
                        if(LOG.isDebugEnabled()) {
                            LOG.debug("Accepted a new client connection: "
                                    + channel.socket().getRemoteSocketAddress());
                        }
                        SelectorReadThread reader = getSelectorReadThread();
                        reader.addChannel(channel);
                    }
                }
            }
        }

        public void close() throws IOException {
            if(!closed) {
                this.closed = true;
                selector.wakeup(); // Causes the first selection operation that has not yet returned to return immediately.
                NIOUtils.close(selector);
            }
        }

        @Nonnull
        private SelectorReadThread getSelectorReadThread() {
            if(curReadThread == readThreads.length) {
                this.curReadThread = 0;
            }
            return readThreads[curReadThread++];
        }
    }

    private static final class SelectorReadThread extends Thread implements Closeable {

        final Queue<SocketChannel> requestQueue = new LinkedTransferQueue<SocketChannel>();

        final ExecutorService execPool;
        final GridTransportListener notifier;
        final Selector selector;
        final ByteBuffer sharedReadBuf;
        boolean closed = false;

        public SelectorReadThread(ExecutorService msgProcPool, GridTransportListener listener)
                throws IOException {
            super();
            this.execPool = msgProcPool;
            this.notifier = listener;
            this.selector = Selector.open();
            this.sharedReadBuf = ByteBuffer.allocate(8192);
            setDaemon(true);
        }

        void addChannel(SocketChannel channel) {// REVIEWME called from single thread
            requestQueue.offer(channel);
            selector.wakeup();
        }

        @Override
        public void run() {
            try {
                while(!closed) {
                    registerNewChannels();
                    if(selector.select() > 0) {
                        acceptPendingRequests();
                    }
                }
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
                throw new IllegalStateException(e);
            } finally {
                NIOUtils.close(selector);
            }
            if(LOG.isInfoEnabled()) {
                LOG.info("SelectorReadThread is closed");
            }
        }

        private void registerNewChannels() throws IOException {
            SocketChannel channel;
            while(null != (channel = requestQueue.poll())) {
                channel.configureBlocking(false);
                channel.register(selector, SelectionKey.OP_READ, new GridMessageBuffer());
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Accepted a new client connection: "
                            + channel.socket().getRemoteSocketAddress());
                }
            }
        }

        private void acceptPendingRequests() {
            for(Iterator<SelectionKey> iter = selector.selectedKeys().iterator(); iter.hasNext();) {
                SelectionKey key = iter.next();
                iter.remove();

                if(!key.isValid()) { // Is key closed?
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("Non valid key was detected. Key is already closed?");
                    }
                    continue;
                }

                if(key.isReadable()) {
                    SocketChannel channel = (SocketChannel) key.channel();
                    handleRead(channel, key, sharedReadBuf, notifier, execPool);
                }
            }
        }

        public void close() throws IOException {
            if(!closed) {
                this.closed = true;
                selector.wakeup(); // Causes the first selection operation that has not yet returned to return immediately.
                NIOUtils.close(selector);
            }
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
                        if(LOG.isDebugEnabled()) {
                            LOG.debug("Recieved a GridCommunicationMessage [" + msg.getMessageId()
                                    + "]");
                        }
                        notifier.notifyListener(msg);
                    }
                });
                break;
            }
        }
    }
}
