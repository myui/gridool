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
import gridool.util.concurrent.ExecutorFactory;
import gridool.util.concurrent.jsr166.LinkedTransferQueue;
import gridool.util.io.IOUtils;
import gridool.util.lang.ObjectUtils;
import gridool.util.lang.PrintUtils;
import gridool.util.net.NetUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

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
public final class GridMasterSlaveWorkerServer implements GridTransportServer {
    private static final Log LOG = LogFactory.getLog(GridMasterSlaveWorkerServer.class);
    public static final int READER_POOL_GROW_THRESHOLD = 20;
    private static final int GROW_CHECK_INTERVAL = 100;

    @Nonnull
    private final GridConfiguration config;

    private GridTransportListener listener;
    private AcceptorThread acceptor;
    private ThreadPoolExecutor readerPool;

    public GridMasterSlaveWorkerServer(@Nonnull GridConfiguration config) {
        this.config = config;
    }

    public void setListener(@Nonnull GridTransportListener listener) {
        this.listener = listener;
    }

    public void start() throws GridException {
        if(listener == null) {
            throw new IllegalStateException("GridTransportListener is not set");
        }

        final BlockingQueue<Socket> taskQueue = new LinkedTransferQueue<Socket>();
        final int msgProcs = config.getMessageProcessorPoolSize();
        final ExecutorService msgProcPool = ExecutorFactory.newFixedThreadPool(msgProcs, "OioMessageProcessor", true);
        final int readers = config.getSelectorReadThreadsCount();

        this.readerPool = ExecutorFactory.newThreadPool(readers, readers * 3, 60L, "GridMultiWorkersServer#PooledRequestHandler", true);
        PooledRequestHandler handler = setUpHandlers(taskQueue, msgProcPool, readers);

        final int port = config.getTransportServerPort();
        this.acceptor = acceptConnections(port, taskQueue, handler);
    }

    private PooledRequestHandler setUpHandlers(final BlockingQueue<Socket> taskQueue, final ExecutorService msgProcPool, final int numReaders) {
        if(numReaders < 1) {
            throw new IllegalArgumentException("at least one PooledRequestHandler, but was "
                    + numReaders);
        }
        final PooledRequestHandler handler = new PooledRequestHandler(taskQueue, listener, msgProcPool);
        readerPool.execute(handler);
        for(int i = 1; i < numReaders; i++) {
            PooledRequestHandler h = new PooledRequestHandler(taskQueue, listener, msgProcPool);
            readerPool.execute(h);
        }
        return handler;
    }

    private AcceptorThread acceptConnections(final int port, final BlockingQueue<Socket> taskQueue, final PooledRequestHandler handler)
            throws GridException {
        final ServerSocket ss;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
        } catch (IOException e) {
            String errmsg = "Could not create ServerSocket on port: " + port;
            LogFactory.getLog(getClass()).error(errmsg, e);
            throw new GridException(errmsg);
        }

        if(LOG.isInfoEnabled()) {
            LOG.info("GridMultiWorkersServer is started at port: " + port);
        }

        final int growThreshold = config.getReadThreadsGrowThreshold();
        final AcceptorThread acceptor = new AcceptorThread(ss, taskQueue, readerPool, growThreshold, handler);
        acceptor.start();
        return acceptor;
    }

    public void close() throws IOException {
        acceptor.close();
    }

    private static final class AcceptorThread extends Thread implements Closeable {
        final ServerSocket serverSocket;
        final BlockingQueue<Socket> taskQueue;
        final ThreadPoolExecutor readerPool;
        final PooledRequestHandler handler;

        int readerPoolSize;
        final int maxReaders;
        final int growThreshold;

        public AcceptorThread(ServerSocket socket, BlockingQueue<Socket> taskQueue, ThreadPoolExecutor readerPool, int readerGrowThreshold, PooledRequestHandler handler) {
            super("GridMultiWorkersServer#AcceptorThread");
            this.serverSocket = socket;
            this.taskQueue = taskQueue;
            this.readerPool = readerPool;
            this.handler = handler;

            this.readerPoolSize = readerPool.getCorePoolSize();
            this.maxReaders = readerPool.getMaximumPoolSize();
            this.growThreshold = readerGrowThreshold;
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                for(int i = 0; !Thread.interrupted(); i++) {
                    final Socket clientSocket = serverSocket.accept();
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("Accepted a new connection: " + clientSocket);
                    }
                    if(i == GROW_CHECK_INTERVAL) {
                        final int remaining = taskQueue.size();
                        if(remaining >= growThreshold) {
                            if((++readerPoolSize) < maxReaders) {
                                if(LOG.isInfoEnabled()) {
                                    LOG.info("Maximum pool size of GridMultiWorkersServer#PooledRequestHandler is grown to: "
                                            + readerPoolSize);
                                }
                                PooledRequestHandler newHandler = handler.clone();
                                readerPool.execute(newHandler);
                            }
                        }
                        i = 0;
                    }
                    taskQueue.put(clientSocket);
                }
            } catch (InterruptedException e) {
                LOG.warn(PrintUtils.prettyPrintStackTrace(e, -1));
            } catch (IOException ioe) {
                LOG.error(PrintUtils.prettyPrintStackTrace(ioe, -1));
            } catch (Throwable e) {
                LOG.error(PrintUtils.prettyPrintStackTrace(e, -1));
            } finally {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug(PrintUtils.prettyPrintStackTrace(e, -1));
                    }
                }
            }
            if(LOG.isInfoEnabled()) {
                LOG.info("ServerSocket is closed: " + serverSocket);
            }
        }

        public void close() throws IOException {
            interrupt();
            serverSocket.close();
        }
    }

    private static final class PooledRequestHandler implements Runnable, Cloneable {
        final BlockingQueue<Socket> taskQueue;
        final GridTransportListener listener;
        final ExecutorService msgProcPool;

        public PooledRequestHandler(@Nonnull BlockingQueue<Socket> taskQueue, @Nonnull GridTransportListener listener, ExecutorService msgProcPool) {
            this.taskQueue = taskQueue;
            this.listener = listener;
            this.msgProcPool = msgProcPool;
        }

        public void run() {
            try {
                while(true) {
                    final Socket clientSocket = taskQueue.take();
                    if(handleConnection(clientSocket, listener, msgProcPool)) {
                        taskQueue.put(clientSocket); // clientSocket may be reused
                    } else {// to avoid broken pipe IOException
                        NetUtils.closeQuietly(clientSocket);
                    }
                }
            } catch (InterruptedException ie) {
                LOG.warn(PrintUtils.prettyPrintStackTrace(ie, -1));
            } catch (Throwable e) {
                LOG.fatal(PrintUtils.prettyPrintStackTrace(e, -1));
            }
            if(LOG.isInfoEnabled()) {
                LOG.info("PooledRequestHandler is going to shutdown");
            }
        }

        @Override
        protected PooledRequestHandler clone() throws CloneNotSupportedException {
            return new PooledRequestHandler(taskQueue, listener, msgProcPool);
        }
    }

    static boolean handleConnection(final Socket clientSocket, final GridTransportListener listener, final ExecutorService msgProcPool) {
        final InputStream is;
        final int size;
        try {
            is = clientSocket.getInputStream();
            if(LOG.isDebugEnabled()) {
                final int available = is.available();
                if(available == 0) {
                    LOG.debug("Reading data from socket [" + clientSocket + "] seems to be blocked");
                }
            }
            size = IOUtils.readUnsignedIntOrEOF(is);
        } catch (IOException e) {
            LOG.fatal(PrintUtils.prettyPrintStackTrace(e, -1));
            return false;
        }
        if(size == -1) {
            if(LOG.isDebugEnabled()) {
                LOG.debug("Connection is closed by remote peer: " + clientSocket);
            }
            return false;
        } else if(size <= 0) {
            LOG.warn("Illegal message (size: " + size + ") was detected for a connection: "
                    + clientSocket);
            return false;
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("Start reading " + size + " bytes from a socket [" + clientSocket + ']');
        }
        // read an object from stream
        final byte[] b = new byte[size];
        try {
            IOUtils.readFully(is, b, 0, size);
        } catch (IOException ioe) {
            LOG.fatal("Failed to read data from " + clientSocket, ioe);
            return false;
        }
        msgProcPool.execute(new Runnable() {
            public void run() {
                processRequest(b, listener, clientSocket);
            }
        });
        return true;
    }

    private static void processRequest(final byte[] b, final GridTransportListener listener, final Socket clientSocket) {
        final Object obj;
        try {
            obj = ObjectUtils.readObject(b);
        } catch (IOException ioe) {
            LOG.fatal("Failed to read an object from " + clientSocket, ioe);
            return;
        } catch (ClassNotFoundException cnfe) {
            LOG.fatal("Failed to read an object from " + clientSocket, cnfe);
            return;
        }
        // handle message
        final GridCommunicationMessage msg = (GridCommunicationMessage) obj;
        if(LOG.isDebugEnabled()) {
            LOG.debug("Recieved a GridCommunicationMessage [" + msg.getMessageId() + "] on "
                    + clientSocket);
        }
        listener.notifyListener(msg);
    }

}
