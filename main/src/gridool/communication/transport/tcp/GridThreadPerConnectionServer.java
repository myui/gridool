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
import gridool.communication.GridTransportListener;
import gridool.communication.transport.GridTransportServer;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.concurrent.ExecutorFactory;
import xbird.util.lang.PrintUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridThreadPerConnectionServer implements GridTransportServer {
    private static final Log LOG = LogFactory.getLog(GridThreadPerConnectionServer.class);

    @Nonnull
    private final GridConfiguration config;

    private GridTransportListener listener;
    private AcceptorThread acceptor;

    public GridThreadPerConnectionServer(@Nonnull GridConfiguration config) {
        this.config = config;
    }

    public void setListener(@Nonnull GridTransportListener listener) {
        this.listener = listener;
    }

    public void start() throws GridException {
        if(listener == null) {
            throw new IllegalStateException("GridTransportListener is not set");
        }

        final int port = config.getTransportServerPort();
        final ServerSocket ss;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
        } catch (IOException e) {
            String errmsg = "Could not create ServerSocket on port: " + port;
            LOG.error(errmsg, e);
            throw new GridException(errmsg);
        }

        if(LOG.isInfoEnabled()) {
            LOG.info("GridThreadPerConnectionServer is started at port: " + port);
        }

        final int readers = config.getSelectorReadThreadsCount();
        final ExecutorService execPool = ExecutorFactory.newCachedThreadPool(readers, 60L, "GridThreadPerConnectionServer#RequestHandler", false);
        final int msgProcs = config.getMessageProcessorPoolSize();
        final ExecutorService msgProcPool = ExecutorFactory.newFixedThreadPool(msgProcs, "OioMessageProcessor", false);
        this.acceptor = new AcceptorThread(ss, listener, execPool, msgProcPool);

        acceptor.start();
    }

    public void close() throws IOException {
        acceptor.close();
    }

    private static final class AcceptorThread extends Thread implements Closeable {
        final ServerSocket serverSocket;
        final GridTransportListener listener;
        final ExecutorService execPool;
        final ExecutorService msgProcPool;

        public AcceptorThread(ServerSocket socket, GridTransportListener listener, ExecutorService execPool, ExecutorService msgProcPool) {
            super("GridThreadPerConnectionServer#AcceptorThread");
            this.serverSocket = socket;
            this.listener = listener;
            this.execPool = execPool;
            this.msgProcPool = msgProcPool;
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                while(!Thread.interrupted()) {
                    Socket clientSocket = serverSocket.accept();
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("Accepted a new connection: " + clientSocket);
                    }
                    execPool.execute(new RequestHandler(clientSocket, listener, msgProcPool));
                }
            } catch (IOException ioe) {
                LOG.error(PrintUtils.prettyPrintStackTrace(ioe, -1));
            } catch (Throwable e) {
                LOG.error(PrintUtils.prettyPrintStackTrace(e, -1));
            } finally {
                execPool.shutdown();
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug(PrintUtils.prettyPrintStackTrace(e, -1));
                    }
                }
            }
        }

        public void close() throws IOException {
            interrupt();
            execPool.shutdown();
            serverSocket.close();
        }
    }

    private static final class RequestHandler implements Runnable {
        final Socket clientSocket;
        final GridTransportListener listener;
        final ExecutorService msgProcPool;

        public RequestHandler(@Nonnull Socket socket, @Nonnull GridTransportListener listener, ExecutorService msgProcPool) {
            this.clientSocket = socket;
            this.listener = listener;
            this.msgProcPool = msgProcPool;
        }

        public void run() {
            try {
                while(!clientSocket.isClosed()) {
                    if(!GridMasterSlaveWorkerServer.handleConnection(clientSocket, listener, msgProcPool)) {
                        break;
                    }
                }
            } catch (Throwable e) {
                LOG.error(PrintUtils.prettyPrintStackTrace(e, -1));
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug(PrintUtils.prettyPrintStackTrace(e, -1));
                    }
                }
            }
        }
    }
}
