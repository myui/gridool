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
package gridool.util.xfer;

import gridool.util.concurrent.DirectExecutorService;
import gridool.util.concurrent.ExecutorFactory;
import gridool.util.lang.PrintUtils;
import gridool.util.net.NetUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;

import javax.annotation.CheckForNull;
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
public final class TransferServer implements Runnable {
    private static final Log LOG = LogFactory.getLog(TransferServer.class);

    private final ExecutorService execPool;
    private final TransferRequestListener handler;
    private final ServerSocketChannel serverChannel;

    public TransferServer(@Nonnull int numWorkers, @CheckForNull TransferRequestListener handler) {
        this(numWorkers, Thread.NORM_PRIORITY, handler);
    }

    public TransferServer(@Nonnull int numWorkers, int threadPriority, @CheckForNull TransferRequestListener handler) {
        this((numWorkers == 0) ? new DirectExecutorService()
                : ExecutorFactory.newFixedThreadPool(numWorkers, "XferRequestHandler", threadPriority, true), handler);
    }

    public TransferServer(@Nonnull ExecutorService execPool, @CheckForNull TransferRequestListener handler) {
        if(handler == null) {
            throw new IllegalArgumentException();
        }
        this.execPool = execPool;
        this.handler = handler;
        try {
            this.serverChannel = ServerSocketChannel.open();
        } catch (IOException e) {
            LOG.error(e);
            throw new IllegalStateException(e);
        }
    }

    /**
     * @return binded sock address
     */
    public InetSocketAddress setup() throws IOException {
        ServerSocket servSocket = serverChannel.socket();
        servSocket.setReuseAddress(true);
        InetSocketAddress sockaddr = NetUtils.getAnyLocalInetSocketAddress();
        servSocket.bind(sockaddr);
        return sockaddr;
    }

    public void setup(int port) throws IOException {
        ServerSocket servSocket = serverChannel.socket();
        servSocket.setReuseAddress(true);
        InetAddress addr = NetUtils.getLocalHost(false);
        InetSocketAddress sockaddr = new InetSocketAddress(addr, port);
        servSocket.bind(sockaddr);
    }

    public void run() {
        final ExecutorService execPool = this.execPool;
        final TransferRequestListener handler = this.handler;
        try {
            while(true) {
                SocketChannel channel = serverChannel.accept();
                execPool.execute(new RequestHandler(channel, handler));
            }
        } catch (ClosedByInterruptException interrupted) {
            if(LOG.isDebugEnabled()) {
                LOG.debug("Avoidable interrupt happened (Normal case): " + interrupted.getMessage());
            }
        } catch (IOException ioe) {
            LOG.error(ioe);
        } catch (Throwable th) {
            LOG.error(th);
        } finally {
            execPool.shutdown();
            try {
                serverChannel.close();
            } catch (IOException ie) {
                if(LOG.isDebugEnabled()) {
                    LOG.debug(PrintUtils.prettyPrintStackTrace(ie, -1));
                }
            }
        }
    }

    private static final class RequestHandler implements Runnable {

        private final SocketChannel channel;
        private final TransferRequestListener handler;

        RequestHandler(SocketChannel channel, TransferRequestListener handler) {
            this.channel = channel;
            this.handler = handler;
        }

        public void run() {
            final Socket socket = channel.socket();
            try {
                handler.handleRequest(channel, socket);
            } catch (Throwable e) {
                LOG.error(PrintUtils.prettyPrintStackTrace(e, -1));
            } finally {
                NetUtils.closeQuietly(socket);
                NetUtils.closeQuietly(channel);
            }

        }

    }
}
