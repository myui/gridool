/*
 * @(#)$Id$
 *
 * Copyright 2009-2010 Makoto YUI
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
package gridool.memcached.gateway;

import static gridool.memcached.binary.BinaryProtocol.*;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.Settings;
import gridool.memcached.binary.BinaryProtocol;
import gridool.memcached.binary.BinaryProtocol.Header;
import gridool.memcached.binary.BinaryProtocol.Packet;
import gridool.memcached.binary.BinaryProtocol.ResponseStatus;
import gridool.memcached.util.NettyChannelFutureFactory;
import gridool.memcached.util.VerboseListener;
import gridool.routing.GridRouter;
import gridool.util.concurrent.ExecutorFactory;
import gridool.util.lang.ExceptionUtils;
import gridool.util.pool.ConcurrentKeyedStackObjectPool;
import gridool.util.primitive.Primitives;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class BinaryCommandProxy extends SimpleChannelHandler {
    private static final Log LOG = LogFactory.getLog(BinaryCommandProxy.class);

    @Nonnull
    private final ChannelGroup acceptedChannels;
    @Nonnull
    private final GridRouter router;
    private final int dstPort;
    @Nonnull
    private final ClientBootstrap cb;
    @Nonnull
    private final ConcurrentKeyedStackObjectPool<SocketAddress, ChannelFuture> outboundChannelFutures;

    // This lock guards against the race condition that overrides the
    // OP_READ flag incorrectly.
    // See the related discussion: http://markmail.org/message/x7jc6mqx6ripynqf
    private final Object trafficLock = new Object();

    public BinaryCommandProxy(@Nonnull ChannelGroup acceptedChannels, @Nonnull GridResourceRegistry registry) {
        super();
        this.acceptedChannels = acceptedChannels;
        this.router = registry.getRouter();
        this.dstPort = Primitives.parseInt(Settings.get("gridool.memcached.server.port"), 11212);
        ChannelFactory cf = new NioClientSocketChannelFactory(ExecutorFactory.newCachedThreadPool("memc_gw-client-boss"), ExecutorFactory.newCachedThreadPool("memc_gw-client-worker"));
        this.cb = new ClientBootstrap(cf);
        NettyChannelFutureFactory factory = new NettyChannelFutureFactory(cb);
        this.outboundChannelFutures = new ConcurrentKeyedStackObjectPool<SocketAddress, ChannelFuture>("memc_proxy-channelfuture-pool", factory);
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        Channel inboundChannel = e.getChannel();
        acceptedChannels.add(inboundChannel);
    }

    @Override
    public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        final Channel outboundChannel = (Channel) ctx.getAttachment();
        if(outboundChannel == null) {
            return;
        }
        // If inboundChannel is not saturated anymore, continue accepting
        // the incoming traffic from the outboundChannel.
        final Channel inboundChannel = e.getChannel();
        synchronized(trafficLock) {
            if(inboundChannel.isWritable()) {
                outboundChannel.setReadable(true);
            }
        }
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        Channel inboundChannel = e.getChannel();
        acceptedChannels.remove(inboundChannel);
        outboundChannelFutures.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        LOG.error(e, ExceptionUtils.getRootCause(e.getCause()));
        closeOnFlush(e.getChannel());
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        final Packet request = (Packet) e.getMessage();
        Header header = request.getHeader();

        if(LOG.isDebugEnabled()) {
            LOG.debug("recieved memcached message: \n" + header);
        }

        final byte opcode = header.getOpcode();
        switch(opcode) {
            // those who MUST have key
            case OPCODE_GET:
            case OPCODE_SET:
            case OPCODE_ADD:
            case OPCODE_REPLACE:
            case OPCODE_DELETE:
            case OPCODE_INCREMENT:
            case OPCODE_DECREMENT:
            case OPCODE_GETK:
            case OPCODE_APPEND:
            case OPCODE_PREPEND: {
                final byte[] key = getKey(header, request.getBody());
                if(key == null) {
                    LOG.error("Illegal key length was detected");
                    sendError(opcode, ResponseStatus.INVALID_ARGUMENTS, header, e);
                } else {
                    xferMemcacheCmd(opcode, false, header, request.getBody(), key, ctx, e);
                }
                break;
            }
                // Synchronously send operation because quiet operation MUST be ordered among servers.
            case OPCODE_GETQ:
            case OPCODE_GETKQ: {
                byte syncOp = asSyncOp(opcode);
                final byte[] key = getKey(header, request.getBody());
                if(key == null) {
                    LOG.error("Illegal key length was detected");
                    sendError(syncOp, ResponseStatus.INVALID_ARGUMENTS, header, e);
                } else {
                    xferMemcacheCmd(syncOp, false, header, request.getBody(), key, ctx, e);
                }
                break;
            }
                // Quit operation (error response only)
            case OPCODE_SETQ:
            case OPCODE_ADDQ:
            case OPCODE_REPLACEQ:
            case OPCODE_DELETEQ:
            case OPCODE_INCREMENTQ:
            case OPCODE_DECREMENTQ:
            case OPCODE_APPENDQ:
            case OPCODE_PREPENDQ: {
                byte syncOp = asSyncOp(opcode);
                final byte[] key = getKey(header, request.getBody());
                if(key == null) {
                    LOG.error("Illegal key length was detected");
                    sendError(syncOp, ResponseStatus.INVALID_ARGUMENTS, header, e);
                } else {
                    xferMemcacheCmd(syncOp, true, header, request.getBody(), key, ctx, e);
                }
                break;
            }
                // need to broadcast
            case OPCODE_FLUSH:
            case OPCODE_NOOP:
            case OPCODE_STAT:
            case OPCODE_FLUSHQ: {
                LOG.warn("Unsupported opcode = " + BinaryProtocol.resolveName(opcode));
                sendError(opcode, ResponseStatus.NOT_SUPPORTED, header, e);
                break;
            }
                // no need to hand over
            case OPCODE_QUITQ: {
                closeOnFlush(e.getChannel());
                break;
            }
            case OPCODE_QUIT: {
                LOG.warn("Unsupported opcode = " + BinaryProtocol.resolveName(opcode));
                ChannelFuture f = sendError(opcode, ResponseStatus.NOT_SUPPORTED, header, e);
                f.addListener(ChannelFutureListener.CLOSE);
                break;
            }
            case OPCODE_VERSION:
            default: {
                LOG.warn("Unsupported opcode = " + BinaryProtocol.resolveName(opcode));
                sendError(opcode, ResponseStatus.NOT_SUPPORTED, header, e);
            }
        }
    }

    private void xferMemcacheCmd(final byte opcode, final boolean ignoreSucess, final Header reqHeader, final ChannelBuffer body, final byte[] key, final ChannelHandlerContext ctx, final MessageEvent e) {
        int bodylen = body.readableBytes();
        final ChannelBuffer mcmd = ChannelBuffers.buffer(BinaryProtocol.HEADER_LENGTH + bodylen);
        reqHeader.encode(mcmd);
        mcmd.setByte(1, opcode);
        if(bodylen > 0) {
            body.readBytes(mcmd);
        }

        // Suspend incoming traffic until connected to the remote host.
        final Channel inboundChannel = e.getChannel();
        inboundChannel.setReadable(false);
           
        cb.getPipeline().addLast("memc_proxy-outbound-handler", new OutboundHandler(inboundChannel, ignoreSucess, trafficLock));

        // Start the connection attempt.
        SocketAddress sockAddr = getSocket(key);
        ChannelFuture f = outboundChannelFutures.borrowObject(sockAddr);
        ConnectionAttemptListener connListener = new ConnectionAttemptListener(inboundChannel);
        if(f.isDone()) {
            connListener.operationComplete(f);
        } else {
            f.addListener(connListener); // REVIEWME connection may success before setting this listener
        }

        final VerboseListener reqListener = new VerboseListener("sendRequest ["
                + BinaryProtocol.resolveName(opcode) + "] for key: " + key);
        final Channel outboundChannel = f.getChannel();
        ctx.setAttachment(outboundChannel);
        synchronized(trafficLock) {
            outboundChannel.write(mcmd).addListener(reqListener);
            // If outboundChannel is saturated, do not read until notified in
            // OutboundHandler.channelInterestChanged().
            if(!outboundChannel.isWritable()) {
                inboundChannel.setReadable(false);
            }
        }
    }

    private static ChannelFuture sendError(final byte opcode, final ResponseStatus errcode, final Header reqHeader, final MessageEvent e) {
        Header newHeader = new Header(reqHeader);
        newHeader.status(errcode.status);
        ChannelBuffer responseHeader = ChannelBuffers.buffer(BinaryProtocol.HEADER_LENGTH);
        reqHeader.encode(responseHeader);
        Channel dst = e.getChannel();
        String opname = BinaryProtocol.resolveName(opcode);
        ChannelFuture f = dst.write(responseHeader);
        f.addListener(new VerboseListener("sendError [" + opname + "]: " + errcode));
        return f;
    }

    @Nullable
    private static byte[] getKey(Header header, ChannelBuffer body) {
        int extralen = header.getExtraLength();
        int keylen = header.getKeyLength();

        // check illegal arguments
        if(keylen < 0) {
            return null;
        }
        if(extralen < 0) {
            return null;
        }

        // corner case for zero-length key
        if(keylen == 0) {
            return new byte[0];
        }

        byte[] key = new byte[keylen];
        body.getBytes(extralen, key, 0, keylen);
        return key;
    }

    private SocketAddress getSocket(final byte[] key) {
        GridNode node = router.selectNode(key);
        InetAddress addr = node.getPhysicalAdress();
        return new InetSocketAddress(addr, dstPort);
    }

    private static final class ConnectionAttemptListener implements ChannelFutureListener {
        private final Channel inboundChannel;

        ConnectionAttemptListener(Channel inboundChannel) {
            this.inboundChannel = inboundChannel;
        }

        public void operationComplete(ChannelFuture future) {
            if(future.isSuccess()) {
                // Connection attempt succeeded:
                // Begin to accept incoming traffic.
                inboundChannel.setReadable(true);
            } else {
                // Close the connection if the connection attempt has failed.
                inboundChannel.close();
            }
        }
    }

    private static final class OutboundHandler extends SimpleChannelUpstreamHandler {

        private final Channel inboundChannel;
        private final boolean ignoreSucess;
        private final Object trafficLock;

        OutboundHandler(Channel inboundChannel, boolean ignoreSucess, Object trafficLock) {
            this.inboundChannel = inboundChannel;
            this.ignoreSucess = ignoreSucess;
            this.trafficLock = trafficLock;
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e)
                throws Exception {
            final ChannelBuffer resp = (ChannelBuffer) e.getMessage();
            short status = resp.getShort(6);
            final Channel outboundChannel = e.getChannel();
            synchronized(trafficLock) {
                if(status == 0) {
                    if(!ignoreSucess) {
                        inboundChannel.write(resp);
                    }
                }
                // If inboundChannel is saturated, do not read until notified in
                // HexDumpProxyInboundHandler.channelInterestChanged().
                if(!inboundChannel.isWritable()) {
                    outboundChannel.setReadable(false);
                }
            }
        }

        @Override
        public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e)
                throws Exception {
            // If outboundChannel is not saturated anymore, continue accepting
            // the incoming traffic from the inboundChannel.
            final Channel outboundChannel = e.getChannel();
            synchronized(trafficLock) {
                if(outboundChannel.isWritable()) {
                    inboundChannel.setReadable(true);
                }
            }
        }

        @Override
        public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            closeOnFlush(inboundChannel);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            LOG.error(e, ExceptionUtils.getRootCause(e.getCause()));
            Channel outboundChannel = e.getChannel();
            closeOnFlush(outboundChannel);
        }
    }

    private static void closeOnFlush(final Channel ch) {
        if(ch.isConnected()) {
            ch.write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }

}
