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
import gridool.memcached.util.VerboseListener;
import gridool.routing.GridRouter;
import gridool.util.lang.ExceptionUtils;
import gridool.util.net.PoolableSocketChannelFactory;
import gridool.util.nio.NIOUtils;
import gridool.util.pool.ConcurrentKeyedStackObjectPool;
import gridool.util.primitive.Primitives;
import gridool.util.string.StringUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;

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

    private final ConcurrentKeyedStackObjectPool<SocketAddress, SocketChannel> sockPool;

    public BinaryCommandProxy(@Nonnull ChannelGroup acceptedChannels, @Nonnull GridResourceRegistry registry) {
        super();
        this.acceptedChannels = acceptedChannels;
        this.router = registry.getRouter();
        this.dstPort = Primitives.parseInt(Settings.get("gridool.memcached.server.port"), 11212);
        PoolableSocketChannelFactory<SocketChannel> factory = new PoolableSocketChannelFactory<SocketChannel>(false, true);
        this.sockPool = new ConcurrentKeyedStackObjectPool<SocketAddress, SocketChannel>("memcached-proxy-sockpool", factory);
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        acceptedChannels.add(e.getChannel());
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
            case OPCODE_GETK:
            case OPCODE_GETQ:
            case OPCODE_GETKQ:
            case OPCODE_SET:
            case OPCODE_SETQ: {
                final byte[] key = getKey(header, request.getBody());
                if(key == null) {
                    LOG.error("Illegal key length was provided");
                    sendError(opcode, ResponseStatus.INVALID_ARGUMENTS, header, e);
                } else {
                    xferMemcacheCmd(opcode, header, request.getBody(), key, e);
                }
                break;
            }
            case OPCODE_ADD:
            case OPCODE_REPLACE:
            case OPCODE_DELETE:
            case OPCODE_INCREMENT:
            case OPCODE_DECREMENT:
            case OPCODE_APPEND:
            case OPCODE_PREPEND:
            case OPCODE_ADDQ:
            case OPCODE_REPLACEQ:
            case OPCODE_DELETEQ:
            case OPCODE_INCREMENTQ:
            case OPCODE_DECREMENTQ:
            case OPCODE_APPENDQ:
            case OPCODE_PREPENDQ: {
                LOG.warn("Unsupported opcode = " + BinaryProtocol.resolveName(opcode));
                sendError(opcode, ResponseStatus.NOT_SUPPORTED, header, e);
                break;
            }
                // need to broadcast
            case OPCODE_NOOP: {
                flush(e.getChannel());
                break;
            }
            case OPCODE_FLUSH:
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

    private void xferMemcacheCmd(final byte opcode, final Header reqHeader, final ChannelBuffer body, final byte[] key, final MessageEvent e) {
        int bodylen = body.readableBytes();
        final ByteBuffer cmd = ByteBuffer.allocate(BinaryProtocol.HEADER_LENGTH + bodylen);
        reqHeader.encode(cmd);
        if(bodylen > 0) {
            body.readBytes(cmd);
        }
        cmd.flip();

        final SocketAddress sockAddr = getSocket(key);
        final SocketChannel channel = sockPool.borrowObject(sockAddr);
        try {
            NIOUtils.writeFully(channel, cmd);
            xferResponse(opcode, channel, e.getChannel(), StringUtils.toByteString(key));
        } catch (IOException ioe) {
            LOG.error(ioe);
            sendError(reqHeader.getOpcode(), ResponseStatus.INTERNAL_ERROR, reqHeader, e);
        } finally {
            sockPool.returnObject(sockAddr, channel);
        }
    }

    private static void xferResponse(final byte opcode, final SocketChannel src, final Channel dst, final String key)
            throws IOException {
        ByteBuffer headerBuf = ByteBuffer.allocate(BinaryProtocol.HEADER_LENGTH);
        int headerRead = NIOUtils.readFully(src, headerBuf, BinaryProtocol.HEADER_LENGTH);
        assert (headerRead == BinaryProtocol.HEADER_LENGTH) : headerRead;
        headerBuf.flip();

        if(BinaryProtocol.surpressSuccessResponse(opcode)) {
            // piggyback will never happens 
            final short status = headerBuf.getShort(6);
            if(status == 0) {
                return;
            }
        }

        ChannelBuffer res;
        int totalBody = headerBuf.getInt(8);
        if(totalBody > 0) {
            ByteBuffer bodyBuf = ByteBuffer.allocate(totalBody);
            int bodyRead = NIOUtils.readFully(src, bodyBuf, totalBody);
            assert (bodyRead == totalBody) : "bodyRead (" + bodyRead + ") != totalBody ("
                    + totalBody + ")";
            bodyBuf.flip();
            res = ChannelBuffers.wrappedBuffer(headerBuf, bodyBuf);
        } else {
            res = ChannelBuffers.wrappedBuffer(headerBuf);
        }
        String opname = BinaryProtocol.resolveName(headerBuf.get(1));
        if(LOG.isDebugEnabled()) {
            Header header = new Header();
            header.decode(headerBuf);
            LOG.debug("Start sending memcached response [" + opname + "] " + res.readableBytes()
                    + " bytes for key '" + key + "'\n" + header + '\n'
                    + Arrays.toString(res.toByteBuffer().array()));
        }
        dst.write(res).addListener(new VerboseListener("sendResponse [" + opname + "] for key: "
                + key));
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

    private SocketAddress getSocket(final byte[] key) {
        GridNode node = router.selectNode(key);
        InetAddress addr = node.getPhysicalAdress();
        return new InetSocketAddress(addr, dstPort);
    }

    private static void flush(final Channel ch) {
        if(ch.isConnected()) {
            ch.write(ChannelBuffers.EMPTY_BUFFER);
        }
    }

    private static void closeOnFlush(final Channel ch) {
        if(ch.isConnected()) {
            ch.write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }

}
