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
package gridool.memcached.binary;

import static gridool.memcached.binary.BinaryProtocol.OPCODE_GET;
import static gridool.memcached.binary.BinaryProtocol.OPCODE_GETK;
import static gridool.memcached.binary.BinaryProtocol.OPCODE_GETKQ;
import static gridool.memcached.binary.BinaryProtocol.OPCODE_GETQ;
import static gridool.memcached.binary.BinaryProtocol.OPCODE_QUIT;
import static gridool.memcached.binary.BinaryProtocol.OPCODE_QUITQ;
import static gridool.memcached.binary.BinaryProtocol.OPCODE_SET;
import static gridool.memcached.binary.BinaryProtocol.OPCODE_SETQ;
import gridool.memcached.MemcachedCommandHandler;
import gridool.memcached.binary.BinaryProtocol.Header;
import gridool.memcached.binary.BinaryProtocol.Packet;
import gridool.memcached.binary.BinaryProtocol.ResponseStatus;
import gridool.memcached.util.MemcachedUtils;
import gridool.memcached.util.VerboseListener;
import gridool.util.lang.ExceptionUtils;
import gridool.util.string.StringUtils;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
public final class BinaryRequestHandler extends SimpleChannelHandler {
    private static final Log LOG = LogFactory.getLog(BinaryRequestHandler.class);

    private final ChannelGroup acceptedChannels;
    private final MemcachedCommandHandler cmdHandler;

    private final ConcurrentMap<Integer, ChannelBuffer> corkedBuffers;

    public BinaryRequestHandler(ChannelGroup acceptedChannels, MemcachedCommandHandler cmdHandler) {
        super();
        this.acceptedChannels = acceptedChannels;
        this.cmdHandler = cmdHandler;
        this.corkedBuffers = new ConcurrentHashMap<Integer, ChannelBuffer>();
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
        final Header header = new Header(request.header);

        if(LOG.isDebugEnabled()) {
            LOG.debug("recieved memcached message: \n" + header);
        }

        final byte opcode = request.header.opcode;
        switch(opcode) {
            case OPCODE_GET:
            case OPCODE_GETQ:
            case OPCODE_GETK:
            case OPCODE_GETKQ: {
                handleGet(opcode, request, header, ctx, e);
                break;
            }
            case OPCODE_SET:
            case OPCODE_SETQ: {
                handleSet(opcode, request, header, ctx, e);
                break;
            }
            case OPCODE_QUITQ: {
                closeOnFlush(e.getChannel());
                break;
            }
            default: {
                LOG.warn("Unsupported opcode = " + BinaryProtocol.resolveName(opcode));
                if(!BinaryProtocol.noreply(opcode)) {
                    ChannelFuture f = sendError(opcode, ResponseStatus.NOT_SUPPORTED, header, e);
                    if(opcode == OPCODE_QUIT) {
                        f.addListener(ChannelFutureListener.CLOSE);
                    }
                }
            }
        }
    }

    /**
     * Request:
     *  MUST NOT have extras.
     *  MUST have key.
     *  MUST NOT have value.
     * Response (if found):
     *  MUST have extras.
     *  MAY have key.
     *  MAY have value.
     */
    private void handleGet(final byte opcode, final Packet request, final Header resHeader, final ChannelHandlerContext ctx, final MessageEvent e) {
        final byte[] key = new byte[request.header.keyLength];
        request.body.readBytes(key);
        byte[] storedValue = cmdHandler.handleGet(key);
        if(storedValue == null) {
            if(opcode == OPCODE_GETQ || opcode == OPCODE_GETKQ) {
                return;
            }
        }

        final int flags = MemcachedUtils.getFlags(storedValue);
        final byte[] value = MemcachedUtils.getValue(storedValue);

        resHeader.extraLength = 4;
        final int totalBody;
        final ChannelBuffer body;
        if(opcode == OPCODE_GETK || opcode == OPCODE_GETKQ) {
            resHeader.keyLength = (short) key.length;
            totalBody = 4 + key.length + (value == null ? 0 : value.length);
            body = ChannelBuffers.buffer(totalBody);
            body.writeInt(flags); // flags
            body.writeBytes(key);
        } else {
            totalBody = 4 + (value == null ? 0 : value.length);
            body = ChannelBuffers.buffer(totalBody);
            body.writeInt(flags); // flags
        }
        resHeader.totalBody = totalBody;
        if(value == null) {
            resHeader.status = ResponseStatus.KEY_NOT_FOUND.status;
        } else {
            body.writeBytes(value);
        }
        sendResponse(opcode, resHeader, body, e, StringUtils.toByteString(key));
    }

    /**
     * Request:
     *  MUST have extras.
     *      o  4 byte flags
     *      o  4 byte expiration time
     *  MUST have key.
     *  MUST have value.
     */
    private void handleSet(final byte opcode, final Packet request, final Header resHeader, final ChannelHandlerContext ctx, final MessageEvent e) {
        // request body (flags, expiration, key, value)
        int flags = request.body.readInt();
        int expiry = request.body.readInt();
        byte[] key = new byte[request.header.keyLength];
        request.body.readBytes(key);
        int valueLength = request.header.totalBody - request.header.extraLength
                - request.header.keyLength;

        byte[] storedValue = MemcachedUtils.makeInternalValue(flags, valueLength);
        int valueOffset = storedValue.length - valueLength;
        request.body.readBytes(storedValue, valueOffset, valueLength);

        cmdHandler.handleSet(key, storedValue, flags, expiry);
        if(opcode == OPCODE_SETQ) {
            return;
        }
        sendResponse(opcode, resHeader, null, e, StringUtils.toByteString(key));
    }

    private ChannelFuture sendError(final byte opcode, final ResponseStatus errcode, final Header reqHeader, final MessageEvent e) {
        assert (BinaryProtocol.noreply(opcode) == false) : BinaryProtocol.resolveName(opcode);

        Header newHeader = new Header(reqHeader);
        newHeader.status(errcode.status);
        ChannelBuffer responseHeader = ChannelBuffers.buffer(BinaryProtocol.HEADER_LENGTH);
        reqHeader.encode(responseHeader);

        Channel dst = e.getChannel();
        if(!corkedBuffers.isEmpty()) {
            uncorkResponses(newHeader.opaque, dst);
        }
        ChannelFuture f = dst.write(responseHeader);
        f.addListener(new VerboseListener("sendError [" + BinaryProtocol.resolveName(opcode)
                + "]: " + errcode));
        return f;
    }

    private void sendResponse(final byte opcode, final Header header, @Nullable final ChannelBuffer body, final MessageEvent e, final String key) {
        ChannelBuffer res = ChannelBuffers.buffer(BinaryProtocol.HEADER_LENGTH);
        header.encode(res);
        if(body != null) {
            res = ChannelBuffers.wrappedBuffer(res, body);
        }
        String opname = BinaryProtocol.resolveName(opcode);
        if(LOG.isDebugEnabled()) {
            LOG.debug("Start sending memcached response [" + opname + "] " + res.readableBytes()
                    + " bytes for key '" + key + "'\n" + header + '\n'
                    + Arrays.toString(res.toByteBuffer().array()));
        }
        if(BinaryProtocol.noreply(opcode)) {
            corkResponse(header.opaque, res);
        } else {
            Channel dst = e.getChannel();
            if(!corkedBuffers.isEmpty()) {
                uncorkResponses(header.opaque, dst);
            }
            dst.write(res).addListener(new VerboseListener("sendResponse [" + opname
                    + "] for key: " + key));
        }
    }

    private void corkResponse(final int opaque, @Nonnull final ChannelBuffer res) {
        ChannelBuffer prev = corkedBuffers.putIfAbsent(opaque, res);
        if(prev == null) {
            return;
        }
        for(;;) {
            ChannelBuffer merged = ChannelBuffers.wrappedBuffer(prev, res);
            ChannelBuffer curr = corkedBuffers.putIfAbsent(opaque, merged);
            if(curr == prev) {
                break;
            } else {
                prev = curr;
            }
        }
    }

    private void uncorkResponses(final int opaque, final Channel dst) {
        ChannelBuffer corkedBuffer = corkedBuffers.get(opaque);
        if(corkedBuffer != null) {
            dst.write(corkedBuffer);
        }
    }

    private static void closeOnFlush(final Channel ch) {
        if(ch.isConnected()) {
            ch.write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }

}
