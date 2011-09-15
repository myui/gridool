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

import static gridool.memcached.binary.BinaryProtocol.GET_EXTRA_LENGTH;
import static gridool.memcached.binary.BinaryProtocol.HEADER_LENGTH;
import static gridool.memcached.binary.BinaryProtocol.MAGIC_BYTE_REQUEST;
import static gridool.memcached.binary.BinaryProtocol.OPCODE_GET;
import static gridool.memcached.binary.BinaryProtocol.OPCODE_SET;
import static gridool.memcached.binary.BinaryProtocol.SET_EXTRA_LENGTH;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.Settings;
import gridool.memcached.MemcachedCommandHandler;
import gridool.memcached.binary.BinaryProtocol.Header;
import gridool.memcached.binary.BinaryProtocol.ResponseStatus;
import gridool.routing.GridRouter;
import gridool.util.net.PoolableSocketChannelFactory;
import gridool.util.nio.NIOUtils;
import gridool.util.pool.ConcurrentKeyedStackObjectPool;
import gridool.util.primitive.Primitives;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

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
public final class MemcachedProxyHandler implements MemcachedCommandHandler {
    private static final Log LOG = LogFactory.getLog(MemcachedProxyHandler.class);

    @Nonnull
    private final GridRouter router;
    private final int dstPort;

    private final ConcurrentKeyedStackObjectPool<SocketAddress, ByteChannel> sockPool;

    public MemcachedProxyHandler(GridResourceRegistry registry) {
        this.router = registry.getRouter();
        this.dstPort = Primitives.parseInt(Settings.get("gridool.memcached.server.port"), 11212);
        PoolableSocketChannelFactory factory = new PoolableSocketChannelFactory(false, true);
        this.sockPool = new ConcurrentKeyedStackObjectPool<SocketAddress, ByteChannel>("memcached-proxy-sockpool", factory);
    }

    @Override
    public byte[] handleGet(byte[] key) {
        final ByteBuffer reqPacket = ByteBuffer.allocate(HEADER_LENGTH + key.length);
        // request header
        Header header = new Header(MAGIC_BYTE_REQUEST, OPCODE_GET);
        header.setBodyLength(GET_EXTRA_LENGTH, key.length, 0);
        header.encode(reqPacket);
        // request body (key)
        reqPacket.put(key);
        reqPacket.flip();

        final byte[] value;
        final SocketAddress sockAddr = getSocket(key);
        final ByteChannel channel = sockPool.borrowObject(sockAddr);
        try {
            // handle request
            NIOUtils.writeFully(channel, reqPacket);

            // handle response header
            ByteBuffer responseHeaderPacket = ByteBuffer.allocate(HEADER_LENGTH);
            NIOUtils.readFully(channel, responseHeaderPacket);
            responseHeaderPacket.flip();
            // handle response body 
            int totalBody = responseHeaderPacket.getInt(8);
            int keyLen = responseHeaderPacket.getShort(2);
            int extraLen = responseHeaderPacket.get(4);
            int bodyPos = extraLen + keyLen;
            int bodyLen = totalBody - bodyPos;
            if(bodyLen <= 0) {
                return null;
            }
            ByteBuffer responseBodyPacket = ByteBuffer.allocate(totalBody);
            NIOUtils.readFully(channel, responseBodyPacket);
            responseBodyPacket.flip();
            value = new byte[bodyLen];
            responseBodyPacket.get(value, 0, bodyLen);
        } catch (IOException e) {
            LOG.error(e);
            return null;
        } finally {
            sockPool.returnObject(sockAddr, channel);
        }
        return value;
    }

    @Override
    public short handleSet(byte[] key, byte[] value, int flags, int expiry) {
        final ByteBuffer reqPacket = ByteBuffer.allocate(HEADER_LENGTH + SET_EXTRA_LENGTH
                + key.length + value.length);
        // request header
        Header header = new Header(MAGIC_BYTE_REQUEST, OPCODE_SET);
        header.setBodyLength(SET_EXTRA_LENGTH, key.length, value.length);
        header.encode(reqPacket);
        // request body (flags, expiration, key, value)
        reqPacket.putInt(flags);
        reqPacket.putInt(expiry);
        reqPacket.put(key);
        reqPacket.put(value);
        reqPacket.flip();

        final ByteBuffer resPacket = ByteBuffer.allocate(HEADER_LENGTH);
        final SocketAddress sockAddr = getSocket(key);
        final ByteChannel channel = sockPool.borrowObject(sockAddr);
        try {
            NIOUtils.writeFully(channel, reqPacket);
            NIOUtils.readFully(channel, resPacket);
        } catch (IOException e) {
            LOG.error(e);
            return ResponseStatus.UNKNOWN.status;
        } finally {
            sockPool.returnObject(sockAddr, channel);
        }
        resPacket.flip();
        short status = resPacket.getShort(6);
        return status;
    }

    private SocketAddress getSocket(final byte[] key) {
        GridNode node = router.selectNode(key);
        InetAddress addr = node.getPhysicalAdress();
        return new InetSocketAddress(addr, dstPort);
    }

}
