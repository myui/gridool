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
package gridool.memcached.server;

import gridool.GridException;
import gridool.GridResourceRegistry;
import gridool.GridService;
import gridool.Settings;
import gridool.memcached.MemcachedCommandHandler;
import gridool.memcached.binary.BinaryPipelineFactory;
import gridool.memcached.binary.BinaryRequestHandler;
import gridool.memcached.store.MemcachedStorageFactory;
import gridool.util.concurrent.ExecutorFactory;
import gridool.util.net.NetUtils;
import gridool.util.primitive.Primitives;

import java.net.InetSocketAddress;

import javax.annotation.Nonnull;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class MemcachedServer implements GridService {

    @Nonnull
    private final GridResourceRegistry registry;

    public MemcachedServer(@Nonnull GridResourceRegistry registry) {
        this.registry = registry;
    }

    public static void main(String[] args) throws GridException {
        new MemcachedServer(null).start();
    }

    @Override
    public String getServiceName() {
        return MemcachedServer.class.getSimpleName();
    }

    @Override
    public boolean isDaemon() {
        return true;
    }

    @Override
    public void start() throws GridException {
        final ChannelFactory channelFactory = new NioServerSocketChannelFactory(ExecutorFactory.newCachedThreadPool("memcached-server-boss"), ExecutorFactory.newCachedThreadPool("memcached-server-worker"));
        final ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
        bootstrap.setOption("child.tcpNoDelay", true); // recommended
        bootstrap.setOption("child.reuseAddress", true);
        bootstrap.setOption("child.keepAlive", true);

        final ChannelGroup acceptedChannels = new DefaultChannelGroup("all_proxy_connections");
        MemcachedCommandHandler cmdhandler = MemcachedStorageFactory.create(registry);
        BinaryRequestHandler handler = new BinaryRequestHandler(acceptedChannels, cmdhandler);
        bootstrap.setPipelineFactory(new BinaryPipelineFactory(handler));

        int port = Primitives.parseInt(Settings.get("gridool.memcached.server.port"), 11212);
        final Channel serverChannel = bootstrap.bind(new InetSocketAddress(NetUtils.getLocalHost(), port));

        Runnable shutdownRunnable = new Runnable() {
            public void run() {
                serverChannel.close().awaitUninterruptibly(); // close server socket
                acceptedChannels.close().awaitUninterruptibly(); // close client connections
                channelFactory.releaseExternalResources(); // stop the boss and worker threads of ChannelFactory
            }
        };
        Runtime.getRuntime().addShutdownHook(new Thread(shutdownRunnable));
    }

    @Override
    public void stop() throws GridException {}

}
