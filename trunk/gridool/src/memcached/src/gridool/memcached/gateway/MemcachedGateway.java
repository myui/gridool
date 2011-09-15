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

import gridool.GridException;
import gridool.GridResourceRegistry;
import gridool.GridService;
import gridool.Settings;
import gridool.memcached.binary.BinaryPipelineFactory;
import gridool.util.concurrent.ExecutorFactory;
import gridool.util.net.NetUtils;
import gridool.util.primitive.Primitives;

import java.net.InetSocketAddress;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandler;
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
public final class MemcachedGateway implements GridService {

    private final GridResourceRegistry registry;

    public MemcachedGateway(GridResourceRegistry registry) {
        this.registry = registry;
    }

    @Override
    public String getServiceName() {
        return MemcachedGateway.class.getSimpleName();
    }

    @Override
    public boolean isDaemon() {
        return true;
    }

    @Override
    public void start() throws GridException {
        final ChannelFactory channelFactory = new NioServerSocketChannelFactory(ExecutorFactory.newCachedThreadPool("memcached-gateway-server-boss"), ExecutorFactory.newCachedThreadPool("memcached-gateway-server-worker"));
        final ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
        bootstrap.setOption("child.tcpNoDelay", true); // recommended
        bootstrap.setOption("child.reuseAddress", true);
        bootstrap.setOption("child.keepAlive", true);

        final ChannelGroup acceptedChannels = new DefaultChannelGroup("all_client_connections");
        ChannelHandler handler = new BinaryCommandProxy(acceptedChannels, registry);
        bootstrap.setPipelineFactory(new BinaryPipelineFactory(handler));

        int port = Primitives.parseInt(Settings.get("gridool.memcached.gateway.port"), 11211);
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
