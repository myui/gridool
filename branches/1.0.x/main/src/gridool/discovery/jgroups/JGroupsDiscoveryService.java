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
package gridool.discovery.jgroups;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridNodeMetrics;
import gridool.GridResourceRegistry;
import gridool.communication.payload.GridNodeInfo;
import gridool.discovery.DiscoveryServiceBase;
import gridool.discovery.GridDiscoveryMessage;
import gridool.metrics.GridNodeMetricsProvider;
import gridool.metrics.GridNodeMetricsService;
import gridool.util.GridUtils;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.ChannelListener;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.UDP;
import org.jgroups.stack.IpAddress;

import xbird.config.Settings;
import xbird.util.net.NetUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class JGroupsDiscoveryService extends DiscoveryServiceBase {
    private static final Log LOG = LogFactory.getLog(JGroupsDiscoveryService.class);

    private static final String JGROUPS_DOMAIN_NAME = "JGroups";
    private static final String JGROUPS_CONFIG_FILE = Settings.get("gridool.jgroups.config_file", "jgroups/jgroups.xml");
    private static final String CHANNEL_NAME = Settings.get("gridool.jgroups.channel.name", "gridool.ch1");

    @Nonnull
    private final GridNodeMetricsProvider metricsProvider;
    @Nonnull
    private final GridNodeInfo localTransportNodeInfo;

    private JChannel channel = null;
    private MBeanServer jmx = null;

    @GuardedBy("lock")
    private final Set<Address> members = new HashSet<Address>();
    @GuardedBy("lock")
    private final Set<Address> suspectedMembers = new HashSet<Address>();

    private final Object lock = new Object();

    private MetricsSender metricsSender = null;

    public JGroupsDiscoveryService(@Nonnull GridResourceRegistry registry, @Nonnull GridConfiguration config) {
        super(config);
        GridNodeMetricsService merticsServ = registry.getNodeMetricsService();
        this.metricsProvider = merticsServ.getMetricsProvider();
        this.localTransportNodeInfo = GridUtils.getLocalNode(config);
    }

    public void start() throws GridException {
        final boolean joinToMembership = config.isJoinToMembership();
        this.channel = link(joinToMembership);

        if(joinToMembership) {
            this.jmx = registerMBeans(channel);
            MetricsSender sender = new MetricsSender(this, channel, metricsProvider, config, 5000);
            sender.start();
            this.metricsSender = sender;
        }
    }

    private JChannel link(boolean joinToMembership) throws GridException {
        final JChannel channel;
        try {
            channel = new JChannel(JGROUPS_CONFIG_FILE);
            if(joinToMembership) {
                Map<String, byte[]> m = new HashMap<String, byte[]>(1);
                m.put("additional_data", localTransportNodeInfo.toBytes());
                channel.down(new Event(Event.CONFIG, m));
            } else {
                UDP udp = (UDP) channel.getProtocolStack().findProtocol(UDP.class);
                if(udp != null) {
                    int port = NetUtils.getAvialablePort(45567);
                    udp.setBindPort(port);
                }
            }

            channel.setOpt(Channel.LOCAL, Boolean.FALSE); // disable local message delivery
            channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE); // a shunned channel will leave the group and then try to automatically re-join

            channel.addChannelListener(new JgroupsChannelListener());
            channel.setReceiver(new JGroupsMembershipHandler());

            channel.connect(CHANNEL_NAME);
        } catch (ChannelException e) {
            LOG.error(e.getMessage(), e);
            throw new GridException(e);
        }
        return channel;
    }

    private static MBeanServer registerMBeans(JChannel channel) throws GridException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            JmxConfigurator.registerChannel(channel, server, JGROUPS_DOMAIN_NAME, channel.getClusterName(), true);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new GridException(e);
        }
        return server;
    }

    /**
     * @link http://wiki.jboss.org/wiki/JGroupsJChannel
     */
    public void stop() throws GridException {
        channel.close();

        if(metricsSender != null) {
            unregisterMBean(jmx, channel);
            metricsSender.interrupt();
            this.metricsSender = null;
        }
    }

    private static void unregisterMBean(@Nonnull MBeanServer jmx, @Nonnull JChannel channel) {
        final ObjectName channelMBeanName = GridUtils.makeMBeanName(JGROUPS_DOMAIN_NAME, "channel", CHANNEL_NAME);
        final String protocolMBeanName = GridUtils.makeMBeanNameString(JGROUPS_DOMAIN_NAME, "protocol", CHANNEL_NAME);
        try {
            JmxConfigurator.unregisterChannel(jmx, channelMBeanName);
            JmxConfigurator.unregister(jmx, protocolMBeanName);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private final class JgroupsChannelListener implements ChannelListener {

        public JgroupsChannelListener() {}

        public void channelConnected(Channel channel) {
            if(LOG.isDebugEnabled()) {
                LOG.debug("JGroups channel has connected: " + channel.getClusterName());
            }

            final Address addr = channel.getLocalAddress();
            final JGroupsNode transportNode = new JGroupsNode(localTransportNodeInfo);

            synchronized(lock) {
                if(members.add(addr)) {
                    GridNodeMetrics metrics = metricsProvider.getMetrics();
                    transportNode.setMetrics(metrics);
                    handleJoin(transportNode);
                } else {
                    LOG.warn("Adress already exists in the member list: " + addr);
                }
            }
        }

        public void channelClosed(Channel channel) {
            if(LOG.isDebugEnabled()) {
                LOG.debug("JGroups channel has closed: " + channel.getClusterName());
            }
            synchronized(lock) {
                handleClose();
            }
        }

        public void channelDisconnected(Channel channel) {
            if(LOG.isDebugEnabled()) {
                LOG.debug("JGroups channel has disconnected: " + channel.getClusterName());
            }
        }

        public void channelReconnected(Address addr) {
            if(LOG.isDebugEnabled()) {
                LOG.debug("JGroups channel has reconnected: " + channel.getClusterName());
            }
        }

        public void channelShunned() {
            if(LOG.isDebugEnabled()) {
                LOG.debug("JGroups channel has shunned: " + channel.getClusterName());
            }
            synchronized(lock) {
                Address localAddess = channel.getLocalAddress();
                if(members.remove(localAddess)) {
                    GridNodeMetrics metrics = metricsProvider.getMetrics();
                    JGroupsNode localTransportNode = new JGroupsNode(localTransportNodeInfo, metrics);
                    handleLeave(localTransportNode);
                }
            }
        }

    }

    private final class JGroupsMembershipHandler extends ReceiverAdapter {

        public JGroupsMembershipHandler() {
            super();
        }

        @Override
        public void suspect(Address addr) {
            synchronized(lock) {
                if(members.contains(addr)) {
                    suspectedMembers.add(addr);
                }
            }
        }

        @Override
        public void viewAccepted(View view) {
            if(view == null) {// Services view might be null during startup.
                return;
            }
            if(LOG.isDebugEnabled()) {
                LOG.debug("JGroups membership changes: " + view.printDetails());
            }
            synchronized(lock) {
                // Check for left nodes.
                for(final Iterator<Address> mbrItor = members.iterator(); mbrItor.hasNext();) {
                    final Address mbr = mbrItor.next();
                    if(!view.containsMember(mbr)) {
                        mbrItor.remove();
                        IpAddress ipAddr = (IpAddress) mbr;
                        final GridNodeInfo nodeInfo = GridUtils.getNodeInfo(ipAddr);
                        if(nodeInfo == null) {
                            continue;
                        }
                        final JGroupsNode leftTransportNode = new JGroupsNode(nodeInfo);
                        if(suspectedMembers.remove(mbr)) {
                            handleDropout(leftTransportNode);
                        } else {
                            handleLeave(leftTransportNode);
                        }
                    }
                }
                // Check for joining nodes.
                for(Address mbr : view.getMembers()) {
                    if(members.add(mbr)) {
                        IpAddress ipAddr = (IpAddress) mbr;
                        final GridNodeInfo nodeInfo = GridUtils.getNodeInfo(ipAddr);

                        if(LOG.isTraceEnabled()) {
                            LOG.trace("A node [" + nodeInfo + "] of the ip address " + ipAddr
                                    + " is joined");
                        }
                        if(nodeInfo == null) {
                            continue;
                        }
        
                        final JGroupsNode newTransportNode = new JGroupsNode(nodeInfo);
                        if(nodeInfo.equals(localTransportNodeInfo)) {
                            GridNodeMetrics metrics = metricsProvider.getMetrics();
                            newTransportNode.setMetrics(metrics);
                        }                        
                        handleJoin(newTransportNode);
                    }
                }
            }
        }

        @Override
        public void receive(Message msg) {
            if(msg == null || msg.getLength() == 0) {
                return;
            }
            Object payload = msg.getObject();
            if(payload instanceof GridDiscoveryMessage) {
                GridDiscoveryMessage discoveryMsg = (JGroupsDiscoveryMessage) payload;
                switch(discoveryMsg.getGmsMessageType()) {
                    case metricsUpdate:
                        JGroupsDiscoveryMessage jgMsg = (JGroupsDiscoveryMessage) payload;
                        IpAddress addr = jgMsg.getIpAddress();
                        GridNodeMetrics metrics = jgMsg.getMetrics();
                        onMetricsReceived(addr, metrics);
                        break;
                    case gmsMessage:

                        // TODO
                        break;
                    default:
                        assert false : discoveryMsg.getGmsMessageType();
                }
            }
        }
    }

    private void onMetricsReceived(@Nonnull IpAddress addr, GridNodeMetrics metrics) {
        if(metrics == null) {
            throw new IllegalStateException("No Metrics was set for the node: " + addr);
        }

        final GridNodeInfo nodeInfo = GridUtils.getNodeInfo(addr);
        if(nodeInfo == null) {
            return;
        }
        final JGroupsNode transportNode = new JGroupsNode(nodeInfo, metrics);
        synchronized(lock) {
            if(members.add(addr)) {
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Received metrics from unknown node: " + addr);
                }
                handleJoin(transportNode);
            } else {
                handleMetricsUpdate(transportNode);
            }
        }
    }

    private static final class MetricsSender extends Thread {

        final JGroupsDiscoveryService gms;
        final JChannel channel;
        final GridNodeMetricsProvider metricsProvider;
        final IpAddress localAddr;
        final GridConfiguration config;
        final int initialDelay;
        boolean interrupted = false;

        public MetricsSender(@Nonnull JGroupsDiscoveryService groupsDiscoveryService, @Nonnull JChannel channel, @Nonnull GridNodeMetricsProvider metricsProvider, @Nonnull GridConfiguration config, int initialDelay) {
            super("gridool-grid-metrics-sender");
            this.gms = groupsDiscoveryService;
            this.channel = channel;
            this.metricsProvider = metricsProvider;
            this.localAddr = (IpAddress) channel.getLocalAddress();
            this.config = config;
            this.initialDelay = initialDelay;
        }

        @Override
        public void run() {
            if(initialDelay > 0) {
                try {
                    Thread.sleep(initialDelay);
                } catch (InterruptedException e) {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("MetricsSender interrupted: " + e);
                    }
                }
            }
            while(!isInterrupted()) {
                final GridNodeMetrics metrics = metricsProvider.getMetrics();       

                // handle local metrics update
                gms.onMetricsReceived(localAddr, metrics);

                // send metrics to remote members
                final JGroupsDiscoveryMessage msg = new JGroupsDiscoveryMessage(localAddr, metrics);
                try {
                    gms.sendMessage(msg);
                } catch (ChannelException e) {
                    LOG.error("Failed sending metrics", e);
                }
                try {
                    Thread.sleep(config.getMetricsSyncFrequency());
                } catch (InterruptedException e) {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("MetricsSender interrupted: " + e);
                    }
                }
            }
        }

        @Override
        public void interrupt() {
            this.interrupted = true;
            super.interrupt();
        }

        @Override
        public boolean isInterrupted() {
            return interrupted || super.isInterrupted();
        }

    }

    public void sendMessage(@Nonnull JGroupsDiscoveryMessage payloadMessage)
            throws ChannelException {
        if(!channel.isConnected()) {
            LOG.debug("Channel already closed, thus avoid sending msg to node: "
                    + payloadMessage.getIpAddress());
            return;
        }
        Message msg = new Message(null, payloadMessage.getIpAddress(), payloadMessage);
        msg.setFlag(Message.OOB); // unordered
        channel.send(msg);
    }

}
