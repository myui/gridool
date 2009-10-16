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
package gridool;

import gridool.communication.transport.CommunicationServiceBase;
import gridool.communication.transport.GridTransportClient;
import gridool.communication.transport.tcp.GridMasterSlaveWorkerServer;
import gridool.directory.ILocalDirectory.DirectoryIndexType;
import gridool.loadblancing.GridLoadProbe;
import gridool.loadblancing.GridLoadProbeFactory;
import gridool.mbean.GridConfigurationMBean;
import gridool.routing.GridNodeSelector;
import gridool.routing.GridNodeSelectorFactory;
import gridool.util.DefaultHashFunction;
import gridool.util.HashFunction;

import javax.annotation.Nonnull;

import xbird.config.Settings;
import xbird.util.lang.ObjectUtils;
import xbird.util.lang.Primitives;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridConfiguration implements GridConfigurationMBean {
    private static final long serialVersionUID = -5466809166525908272L;

    private final int numberOfVirtualNodes;
    private transient HashFunction hashFunction = null;
    private transient GridLoadProbe probe = null;
    private transient GridNodeSelector nodeSelector = null;
    private long pingTimeout = 2000;
    private long metricsSyncFrequency;
    private final long metricsSyncInitialDelay;
    private final int metricsCollectInterval;
    private int metricsHistorySize;
    private final int jobProcessorPoolSize;
    private int taskAssignorPoolSize;
    private final int taskProcessorPoolSize;
    private final int transportServerPort;
    private final int transportChannelSweepInterval;
    private final int transportChannelTTL;
    private final int socketReceiveBufferSize;
    private final int selectorReadThreadsCount;
    private final int readThreadsGrowThreshold;
    private final int messageProcessorPoolSize;

    private boolean joinToMembership;
    private final boolean superNode;
    private final DirectoryIndexType ldIdxType;

    public GridConfiguration() {
        this.numberOfVirtualNodes = Primitives.parseInt(Settings.get("gridool.num_virtual_nodes"), 64);
        this.metricsSyncFrequency = Primitives.parseInt(Settings.get("gridool.metrics.sync_freq"), 5000);
        this.metricsSyncInitialDelay = Primitives.parseInt(Settings.get("gridool.metrics.sync_initial_delay"), 3000);
        this.metricsCollectInterval = Primitives.parseInt(Settings.get("gridool.metrics.collect_interval"), 2000);
        this.metricsHistorySize = Primitives.parseInt(Settings.get("gridool.metrics.history_size"), 1000);
        this.jobProcessorPoolSize = Primitives.parseInt(Settings.get("gridool.job.proc.poolsize"), 16);
        this.taskAssignorPoolSize = Primitives.parseInt(Settings.get("gridool.job.taskassignor.poolsize"), 16);
        this.taskProcessorPoolSize = Primitives.parseInt(Settings.get("gridool.task.proc.poolsize"), 64);
        this.transportServerPort = Primitives.parseInt(Settings.get("gridool.transport.port"), CommunicationServiceBase.DEFAULT_PORT);
        this.transportChannelSweepInterval = Primitives.parseInt(Settings.get("gridool.transport.channel.sweep_interval"), GridTransportClient.DEFAULT_SWEEP_INTERVAL);
        this.transportChannelTTL = Primitives.parseInt(Settings.get("gridool.transport.channel.ttl"), GridTransportClient.DEFAULT_TTL);
        this.socketReceiveBufferSize = Primitives.parseInt(Settings.get("gridool.transport.channel.rcvbufsz"), GridTransportClient.DEFAULT_RCVBUFSZ);
        this.selectorReadThreadsCount = Primitives.parseInt(Settings.get("gridool.transport.readThreads"), 6);
        this.readThreadsGrowThreshold = Primitives.parseInt(Settings.get("gridool.transport.readThreads.growThreshold"), GridMasterSlaveWorkerServer.READER_POOL_GROW_THRESHOLD);
        this.messageProcessorPoolSize = Primitives.parseInt(Settings.get("gridool.transport.msgproc.poolsize"), 6);
        this.joinToMembership = System.getProperty("gridool.kernel.nojoin") == null;
        this.superNode = Boolean.parseBoolean(Settings.getThroughSystemProperty("gridool.superNode"));
        this.ldIdxType = DirectoryIndexType.resolve(Settings.get("gridool.ld.idxtype"));
    }

    public int getNumberOfVirtualNodes() {
        return numberOfVirtualNodes;
    }

    public HashFunction getHashFunction() {
        if(hashFunction == null) {// avoid existence of multiple instances
            this.hashFunction = createHashFunction();
        }
        return hashFunction;
    }

    @Nonnull
    private static HashFunction createHashFunction() {
        String clazz = Settings.get("gridool.hasher");
        if(clazz == null) {
            return new DefaultHashFunction();
        }
        Object instance = ObjectUtils.instantiate(clazz);
        if(!(instance instanceof HashFunction)) {
            throw new IllegalStateException("Hash function must be subclass of "
                    + HashFunction.class.getName() + ", but was "
                    + (instance == null ? "null" : instance.getClass().getName()));
        }
        return (HashFunction) instance;
    }

    @Nonnull
    public GridLoadProbe getProbe() {
        if(probe == null) {// avoid existence of multiple instances
            this.probe = GridLoadProbeFactory.createProbe();
        }
        return probe;
    }

    public void setProbe(GridLoadProbe probe) {
        if(probe == null) {
            throw new IllegalArgumentException();
        }
        this.probe = probe;
    }

    public GridNodeSelector getNodeSelector() {
        if(nodeSelector == null) {// avoid existence of multiple instances
            this.nodeSelector = GridNodeSelectorFactory.createSelector();
        }
        return nodeSelector;
    }

    public void setNodeSelector(GridNodeSelector nodeSelector) {
        if(nodeSelector == null) {
            throw new IllegalArgumentException();
        }
        this.nodeSelector = nodeSelector;
    }

    public long getPingTimeout() {
        return pingTimeout;
    }

    public void setPingTimeout(long timeoutInMills) {
        pingTimeout = timeoutInMills;
    }

    public long getMetricsSyncFrequency() {
        return metricsSyncFrequency;
    }

    public void setMetricsSyncFrequency(long freq) {
        this.metricsSyncFrequency = freq;
    }

    public long getMetricsSyncInitialDelay() {
        return metricsSyncInitialDelay;
    }

    public int getMetricsCollectInterval() {
        return metricsCollectInterval;
    }

    public int getMetricsHistorySize() {
        return metricsHistorySize;
    }

    public void setMetricsHistorySize(int size) {
        this.metricsHistorySize = size;
    }

    public int getJobProcessorPoolSize() {
        return jobProcessorPoolSize;
    }

    public int getTaskAssignorPoolSize() {
        return taskAssignorPoolSize;
    }

    public void setTaskAssignorPoolSize(int taskAssignorPoolSize) {
        this.taskAssignorPoolSize = taskAssignorPoolSize;
    }

    public int getTaskProcessorPoolSize() {
        return taskProcessorPoolSize;
    }

    public int getTransportServerPort() {
        return transportServerPort;
    }

    public int getTransportChannelSweepInterval() {
        return transportChannelSweepInterval;
    }

    public int getTransportChannelTTL() {
        return transportChannelTTL;
    }

    public int getTransportSocketReceiveBufferSize() {
        return socketReceiveBufferSize;
    }

    public int getSelectorReadThreadsCount() {
        return selectorReadThreadsCount;
    }

    public int getReadThreadsGrowThreshold() {
        return readThreadsGrowThreshold;
    }

    public int getMessageProcessorPoolSize() {
        return messageProcessorPoolSize;
    }

    public boolean isSuperNode() {
        return superNode;
    }

    public boolean isJoinToMembership() {
        return joinToMembership;
    }

    public void setJoinToMembership(boolean joinToMembership) {
        this.joinToMembership = joinToMembership;
    }

    public DirectoryIndexType getDirectoryIndexType() {
        return ldIdxType;
    }

}
