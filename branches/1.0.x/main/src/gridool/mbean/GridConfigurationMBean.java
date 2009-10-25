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
package gridool.mbean;

import gridool.directory.ILocalDirectory.DirectoryIndexType;
import gridool.loadblancing.GridLoadProbe;
import gridool.routing.GridNodeSelector;
import gridool.util.HashFunction;

import java.io.Serializable;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public interface GridConfigurationMBean extends Serializable {

    @Nonnegative
    int getNumberOfVirtualNodes();

    @Nonnull
    HashFunction getHashFunction();

    @Nonnull
    GridLoadProbe getProbe();

    void setProbe(@Nonnull GridLoadProbe probe);

    @Nonnull
    GridNodeSelector getNodeSelector();

    void setNodeSelector(@Nonnull GridNodeSelector nodeSelector);

    @Nonnegative
    long getPingTimeout();

    void setPingTimeout(@Nonnegative long timeoutInMills);

    @Nonnegative
    long getMetricsSyncFrequency();

    void setMetricsSyncFrequency(@Nonnegative long freq);

    long getMetricsSyncInitialDelay();
    
    int getMetricsHistorySize();
    
    void setMetricsHistorySize(int size);

    @Nonnegative
    int getJobProcessorPoolSize();

    @Nonnegative
    int getTaskAssignorPoolSize();

    void setTaskAssignorPoolSize(@Nonnegative int taskAssignorPoolSize);

    @Nonnegative
    int getTaskProcessorPoolSize();

    @Nonnegative
    int getTransportServerPort();

    @Nonnegative
    int getTransportChannelSweepInterval();

    @Nonnegative
    int getTransportChannelTTL();

    @Nonnegative
    int getTransportSocketReceiveBufferSize();

    @Nonnegative
    int getSelectorReadThreadsCount();

    @Nonnegative
    int getReadThreadsGrowThreshold();

    @Nonnegative
    int getMessageProcessorPoolSize();

    @Nonnegative
    int getMetricsCollectInterval();

    boolean isSuperNode();

    boolean isJoinToMembership();

    @Nonnull
    DirectoryIndexType getDirectoryIndexType();

}
