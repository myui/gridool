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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jgroups.stack.IpAddress;

import gridool.GridNodeMetrics;
import gridool.discovery.GridDiscoveryMessage;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class JGroupsDiscoveryMessage implements GridDiscoveryMessage, Externalizable {
    private static final long serialVersionUID = 2626508731630161246L;

    @Nonnull
    private IpAddress ipAddress;
    @Nonnull
    private DiscoveryMessageType messageType;

    @Nullable
    private GridNodeMetrics metrics = null;

    public JGroupsDiscoveryMessage() {} // for Externalizable

    @Deprecated
    public JGroupsDiscoveryMessage(@Nonnull IpAddress addr, @Nonnull DiscoveryMessageType type) {
        assert (addr != null);
        assert (type != null);
        this.ipAddress = addr;
        this.messageType = type;
    }

    public JGroupsDiscoveryMessage(@Nonnull IpAddress addr, GridNodeMetrics metrics) {
        assert (addr != null);
        assert (metrics != null);
        this.ipAddress = addr;
        this.messageType = DiscoveryMessageType.metricsUpdate;
        this.metrics = metrics;
    }

    @Nonnull
    public IpAddress getIpAddress() {
        return ipAddress;
    }

    public DiscoveryMessageType getGmsMessageType() {
        return messageType;
    }

    public GridNodeMetrics getMetrics() {
        return metrics;
    }

    public void setMetrics(GridNodeMetrics metrics) {
        this.metrics = metrics;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.ipAddress = (IpAddress) in.readObject();
        this.messageType = (DiscoveryMessageType) in.readObject();
        this.metrics = (GridNodeMetrics) in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ipAddress);
        out.writeObject(messageType);
        out.writeObject(metrics);
    }
}
